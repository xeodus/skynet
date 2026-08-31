use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use steering::{choose, NodeSnapshot, Weights};
use tokio::net::{TcpListener, UdpSocket};

#[derive(Clone)]
pub struct ControlConfig {
    pub replica_factor: usize,
    pub stale_after: Duration,
    pub weights: Weights,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            replica_factor: 2,
            stale_after: Duration::from_secs(3),
            weights: Weights::default(),
        }
    }
}

#[derive(Clone)]
struct Record {
    snapshot: NodeSnapshot,
    hot_keys: Vec<String>,
    last_seen: Instant,
}

#[derive(Clone)]
struct AppState {
    nodes: Arc<Mutex<HashMap<String, Record>>>,
    config: ControlConfig,
}

#[derive(Deserialize)]
struct LocateQuery {
    path: Option<String>,
    key: Option<String>,
}

#[derive(Serialize, Clone, Debug)]
pub struct LocateResponse {
    pub node_id: String,
    pub addr: String,
    pub score: f64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Heartbeat {
    pub node_id: String,
    pub addr: String,
    pub healthy: bool,
    pub inflight: u64,
    pub capacity: u64,
    pub cache_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub bandwidth_price: f64,
    pub ewma_rtt_ms: f64,
    pub hot_keys: Vec<String>,
}

pub async fn serve(listener: TcpListener, config: ControlConfig) -> std::io::Result<()> {
    let state = make_state(config);
    axum::serve(listener, router(state)).await
}

pub async fn serve_with_dns(
    listener: TcpListener,
    dns: UdpSocket,
    config: ControlConfig,
) -> std::io::Result<()> {
    let state = make_state(config);
    let dns_state = state.clone();
    tokio::spawn(async move {
        dns_loop(dns, dns_state).await;
    });
    axum::serve(listener, router(state)).await
}

fn make_state(config: ControlConfig) -> AppState {
    AppState {
        nodes: Arc::new(Mutex::new(HashMap::new())),
        config,
    }
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/heartbeat", post(heartbeat))
        .route("/locate", get(locate))
        .route("/nodes", get(list_nodes))
        .with_state(state)
}

async fn heartbeat(State(state): State<AppState>, Json(body): Json<Heartbeat>) {
    let snap = NodeSnapshot {
        node_id: body.node_id.clone(),
        addr: body.addr,
        healthy: body.healthy,
        inflight: body.inflight,
        capacity: body.capacity,
        cache_bytes: body.cache_bytes,
        hits: body.hits,
        misses: body.misses,
        bandwidth_price: body.bandwidth_price,
        ewma_rtt_ms: body.ewma_rtt_ms,
        has_key: false,
    };
    state.nodes.lock().expect("registry").insert(
        body.node_id,
        Record {
            snapshot: snap,
            hot_keys: body.hot_keys,
            last_seen: Instant::now(),
        },
    );
}

async fn locate(
    State(state): State<AppState>,
    Query(q): Query<LocateQuery>,
) -> Result<Json<LocateResponse>, StatusCode> {
    let key = q
        .key
        .or(q.path)
        .unwrap_or_else(|| "cdn.local".to_string());
    locate_key(&state, &key).ok_or(StatusCode::SERVICE_UNAVAILABLE).map(Json)
}

async fn list_nodes(State(state): State<AppState>) -> Json<Vec<NodeSnapshot>> {
    Json(snapshots_for(&state, ""))
}

fn locate_key(state: &AppState, key: &str) -> Option<LocateResponse> {
    let nodes = snapshots_for(state, key);
    let some = choose(key, &nodes, &state.config.weights, state.config.replica_factor)?;
    Some(LocateResponse {
        node_id: some.node_id.clone(),
        addr: some.addr.clone(),
        score: steering::score(some, &state.config.weights),
    })
}

fn snapshots_for(state: &AppState, key: &str) -> Vec<NodeSnapshot> {
    let mut nodes = state.nodes.lock().expect("registry");
    nodes.retain(|_, rec| rec.last_seen.elapsed() < state.config.stale_after);
    nodes
        .values()
        .map(|rec| {
            let mut snap = rec.snapshot.clone();
            snap.has_key = rec.hot_keys.iter().any(|k| k == key);
            snap
        })
        .collect()
}

async fn dns_loop(socket: UdpSocket, state: AppState) {
    let mut buf = [0u8; 512];
    loop {
        let Ok((n, from)) = socket.recv_from(&mut buf).await else {
            continue;
        };
        let Some(reply) = build_dns_reply(&buf[..n], &state) else {
            continue;
        };
        let _ = socket.send_to(&reply, from).await;
    }
}

fn build_dns_reply(query: &[u8], state: &AppState) -> Option<Vec<u8>> {
    if query.len() < 12 {
        return None;
    }
    let qid = &query[0..2];
    let (_name, qtype_at) = parse_qname(query, 12)?;
    if qtype_at + 4 > query.len() {
        return None;
    }
    let qtype = u16::from_be_bytes([query[qtype_at], query[qtype_at + 1]]);
    let chosen = locate_key(state, "cdn.local");
    let ip = chosen
        .as_ref()
        .and_then(|c| parse_ipv4(&c.addr))
        .unwrap_or(Ipv4Addr::UNSPECIFIED);

    let mut out = Vec::new();
    out.extend_from_slice(qid);
    // QR=1, RD copied, RA=1, rcode=0 or 2 if no node
    let flags_lo = query[3] & 0x01;
    let rcode: u8 = if chosen.is_none() { 2 } else { 0 };
    out.push(0x80 | (query[2] & 0x01));
    out.push(flags_lo | 0x80 | rcode);
    out.extend_from_slice(&query[4..6]); // QDCOUNT
    let an: u16 = if chosen.is_some() && qtype == 1 { 1 } else { 0 };
    out.extend_from_slice(&an.to_be_bytes());
    out.extend_from_slice(&[0, 0, 0, 0]); // NSCOUNT, ARCOUNT
    out.extend_from_slice(&query[12..qtype_at + 4]);
    if an == 1 {
        out.extend_from_slice(&[0xC0, 0x0C]);
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(&5u32.to_be_bytes());
        out.extend_from_slice(&4u16.to_be_bytes());
        out.extend_from_slice(&ip.octets());
    }
    Some(out)
}

fn parse_qname(msg: &[u8], mut i: usize) -> Option<(String, usize)> {
    let mut labels = Vec::new();
    loop {
        if i >= msg.len() {
            return None;
        }
        let len = msg[i] as usize;
        if len == 0 {
            return Some((labels.join("."), i + 1));
        }
        if len >= 192 {
            return None;
        }
        i += 1;
        if i + len > msg.len() {
            return None;
        }
        labels.push(String::from_utf8_lossy(&msg[i..i + len]).into_owned());
        i += len;
    }
}

fn parse_ipv4(addr: &str) -> Option<Ipv4Addr> {
    let host = addr.split(':').next()?;
    host.parse().ok()
}

pub async fn wait_for_tcp(addr: SocketAddr) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("nothing is listening on {addr}");
}
