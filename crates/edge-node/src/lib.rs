use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, StatusCode},
    response::Response,
    routing::get,
    Json, Router,
};
use bytes::Bytes;
use cache::{ByteLru, CachedObject, FetchOutcome, Flight, SingleFlight};
use futures_util::stream::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use telemetry::EdgeMetrics;
use tokio::net::TcpListener;

#[derive(Clone, Debug)]
pub struct EdgeConfig {
    pub node_id: String,
    pub origin: SocketAddr,
    pub listen: SocketAddr,
    pub cache_max_bytes: u64,
    pub cache_max_object_bytes: u64,
    pub bandwidth_price: f64,
    pub capacity: u64,
    pub ewma_rtt_ms: f64,
    pub control_plane: Option<String>,
}

impl EdgeConfig {
    pub fn for_origin(origin: SocketAddr) -> Self {
        Self {
            node_id: "edge".into(),
            origin,
            listen: "127.0.0.1:0".parse().unwrap(),
            cache_max_bytes: 64 * 1024 * 1024,
            cache_max_object_bytes: 8 * 1024 * 1024,
            bandwidth_price: 1.0,
            capacity: 1024,
            ewma_rtt_ms: 1.0,
            control_plane: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct HealthSnapshot {
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

#[derive(Clone)]
struct EdgeState {
    config: EdgeConfig,
    client: Client,
    metrics: Arc<EdgeMetrics>,
    cache: ByteLru,
    singleflight: SingleFlight,
}

pub async fn serve(listener: TcpListener, origin: SocketAddr) -> std::io::Result<()> {
    let mut config = EdgeConfig::for_origin(origin);
    config.listen = listener.local_addr()?;
    serve_with(listener, config).await
}

pub async fn serve_with(listener: TcpListener, mut config: EdgeConfig) -> std::io::Result<()> {
    config.listen = listener.local_addr()?;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(64)
        .build()
        .expect("reqwest client should build");

    let state = EdgeState {
        cache: ByteLru::new(config.cache_max_bytes, config.cache_max_object_bytes),
        config,
        client,
        metrics: Arc::new(EdgeMetrics::new()),
        singleflight: SingleFlight::new(),
    };

    if let Some(control) = state.config.control_plane.clone() {
        let hb_state = state.clone();
        tokio::spawn(async move {
            heartbeat_loop(control, hb_state).await;
        });
    }

    let app = Router::new()
        .route("/__metrics", get(metrics_handler))
        .route("/__health", get(health_handler))
        .fallback(proxy_handler)
        .with_state(state);

    axum::serve(listener, app).await
}

async fn metrics_handler(State(state): State<EdgeState>) -> String {
    state.metrics.set_cache_bytes(state.cache.bytes());
    state.metrics.render()
}

async fn health_handler(State(state): State<EdgeState>) -> Json<HealthSnapshot> {
    Json(snapshot(&state))
}

pub fn snapshot_from_metrics(
    config: &EdgeConfig,
    metrics: &EdgeMetrics,
    cache: &ByteLru,
) -> HealthSnapshot {
    HealthSnapshot {
        node_id: config.node_id.clone(),
        addr: config.listen.to_string(),
        healthy: true,
        inflight: metrics.inflight(),
        capacity: config.capacity,
        cache_bytes: cache.bytes(),
        hits: metrics.hits(),
        misses: metrics.misses(),
        bandwidth_price: config.bandwidth_price,
        ewma_rtt_ms: config.ewma_rtt_ms,
        hot_keys: cache.keys(),
    }
}

fn snapshot(state: &EdgeState) -> HealthSnapshot {
    snapshot_from_metrics(&state.config, &state.metrics, &state.cache)
}

async fn heartbeat_loop(control: String, state: EdgeState) {
    let beat_url = format!("{}/heartbeat", control.trim_end_matches('/'));
    loop {
        let snap = snapshot(&state);
        let _ = state.client.post(&beat_url).json(&snap).send().await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

struct InflightGuard(Arc<EdgeMetrics>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.dec_inflight();
    }
}

async fn proxy_handler(
    State(state): State<EdgeState>,
    req: Request,
) -> Result<Response<Body>, StatusCode> {
    state.metrics.inc_inflight();
    let _inflight = InflightGuard(state.metrics.clone());
    let start = Instant::now();

    if req.method() != axum::http::Method::GET {
        return proxy_uncached(&state, req, start).await;
    }

    let path = req.uri().path().to_string();
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/")
        .to_string();
    let key = path.clone();

    if let Some(obj) = state.cache.get(&key) {
        state.metrics.hit();
        return Ok(cached_response(
            obj,
            &state.metrics,
            start,
            state.config.bandwidth_price,
        ));
    }

    match state.singleflight.join(&key) {
        Flight::Waiter(mut rx) => {
            state.metrics.coalesced();
            match rx.recv().await {
                Ok(FetchOutcome::Object(obj)) => {
                    Ok(cached_response(
                        obj,
                        &state.metrics,
                        start,
                        state.config.bandwidth_price,
                    ))
                }
                _ => {
                    state.metrics.observe_latency(start.elapsed());
                    Err(StatusCode::BAD_GATEWAY)
                }
            }
        }
        Flight::Leader(guard) => {
            state.metrics.miss();
            state.metrics.origin_fetch();
            fetch_and_tee(state, key, path_and_query, start, guard).await
        }
    }
}

async fn proxy_uncached(
    state: &EdgeState,
    req: Request,
    start: Instant,
) -> Result<Response<Body>, StatusCode> {
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let upstream_url = format!("http://{}{}", state.config.origin, path_and_query);
    let upstream_response = state
        .client
        .request(req.method().clone(), upstream_url)
        .send()
        .await
        .map_err(|_| {
            state.metrics.observe_latency(start.elapsed());
            StatusCode::BAD_GATEWAY
        })?;

    Ok(stream_origin(state.clone(), upstream_response, start, None, None))
}

async fn fetch_and_tee(
    state: EdgeState,
    key: String,
    path_and_query: String,
    start: Instant,
    guard: cache::LeaderGuard,
) -> Result<Response<Body>, StatusCode> {
    let upstream_url = format!("http://{}{}", state.config.origin, path_and_query);
    let upstream_response = match state.client.get(upstream_url).send().await {
        Ok(res) => res,
        Err(_) => {
            guard.complete(FetchOutcome::Error);
            state.metrics.observe_latency(start.elapsed());
            return Err(StatusCode::BAD_GATEWAY);
        }
    };

    Ok(stream_origin(
        state,
        upstream_response,
        start,
        Some(key),
        Some(guard),
    ))
}

fn stream_origin(
    state: EdgeState,
    upstream_response: reqwest::Response,
    start: Instant,
    cache_key: Option<String>,
    guard: Option<cache::LeaderGuard>,
) -> Response<Body> {
    let status = upstream_response.status();
    let mut response_builder = Response::builder().status(status);

    for (name, value) in upstream_response.headers() {
        if is_hop_by_hop(name.as_str())
            || name == header::CONTENT_LENGTH
            || name == header::TRANSFER_ENCODING
        {
            continue;
        }
        response_builder = response_builder.header(name.clone(), value.clone());
    }

    let max_object = state.cache.max_object_bytes();
    let stream = TeeStream {
        inner: Box::pin(upstream_response.bytes_stream()),
        metrics: state.metrics.clone(),
        cache: state.cache.clone(),
        start,
        bytes: 0,
        finished: false,
        status: status.as_u16(),
        buf: Vec::new(),
        max_object,
        cache_key,
        guard,
        bandwidth_price: state.config.bandwidth_price,
    };

    response_builder
        .body(Body::from_stream(stream))
        .expect("edge response")
}

fn cached_response(
    obj: CachedObject,
    metrics: &EdgeMetrics,
    start: Instant,
    bandwidth_price: f64,
) -> Response<Body> {
    metrics.add_bytes(obj.len(), bandwidth_price);
    metrics.observe_latency(start.elapsed());
    Response::builder()
        .status(obj.status)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(obj.body))
        .expect("cached response")
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

struct TeeStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    metrics: Arc<EdgeMetrics>,
    cache: ByteLru,
    start: Instant,
    bytes: u64,
    finished: bool,
    status: u16,
    buf: Vec<u8>,
    max_object: u64,
    cache_key: Option<String>,
    guard: Option<cache::LeaderGuard>,
    bandwidth_price: f64,
}

impl TeeStream {
    fn finish(&mut self, error: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.metrics.add_bytes(self.bytes, self.bandwidth_price);
        self.metrics.observe_latency(self.start.elapsed());

        let outcome = if error {
            FetchOutcome::Error
        } else {
            let obj = CachedObject {
                status: self.status,
                body: Bytes::from(std::mem::take(&mut self.buf)),
            };
            if let Some(key) = self.cache_key.take() {
                if obj.cacheable() && obj.len() <= self.max_object && obj.len() > 0 {
                    let evicted = self.cache.insert(key, obj.clone());
                    self.metrics.add_evictions(evicted);
                    self.metrics.set_cache_bytes(self.cache.bytes());
                }
            }
            FetchOutcome::Object(obj)
        };

        if let Some(guard) = self.guard.take() {
            guard.complete(outcome);
        }
    }
}

impl Stream for TeeStream {
    type Item = Result<Bytes, reqwest::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(chunk))) => {
                this.bytes += chunk.len() as u64;
                this.buf.extend_from_slice(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.finish(true);
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                this.finish(false);
                Poll::Ready(None)
            }
        }
    }
}

impl Drop for TeeStream {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(true);
        }
    }
}
