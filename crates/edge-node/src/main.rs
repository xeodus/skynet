use edge_node::EdgeConfig;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let bind: SocketAddr = env("BIND", "127.0.0.1:18081").parse().expect("BIND");
    let origin: SocketAddr = env("ORIGIN", "127.0.0.1:18080").parse().expect("ORIGIN");
    let node_id = env("NODE_ID", "edge-a");
    let price: f64 = env("PRICE", "1.0").parse().unwrap_or(1.0);
    let cache_max: u64 = env("CACHE_BYTES", "67108864").parse().unwrap_or(64 * 1024 * 1024);
    let capacity: u64 = env("CAPACITY", "1024").parse().unwrap_or(1024);
    let rtt: f64 = env("RTT_MS", "5").parse().unwrap_or(5.0);
    let control_plane = std::env::var("CONTROL_PLANE").ok();

    let listener = TcpListener::bind(bind).await.expect("bind edge");
    let addr = listener.local_addr().expect("addr");
    eprintln!("edge-node {node_id} {addr} origin={origin} price={price}");

    let config = EdgeConfig {
        node_id,
        origin,
        listen: addr,
        cache_max_bytes: cache_max,
        cache_max_object_bytes: cache_max,
        bandwidth_price: price,
        capacity,
        ewma_rtt_ms: rtt,
        control_plane,
    };
    edge_node::serve_with(listener, config)
        .await
        .expect("edge-node");
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}
