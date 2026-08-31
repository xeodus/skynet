use origin_mock::{OriginConfig, OriginHandle};
use std::net::SocketAddr;
use std::time::Duration;

async fn wait_for_tcp(addr: SocketAddr) {
    origin_mock::wait_for_tcp(addr).await;
}

async fn wait_for_metric_contains(edge_addr: SocketAddr, needle: &str) {
    let url = format!("http://{edge_addr}/__metrics");

    for _ in 0..100 {
        let res = reqwest::get(url.clone()).await;

        if let Ok(res) = res {
            if res.status().is_success() {
                let text = res.text().await.unwrap_or_default();

                if text.contains(needle) {
                    return;
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    panic!("metric '{needle}' never appeared");
}

async fn spawn_origin(payload_size: usize, chunk_size: usize, delay_ms: u64) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind origin listener");

    let addr = listener.local_addr().expect("origin addr");

    tokio::spawn(origin_mock::serve(
        listener,
        payload_size,
        chunk_size,
        delay_ms,
    ));

    wait_for_tcp(addr).await;

    addr
}

async fn spawn_edge(origin_addr: SocketAddr) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind edge listener");

    let addr = listener.local_addr().expect("edge addr");

    tokio::spawn(async move {
        edge_node::serve(listener, origin_addr)
            .await
            .expect("edge node failed");
    });

    wait_for_tcp(addr).await;

    addr
}

async fn unused_addr() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind unused listener");

    let addr = listener.local_addr().expect("unused addr");

    drop(listener);

    addr
}

#[tokio::test]
async fn phase1_streams_payload_and_records_metrics() {
    let payload_size = 2 * 1024 * 1024;
    let chunk_size = 64 * 1024;

    let origin_addr = spawn_origin(payload_size, chunk_size, 0).await;
    let edge_addr = spawn_edge(origin_addr).await;

    let url = format!("http://{edge_addr}/large-file");

    let res = reqwest::get(url).await.expect("edge request failed");

    assert!(
        res.status().is_success(),
        "expected success, got {}",
        res.status()
    );

    let body = res.bytes().await.expect("failed reading body");

    assert_eq!(body.len(), payload_size);

    let needle = format!("edge_bytes_served_total {payload_size}");

    wait_for_metric_contains(edge_addr, &needle).await;
    wait_for_metric_contains(edge_addr, "edge_request_duration_seconds_count").await;
}

#[tokio::test]
async fn phase1_origin_serves_payload_directly() {
    let payload_size = 256 * 1024;
    let chunk_size = 64 * 1024;

    let origin_addr = spawn_origin(payload_size, chunk_size, 0).await;

    let url = format!("http://{origin_addr}/large-file");

    let res = reqwest::get(url)
        .await
        .expect("origin request failed");

    assert!(
        res.status().is_success(),
        "origin returned {}",
        res.status()
    );

    let body = res
        .bytes()
        .await
        .expect("origin body read failed");

    assert_eq!(body.len(), payload_size);
}

#[tokio::test]
async fn phase1_origin_counts_and_honors_delay() {
    let handle = OriginHandle::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let h = handle.clone();
    tokio::spawn(async move {
        origin_mock::run(
            listener,
            OriginConfig {
                default_size: 1024,
                chunk_size: 256,
                delay_ms: 50,
                error_status: None,
            },
            h,
        )
        .await;
    });
    wait_for_tcp(addr).await;

    let started = std::time::Instant::now();
    let res = reqwest::get(format!("http://{addr}/obj/slow?size=1024"))
        .await
        .unwrap();
    let body = res.bytes().await.unwrap();
    assert_eq!(body.len(), 1024);
    assert!(started.elapsed() >= Duration::from_millis(40));
    assert_eq!(handle.path_hits("/obj/slow"), 1);
}

#[tokio::test]
async fn phase1_returns_502_when_origin_is_down() {
    let origin_addr = unused_addr().await;
    let edge_addr = spawn_edge(origin_addr).await;

    let url = format!("http://{edge_addr}/large-file");

    let res = reqwest::get(url).await.expect("edge request failed");

    assert_eq!(res.status(), reqwest::StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn phase1_exposes_metrics_endpoint() {
    let origin_addr = unused_addr().await;
    let edge_addr = spawn_edge(origin_addr).await;

    let url = format!("http://{edge_addr}/__metrics");

    let res = reqwest::get(url).await.expect("metrics request failed");

    assert!(res.status().is_success());

    let text = res.text().await.expect("metrics body");

    assert!(text.contains("edge_bytes_served_total"));
    assert!(text.contains("edge_request_duration_seconds"));
    assert!(text.contains("edge_cache_hits_total"));
}
