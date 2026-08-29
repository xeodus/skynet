use edge_node::{serve_plain, serve_tls, tls_server_config_from_pem};
use origin_mock::serve_mock_origin;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use telemetry::EdgeMetrics;

async fn wait_for_tcp(addr: SocketAddr) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    panic!("nothing is listening on {addr}");
}

async fn wait_for_metric(metrics: &Arc<EdgeMetrics>, needle: &str) {
    for _ in 0..100 {
        let text = metrics.render();

        if text.contains(needle) {
            return;
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    panic!(
        "metric not found. Expected '{needle}'\nMetrics:\n{}",
        metrics.render()
    );
}

async fn spawn_origin(payload_size: usize, chunk_size: usize, delay_ms: u64) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind origin listener");

    let addr = listener.local_addr().expect("failed to get origin addr");

    tokio::spawn(serve_mock_origin(
        listener,
        payload_size,
        chunk_size,
        delay_ms,
    ));

    wait_for_tcp(addr).await;

    addr
}

async fn spawn_edge_plain(origin_addr: SocketAddr) -> (SocketAddr, Arc<EdgeMetrics>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind edge listener");

    let addr = listener.local_addr().expect("failed to get edge addr");

    let metrics = Arc::new(EdgeMetrics::new());

    tokio::spawn(serve_plain(listener, origin_addr, metrics.clone()));

    wait_for_tcp(addr).await;

    (addr, metrics)
}

#[tokio::test]
async fn phase1_proxy_forwards_payload_and_records_metrics() {
    let payload_size = 5 * 1024 * 1024;

    let origin_addr = spawn_origin(payload_size, 64 * 1024, 0).await;

    let (proxy_addr, metrics) = spawn_edge_plain(origin_addr).await;

    let url = format!("http://{proxy_addr}/large-file");

    let res = reqwest::get(url)
        .await
        .expect("proxy request failed");

    assert!(
        res.status().is_success(),
        "expected successful proxy response, got {}",
        res.status()
    );

    let body = res.bytes().await.expect("failed to read response body");

    assert_eq!(
        body.len(),
        payload_size,
        "proxy did not forward the full origin payload"
    );

    let needle = format!("edge_bytes_served_total {payload_size}");

    wait_for_metric(&metrics, &needle).await;

    let text = metrics.render();

    assert!(
        text.contains("edge_request_duration_seconds_count"),
        "latency histogram was not observed"
    );
}

#[tokio::test]
async fn phase1_proxy_returns_502_when_origin_is_down() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind unused origin listener");

    let origin_addr = listener.local_addr().expect("failed to get origin addr");

    drop(listener);

    let (proxy_addr, _metrics) = spawn_edge_plain(origin_addr).await;

    let res = reqwest::get(format!("http://{proxy_addr}/large-file"))
        .await
        .expect("proxy request failed");

    assert_eq!(res.status(), reqwest::StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn phase1_metrics_endpoint_is_exposed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind unused origin listener");

    let origin_addr = listener.local_addr().expect("failed to get origin addr");

    drop(listener);

    let (proxy_addr, _metrics) = spawn_edge_plain(origin_addr).await;

    let res = reqwest::get(format!("http://{proxy_addr}/__metrics"))
        .await
        .expect("metrics request failed");

    assert!(res.status().is_success());

    let text = res.text().await.expect("failed to read metrics body");

    assert!(text.contains("edge_bytes_served_total"));
    assert!(text.contains("edge_request_duration_seconds"));
}

#[tokio::test]
async fn phase1_edge_terminates_tls() {
    let payload_size = 1024 * 1024;

    let origin_addr = spawn_origin(payload_size, 64 * 1024, 0).await;

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("failed to generate test certificate");

    let cert_pem = cert.serialize_pem().expect("failed to serialize certificate pem");
    let key_pem = cert.serialize_private_key_pem();

    let tls_config = tls_server_config_from_pem(&cert_pem, &key_pem)
        .expect("failed to create tls server config");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind tls edge listener");

    let proxy_addr = listener.local_addr().expect("failed to get tls edge addr");

    let metrics = Arc::new(EdgeMetrics::new());

    tokio::spawn(serve_tls(
        listener,
        origin_addr,
        tls_config,
        metrics.clone(),
    ));

    wait_for_tcp(proxy_addr).await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("failed to build reqwest client");

    let res = client
        .get(format!("https://{proxy_addr}/large-file"))
        .send()
        .await
        .expect("TLS request failed");

    assert!(
        res.status().is_success(),
        "expected successful TLS proxy response, got {}",
        res.status()
    );

    let body = res.bytes().await.expect("failed to read TLS response body");

    assert_eq!(body.len(), payload_size);
}
