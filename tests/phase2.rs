use edge_node::EdgeConfig;
use origin_mock::{OriginConfig, OriginHandle};
use std::net::SocketAddr;

async fn spawn_origin(config: OriginConfig, handle: OriginHandle) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        origin_mock::run(listener, config, handle).await;
    });
    origin_mock::wait_for_tcp(addr).await;
    addr
}

async fn spawn_edge(origin: SocketAddr, cache_max_bytes: u64, max_object: u64) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let config = EdgeConfig {
            node_id: "edge-test".into(),
            origin,
            listen: addr,
            cache_max_bytes,
            cache_max_object_bytes: max_object,
            bandwidth_price: 1.0,
            capacity: 1024,
            ewma_rtt_ms: 1.0,
            control_plane: None,
        };
        edge_node::serve_with(listener, config)
            .await
            .expect("edge");
    });
    origin_mock::wait_for_tcp(addr).await;
    addr
}

async fn get_bytes(url: &str) -> (reqwest::StatusCode, usize) {
    let res = reqwest::get(url).await.unwrap();
    let status = res.status();
    let n = res.bytes().await.unwrap().len();
    (status, n)
}

async fn metrics(edge: SocketAddr) -> String {
    reqwest::get(format!("http://{edge}/__metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

#[tokio::test]
async fn phase2_cache_hit_bypasses_origin() {
    let handle = OriginHandle::new();
    let origin = spawn_origin(
        OriginConfig {
            default_size: 4096,
            chunk_size: 1024,
            delay_ms: 0,
            error_status: None,
        },
        handle.clone(),
    )
    .await;
    let edge = spawn_edge(origin, 1024 * 1024, 1024 * 1024).await;
    let url = format!("http://{edge}/obj/a?size=4096");

    let (s1, n1) = get_bytes(&url).await;
    let (s2, n2) = get_bytes(&url).await;

    assert_eq!(s1, reqwest::StatusCode::OK);
    assert_eq!(s2, reqwest::StatusCode::OK);
    assert_eq!(n1, 4096);
    assert_eq!(n2, 4096);
    assert_eq!(handle.path_hits("/obj/a"), 1);

    let text = metrics(edge).await;
    assert!(text.contains("edge_cache_hits_total 1"), "{text}");
    assert!(text.contains("edge_origin_fetches_total 1"), "{text}");
}

#[tokio::test]
async fn phase2_cache_evicts_lru_when_full() {
    let handle = OriginHandle::new();
    let origin = spawn_origin(
        OriginConfig {
            default_size: 600 * 1024,
            chunk_size: 32 * 1024,
            delay_ms: 0,
            error_status: None,
        },
        handle.clone(),
    )
    .await;
    let edge = spawn_edge(origin, 1024 * 1024, 1024 * 1024).await;
    let size = 600 * 1024;

    for name in ["a", "b", "c"] {
        let url = format!("http://{edge}/obj/{name}?size={size}");
        let (status, n) = get_bytes(&url).await;
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(n, size);
    }

    let url_a = format!("http://{edge}/obj/a?size={size}");
    let (status, n) = get_bytes(&url_a).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(n, size);
    assert_eq!(handle.path_hits("/obj/a"), 2);
    assert_eq!(handle.path_hits("/obj/b"), 1);
    assert_eq!(handle.path_hits("/obj/c"), 1);
}

#[tokio::test]
async fn phase2_single_flight_coalesces_concurrent_requests() {
    let handle = OriginHandle::new();
    let origin = spawn_origin(
        OriginConfig {
            default_size: 2048,
            chunk_size: 512,
            delay_ms: 500,
            error_status: None,
        },
        handle.clone(),
    )
    .await;
    let edge = spawn_edge(origin, 1024 * 1024, 1024 * 1024).await;
    let url = format!("http://{edge}/obj/hot?size=2048");

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(64)
        .build()
        .unwrap();

    let mut tasks = Vec::new();
    for _ in 0..50 {
        let client = client.clone();
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let res = client.get(url).send().await.unwrap();
            let status = res.status();
            let n = res.bytes().await.unwrap().len();
            (status, n)
        }));
    }

    for task in tasks {
        let (status, n) = task.await.unwrap();
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(n, 2048);
    }

    assert_eq!(handle.path_hits("/obj/hot"), 1);
    let text = metrics(edge).await;
    assert!(text.contains("edge_origin_fetches_total 1"), "{text}");
    assert!(text.contains("edge_inflight_coalesced_total"), "{text}");
}

#[tokio::test]
async fn phase2_oversized_object_is_not_cached() {
    let handle = OriginHandle::new();
    let origin = spawn_origin(
        OriginConfig {
            default_size: 8000,
            chunk_size: 1000,
            delay_ms: 0,
            error_status: None,
        },
        handle.clone(),
    )
    .await;
    let edge = spawn_edge(origin, 1024 * 1024, 1000).await;
    let url = format!("http://{edge}/obj/big?size=8000");
    let (_, n1) = get_bytes(&url).await;
    let (_, n2) = get_bytes(&url).await;
    assert_eq!(n1, 8000);
    assert_eq!(n2, 8000);
    assert_eq!(handle.path_hits("/obj/big"), 2);
}
