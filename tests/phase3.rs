use edge_node::EdgeConfig;
use origin_mock::{OriginConfig, OriginHandle};
use std::net::SocketAddr;

async fn spawn_origin(handle: OriginHandle) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        origin_mock::run(
            listener,
            OriginConfig {
                default_size: 1024,
                chunk_size: 256,
                delay_ms: 0,
                error_status: None,
            },
            handle,
        )
        .await;
    });
    origin_mock::wait_for_tcp(addr).await;
    addr
}

async fn spawn_edge(origin: SocketAddr, id: &str) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let node_id = id.to_string();
    tokio::spawn(async move {
        edge_node::serve_with(
            listener,
            EdgeConfig {
                node_id,
                origin,
                listen: addr,
                cache_max_bytes: 1024 * 1024,
                cache_max_object_bytes: 1024 * 1024,
                bandwidth_price: 1.0,
                capacity: 32,
                ewma_rtt_ms: 5.0,
                control_plane: None,
            },
        )
        .await
        .unwrap();
    });
    origin_mock::wait_for_tcp(addr).await;
    addr
}

#[tokio::test]
async fn phase3_caches_are_independent_per_node() {
    let handle = OriginHandle::new();
    let origin = spawn_origin(handle.clone()).await;
    let a = spawn_edge(origin, "edge-a").await;
    let b = spawn_edge(origin, "edge-b").await;
    let c = spawn_edge(origin, "edge-c").await;

    let path = "/obj/shared?size=1024";
    for edge in [a, b] {
        let res = reqwest::get(format!("http://{edge}{path}")).await.unwrap();
        assert!(res.status().is_success());
        assert_eq!(res.bytes().await.unwrap().len(), 1024);
    }

    assert_eq!(handle.path_hits("/obj/shared"), 2);

    let ha: serde_json::Value = reqwest::get(format!("http://{a}/__health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let hb: serde_json::Value = reqwest::get(format!("http://{b}/__health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let hc: serde_json::Value = reqwest::get(format!("http://{c}/__health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(ha["node_id"], "edge-a");
    assert_eq!(hb["node_id"], "edge-b");
    assert_eq!(hc["node_id"], "edge-c");
    assert_ne!(ha["addr"], hb["addr"]);
    assert!(ha["hits"].as_u64().unwrap() >= 1 || ha["misses"].as_u64().unwrap() >= 1);
}
