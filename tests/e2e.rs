use edge_node::EdgeConfig;
use origin_mock::{OriginConfig, OriginHandle};
use control_plane::ControlConfig;
use std::time::Duration;

#[tokio::test]
async fn origin_stats_counts_object_gets() {
    let handle = OriginHandle::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = handle.clone();
    tokio::spawn(async move {
        origin_mock::run(listener, OriginConfig::default(), h).await;
    });
    origin_mock::wait_for_tcp(addr).await;

    let res = reqwest::get(format!("http://{addr}/obj/x?size=32"))
        .await
        .unwrap();
    assert!(res.status().is_success());
    let _ = res.bytes().await;

    let stats: serde_json::Value = reqwest::get(format!("http://{addr}/__stats"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(stats["total_hits"].as_u64().unwrap() >= 1);
    assert_eq!(handle.path_hits("/obj/x"), 1);
}

#[tokio::test]
async fn e2e_edges_heartbeat_and_second_get_is_cached() {
    let handle = OriginHandle::new();
    let ol = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = ol.local_addr().unwrap();
    let h = handle.clone();
    tokio::spawn(async move {
        origin_mock::run(
            ol,
            OriginConfig {
                default_size: 1024,
                chunk_size: 256,
                delay_ms: 0,
                error_status: None,
            },
            h,
        )
        .await;
    });
    origin_mock::wait_for_tcp(origin).await;

    let cl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ctrl = cl.local_addr().unwrap();
    tokio::spawn(async move {
        let mut config = ControlConfig::default();
        config.stale_after = Duration::from_secs(5);
        config.replica_factor = 2;
        control_plane::serve(cl, config).await.unwrap();
    });
    control_plane::wait_for_tcp(ctrl).await;
    let control_plane = format!("http://{ctrl}");

    for (id, price) in [("edge-a", 1.2), ("edge-b", 0.8), ("edge-c", 1.5)] {
        let el = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = el.local_addr().unwrap();
        let cp = control_plane.clone();
        let node_id = id.to_string();
        tokio::spawn(async move {
            edge_node::serve_with(
                el,
                EdgeConfig {
                    node_id,
                    origin,
                    listen: addr,
                    cache_max_bytes: 1024 * 1024,
                    cache_max_object_bytes: 1024 * 1024,
                    bandwidth_price: price,
                    capacity: 64,
                    ewma_rtt_ms: 5.0,
                    control_plane: Some(cp),
                },
            )
            .await
            .unwrap();
        });
        origin_mock::wait_for_tcp(addr).await;
    }

    let client = reqwest::Client::new();
    let mut registered = false;
    for _ in 0..40 {
        let nodes: serde_json::Value = client
            .get(format!("{control_plane}/nodes"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if nodes.as_array().map(|a| a.len()).unwrap_or(0) >= 3 {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(registered, "edges did not heartbeat into control plane");

    let path = "/obj/hot?size=1024";
    let loc1: serde_json::Value = client
        .get(format!("{control_plane}/locate?key=/obj/hot"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let addr1 = loc1["addr"].as_str().unwrap();
    let res = client
        .get(format!("http://{addr1}{path}"))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());
    assert_eq!(res.bytes().await.unwrap().len(), 1024);

    tokio::time::sleep(Duration::from_millis(300)).await;

    let loc2: serde_json::Value = client
        .get(format!("{control_plane}/locate?key=/obj/hot"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let addr2 = loc2["addr"].as_str().unwrap();
    let res = client
        .get(format!("http://{addr2}{path}"))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());
    assert_eq!(res.bytes().await.unwrap().len(), 1024);

    let origin_hits = handle.path_hits("/obj/hot");
    assert!(origin_hits >= 1);
    if loc2["addr"] == loc1["addr"] {
        assert_eq!(origin_hits, 1);
    }
}
