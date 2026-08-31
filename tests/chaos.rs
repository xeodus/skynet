use edge_node::EdgeConfig;
use origin_mock::{OriginConfig, OriginHandle};
use control_plane::{ControlConfig, Heartbeat};

#[tokio::test]
async fn chaos_origin_5xx_is_not_cached() {
    let handle = OriginHandle::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = listener.local_addr().unwrap();
    let h = handle.clone();
    tokio::spawn(async move {
        origin_mock::run(
            listener,
            OriginConfig {
                default_size: 32,
                chunk_size: 16,
                delay_ms: 0,
                error_status: Some(500),
            },
            h,
        )
        .await;
    });
    origin_mock::wait_for_tcp(origin).await;

    let el = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let edge = el.local_addr().unwrap();
    tokio::spawn(async move {
        edge_node::serve_with(
            el,
            EdgeConfig {
                node_id: "e".into(),
                origin,
                listen: edge,
                cache_max_bytes: 1024 * 1024,
                cache_max_object_bytes: 1024 * 1024,
                bandwidth_price: 1.0,
                capacity: 8,
                ewma_rtt_ms: 1.0,
                control_plane: None,
            },
        )
        .await
        .unwrap();
    });
    origin_mock::wait_for_tcp(edge).await;

    for _ in 0..2 {
        let res = reqwest::get(format!("http://{edge}/obj/fail")).await.unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
        let _ = res.bytes().await;
    }
    assert_eq!(handle.path_hits("/obj/fail"), 2);
}

#[tokio::test]
async fn chaos_over_capacity_is_not_chosen() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ctrl = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut config = ControlConfig::default();
        config.replica_factor = 3;
        control_plane::serve(listener, config).await.unwrap();
    });
    control_plane::wait_for_tcp(ctrl).await;

    let client = reqwest::Client::new();
    let full = Heartbeat {
        node_id: "full".into(),
        addr: "10.0.0.1:80".into(),
        healthy: true,
        inflight: 10,
        capacity: 10,
        cache_bytes: 0,
        hits: 0,
        misses: 0,
        bandwidth_price: 0.01,
        ewma_rtt_ms: 1.0,
        hot_keys: vec![],
    };
    let free = Heartbeat {
        node_id: "free".into(),
        addr: "10.0.0.2:80".into(),
        healthy: true,
        inflight: 0,
        capacity: 10,
        cache_bytes: 0,
        hits: 0,
        misses: 0,
        bandwidth_price: 9.0,
        ewma_rtt_ms: 1.0,
        hot_keys: vec![],
    };
    client
        .post(format!("http://{ctrl}/heartbeat"))
        .json(&full)
        .send()
        .await
        .unwrap();
    client
        .post(format!("http://{ctrl}/heartbeat"))
        .json(&free)
        .send()
        .await
        .unwrap();

    let loc: serde_json::Value = client
        .get(format!("http://{ctrl}/locate?path=/x"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(loc["node_id"], "free");
}

#[tokio::test]
async fn chaos_dns_returns_a_record() {
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http.local_addr().unwrap();
    let dns = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dns_addr = dns.local_addr().unwrap();
    tokio::spawn(async move {
        control_plane::serve_with_dns(http, dns, ControlConfig::default())
            .await
            .unwrap();
    });
    control_plane::wait_for_tcp(http_addr).await;

    let beat = Heartbeat {
        node_id: "edge-a".into(),
        addr: "10.1.2.3:18081".into(),
        healthy: true,
        inflight: 0,
        capacity: 10,
        cache_bytes: 0,
        hits: 0,
        misses: 0,
        bandwidth_price: 1.0,
        ewma_rtt_ms: 1.0,
        hot_keys: vec![],
    };
    reqwest::Client::new()
        .post(format!("http://{http_addr}/heartbeat"))
        .json(&beat)
        .send()
        .await
        .unwrap();

    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let query = dns_query("cdn.local");
    sock.send_to(&query, dns_addr).await.unwrap();
    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.recv(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(n > 12);
    assert_eq!(&buf[n - 4..n], &[10, 1, 2, 3]);
}

fn dns_query(name: &str) -> Vec<u8> {
    let mut q = vec![0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    for label in name.split('.') {
        q.push(label.len() as u8);
        q.extend_from_slice(label.as_bytes());
    }
    q.push(0);
    q.extend_from_slice(&1u16.to_be_bytes());
    q.extend_from_slice(&1u16.to_be_bytes());
    q
}

use std::time::Duration;
