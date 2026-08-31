use control_plane::{ControlConfig, Heartbeat};
use std::time::Duration;

async fn spawn_control(stale: Duration) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut config = ControlConfig::default();
        config.stale_after = stale;
        config.replica_factor = 2;
        control_plane::serve(listener, config).await.unwrap();
    });
    control_plane::wait_for_tcp(addr).await;
    addr
}

fn beat(id: &str, addr: &str, price: f64, healthy: bool, inflight: u64, hot: &[&str]) -> Heartbeat {
    Heartbeat {
        node_id: id.into(),
        addr: addr.into(),
        healthy,
        inflight,
        capacity: 10,
        cache_bytes: 0,
        hits: 0,
        misses: 0,
        bandwidth_price: price,
        ewma_rtt_ms: 10.0,
        hot_keys: hot.iter().map(|s| s.to_string()).collect(),
    }
}

#[tokio::test]
async fn phase5_locate_picks_viable_node() {
    let ctrl = spawn_control(Duration::from_secs(5)).await;
    let client = reqwest::Client::new();
    let url = format!("http://{ctrl}/heartbeat");

    client
        .post(&url)
        .json(&beat("edge-a", "10.0.0.1:80", 9.0, true, 0, &[]))
        .send()
        .await
        .unwrap();
    client
        .post(&url)
        .json(&beat("edge-b", "10.0.0.2:80", 1.0, true, 0, &[]))
        .send()
        .await
        .unwrap();
    client
        .post(&url)
        .json(&beat("edge-c", "10.0.0.3:80", 0.1, false, 0, &[]))
        .send()
        .await
        .unwrap();

    let loc: serde_json::Value = client
        .get(format!("http://{ctrl}/locate?path=/obj/x"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_ne!(loc["node_id"], "edge-c");
    assert!(loc["addr"].as_str().unwrap().starts_with("10.0.0."));
}

#[tokio::test]
async fn phase5_hit_hint_can_override_price() {
    let ctrl = spawn_control(Duration::from_secs(5)).await;
    let client = reqwest::Client::new();
    let url = format!("http://{ctrl}/heartbeat");

    client
        .post(&url)
        .json(&beat("cheap", "10.0.0.1:80", 1.0, true, 0, &[]))
        .send()
        .await
        .unwrap();
    client
        .post(&url)
        .json(&beat(
            "warm",
            "10.0.0.2:80",
            1.2,
            true,
            0,
            &["/obj/hot"],
        ))
        .send()
        .await
        .unwrap();

    let loc: serde_json::Value = client
        .get(format!("http://{ctrl}/locate?path=/obj/hot"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(loc["node_id"], "warm");
}

#[tokio::test]
async fn phase5_stale_heartbeat_is_dropped() {
    let ctrl = spawn_control(Duration::from_millis(200)).await;
    let client = reqwest::Client::new();
    client
        .post(format!("http://{ctrl}/heartbeat"))
        .json(&beat("only", "10.0.0.9:80", 1.0, true, 0, &[]))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(400)).await;

    let res = client
        .get(format!("http://{ctrl}/locate?path=/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
}
