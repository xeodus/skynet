use control_plane::{ControlConfig, Heartbeat};
use std::time::Duration;

async fn spawn_control(stale: Duration, replica_factor: usize) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut config = ControlConfig::default();
        config.stale_after = stale;
        config.replica_factor = replica_factor;
        control_plane::serve(listener, config).await.unwrap();
    });
    control_plane::wait_for_tcp(addr).await;
    addr
}

fn beat(id: &str, addr: &str, price: f64) -> Heartbeat {
    Heartbeat {
        node_id: id.into(),
        addr: addr.into(),
        healthy: true,
        inflight: 0,
        capacity: 10,
        cache_bytes: 0,
        hits: 0,
        misses: 0,
        bandwidth_price: price,
        ewma_rtt_ms: 10.0,
        hot_keys: vec![],
    }
}

#[tokio::test]
async fn failover_reroutes_to_a_live_peer() {
    let ctrl = spawn_control(Duration::from_millis(300), 3).await;
    let client = reqwest::Client::new();
    let url = format!("http://{ctrl}/heartbeat");

    for (id, addr, price) in [
        ("edge-a", "10.0.0.1:80", 1.0),
        ("edge-b", "10.0.0.2:80", 1.0),
        ("edge-c", "10.0.0.3:80", 1.0),
    ] {
        client
            .post(&url)
            .json(&beat(id, addr, price))
            .send()
            .await
            .unwrap();
    }

    let first: serde_json::Value = client
        .get(format!("http://{ctrl}/locate?path=/obj/x"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let chosen = first["node_id"].as_str().unwrap().to_string();
    assert!(["edge-a", "edge-b", "edge-c"].contains(&chosen.as_str()));

    let survivors: Vec<_> = ["edge-a", "edge-b", "edge-c"]
        .into_iter()
        .filter(|id| *id != chosen.as_str())
        .collect();

    for _ in 0..8 {
        tokio::time::sleep(Duration::from_millis(80)).await;
        for id in &survivors {
            let addr = match *id {
                "edge-a" => "10.0.0.1:80",
                "edge-b" => "10.0.0.2:80",
                "edge-c" => "10.0.0.3:80",
                _ => unreachable!(),
            };
            client
                .post(&url)
                .json(&beat(id, addr, 1.0))
                .send()
                .await
                .unwrap();
        }
    }

    let second = client
        .get(format!("http://{ctrl}/locate?path=/obj/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = second.json().await.unwrap();
    let next = body["node_id"].as_str().unwrap();
    assert_ne!(next, chosen.as_str());
    assert!(survivors.contains(&next));
}
