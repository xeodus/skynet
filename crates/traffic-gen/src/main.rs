use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
struct Locate {
    node_id: String,
    addr: String,
}

#[derive(Deserialize)]
struct NodeRow {
    node_id: String,
    addr: String,
}

#[derive(Deserialize)]
struct OriginStats {
    total_hits: u64,
}

#[tokio::main]
async fn main() {
    let locate_base = std::env::var("LOCATE").unwrap_or_else(|_| "http://127.0.0.1:18090".into());
    let origin = std::env::var("ORIGIN").unwrap_or_else(|_| "127.0.0.1:18080".into());
    let n: usize = env_parse("REQUESTS", 200);
    let keys: usize = env_parse("KEYS", 20);
    let size: usize = env_parse("SIZE", 8192);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let started = Instant::now();
    let mut ok = 0u64;
    let mut fail = 0u64;
    let mut latencies = Vec::with_capacity(n);
    let mut steered: HashMap<String, u64> = HashMap::new();

    for i in 0..n {
        let key_id = zipf(i, keys);
        let path = format!("/obj/{key_id}?size={size}");
        let locate_url = format!(
            "{}/locate?key=/obj/{}",
            locate_base.trim_end_matches('/'),
            key_id
        );
        let t0 = Instant::now();
        let result: Result<bool, reqwest::Error> = async {
            let loc: Locate = client.get(&locate_url).send().await?.json().await?;
            *steered.entry(loc.node_id).or_insert(0) += 1;
            let url = format!("http://{}{}", loc.addr, path);
            let res = client.get(url).send().await?;
            let status = res.status();
            let _ = res.bytes().await?;
            Ok(status.is_success())
        }
        .await;

        latencies.push(t0.elapsed());
        match result {
            Ok(true) => ok += 1,
            _ => fail += 1,
        }
    }

    latencies.sort();
    let p50 = percentile(&latencies, 0.50);
    let p99 = percentile(&latencies, 0.99);

    let nodes = load_nodes(&client, &locate_base).await;
    let mut hits = 0.0;
    let mut misses = 0.0;
    let mut origin_fetches = 0.0;
    let mut bytes = 0.0;
    let mut cost = 0.0;
    let mut per_node = Vec::new();

    for node in &nodes {
        let text = match client
            .get(format!("http://{}/__metrics", node.addr))
            .send()
            .await
        {
            Ok(res) => res.text().await.unwrap_or_default(),
            Err(_) => String::new(),
        };
        let h = scrape(&text, "edge_cache_hits_total");
        let m = scrape(&text, "edge_cache_misses_total");
        let o = scrape(&text, "edge_origin_fetches_total");
        let b = scrape(&text, "edge_bytes_served_total");
        let c = scrape(&text, "edge_cost_units_total");
        hits += h;
        misses += m;
        origin_fetches += o;
        bytes += b;
        cost += c;
        per_node.push((node.node_id.clone(), h, o, b, c));
    }

    let origin_hits = match client
        .get(format!("http://{origin}/__stats"))
        .send()
        .await
    {
        Ok(res) => res
            .json::<OriginStats>()
            .await
            .map(|s| s.total_hits)
            .unwrap_or(0),
        Err(_) => 0,
    };

    let hit_ratio = if hits + misses > 0.0 {
        hits / (hits + misses)
    } else {
        0.0
    };
    let cost_per_byte = if bytes > 0.0 { cost / bytes } else { 0.0 };

    println!("proof");
    println!("  requests={n} ok={ok} fail={fail}");
    println!(
        "  p50_ms={:.3} p99_ms={:.3}",
        p50.as_secs_f64() * 1000.0,
        p99.as_secs_f64() * 1000.0
    );
    println!("  cache_hits={hits} cache_misses={misses} hit_ratio={hit_ratio:.4}");
    println!("  origin_fetches_edges={origin_fetches} origin_hits_origin={origin_hits}");
    println!("  bytes_served={bytes} cost_units={cost:.4} cost_per_byte={cost_per_byte:.6}");
    println!("  elapsed_ms={}", started.elapsed().as_millis());
    for (id, h, o, b, c) in &per_node {
        println!("  per_node: {id} hits={h} origin={o} bytes={b} cost={c:.4}");
    }
    let mut mix: Vec<_> = steered.into_iter().collect();
    mix.sort_by(|a, b| a.0.cmp(&b.0));
    for (id, count) in mix {
        println!("  steered: {id}={count}");
    }
}

async fn load_nodes(client: &reqwest::Client, locate_base: &str) -> Vec<NodeRow> {
    if let Ok(list) = env_edges() {
        return list;
    }
    let url = format!("{}/nodes", locate_base.trim_end_matches('/'));
    match client.get(url).send().await {
        Ok(res) => res.json::<Vec<NodeRow>>().await.unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

fn env_edges() -> Result<Vec<NodeRow>, ()> {
    let raw = std::env::var("EDGES").map_err(|_| ())?;
    if raw.is_empty() {
        return Err(());
    }
    Ok(raw
        .split(',')
        .filter(|s| !s.is_empty())
        .enumerate()
        .map(|(i, addr)| NodeRow {
            node_id: format!("edge-{i}"),
            addr: addr.trim().to_string(),
        })
        .collect())
}

fn scrape(text: &str, name: &str) -> f64 {
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        if rest.starts_with(' ') || rest.starts_with('\t') {
            return rest.trim().parse().unwrap_or(0.0);
        }
    }
    0.0
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn zipf(i: usize, keys: usize) -> usize {
    let keys = keys.max(1);
    let u = ((i as u64).wrapping_mul(2654435761) % 10_000) as f64 / 10_000.0;
    let inv = (1.0 - u).max(1e-6).powf(-1.0 / 0.8);
    (inv as usize) % keys
}

fn percentile(xs: &[Duration], p: f64) -> Duration {
    if xs.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((xs.len() as f64 - 1.0) * p).round() as usize;
    xs[idx.min(xs.len() - 1)]
}
