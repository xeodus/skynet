use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub node_id: String,
    pub addr: String,
    pub healthy: bool,
    pub inflight: u64,
    pub capacity: u64,
    pub cache_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub bandwidth_price: f64,
    pub ewma_rtt_ms: f64,
    pub has_key: bool,
}

#[derive(Clone, Debug)]
pub struct Weights {
    pub price: f64,
    pub latency: f64,
    pub load: f64,
    pub hit: f64,
}

impl Default for Weights {
    fn default() -> Self {
        Self {
            price: 1.0,
            latency: 0.01,
            load: 1.0,
            hit: 5.0,
        }
    }
}

pub fn viable(node: &NodeSnapshot) -> bool {
    node.healthy && node.inflight < node.capacity
}

pub fn utilization(node: &NodeSnapshot) -> f64 {
    if node.capacity == 0 {
        1.0
    } else {
        node.inflight as f64 / node.capacity as f64
    }
}

pub fn score(node: &NodeSnapshot, weights: &Weights) -> f64 {
    let hit = if node.has_key { 1.0 } else { 0.0 };
    weights.price * node.bandwidth_price
        + weights.latency * node.ewma_rtt_ms
        + weights.load * utilization(node)
        - weights.hit * hit
}

fn rendezvous_hash(key: &str, node_id: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    node_id.hash(&mut hasher);
    hasher.finish()
}

/// Rendezvous hashing: the R nodes with the highest hash(key, node) values.
pub fn replica_set<'a>(key: &str, node_ids: &[&'a str], r: usize) -> Vec<&'a str> {
    if node_ids.is_empty() || r == 0 {
        return Vec::new();
    }
    let mut ranked: Vec<(u64, &str)> = node_ids
        .iter()
        .map(|id| (rendezvous_hash(key, id), *id))
        .collect();
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    ranked.into_iter().map(|(_, id)| id).take(r.min(node_ids.len())).collect()
}

pub fn choose<'a>(
    key: &str,
    nodes: &'a [NodeSnapshot],
    weights: &Weights,
    replica_factor: usize,
) -> Option<&'a NodeSnapshot> {
    if nodes.is_empty() {
        return None;
    }

    let ids: Vec<&str> = nodes.iter().map(|n| n.node_id.as_str()).collect();
    let replicas = replica_set(key, &ids, replica_factor.max(1));

    let mut candidates: Vec<&NodeSnapshot> = nodes
        .iter()
        .filter(|n| replicas.contains(&n.node_id.as_str()) && viable(n))
        .collect();

    if candidates.is_empty() {
        candidates = nodes.iter().filter(|n| viable(n)).collect();
    }

    candidates.into_iter().min_by(|a, b| {
        score(a, weights)
            .partial_cmp(&score(b, weights))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.node_id.cmp(&b.node_id))
    })
}

fn pick_min<'a, F>(nodes: &'a [NodeSnapshot], mut key: F) -> Option<&'a NodeSnapshot>
where
    F: FnMut(&NodeSnapshot) -> f64,
{
    nodes
        .iter()
        .filter(|n| viable(n))
        .min_by(|a, b| {
            key(a)
                .partial_cmp(&key(b))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.node_id.cmp(&b.node_id))
        })
}

/// Lowest bandwidth_price among viable nodes. Ignores cache locality.
pub fn choose_cheapest(nodes: &[NodeSnapshot]) -> Option<&NodeSnapshot> {
    pick_min(nodes, |n| n.bandwidth_price)
}

/// Lowest ewma_rtt_ms among viable nodes.
pub fn choose_nearest(nodes: &[NodeSnapshot]) -> Option<&NodeSnapshot> {
    pick_min(nodes, |n| n.ewma_rtt_ms)
}

/// First viable node in the rendezvous replica set. No cost or hit hint.
pub fn choose_hash_only<'a>(
    key: &str,
    nodes: &'a [NodeSnapshot],
    replica_factor: usize,
) -> Option<&'a NodeSnapshot> {
    let ids: Vec<&str> = nodes.iter().map(|n| n.node_id.as_str()).collect();
    let replicas = replica_set(key, &ids, replica_factor.max(1));
    for id in replicas {
        if let Some(n) = nodes.iter().find(|n| n.node_id == id && viable(n)) {
            return Some(n);
        }
    }
    nodes.iter().filter(|n| viable(n)).min_by(|a, b| a.node_id.cmp(&b.node_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, price: f64, healthy: bool, inflight: u64, has_key: bool) -> NodeSnapshot {
        NodeSnapshot {
            node_id: id.into(),
            addr: format!("{id}:80"),
            healthy,
            inflight,
            capacity: 10,
            cache_bytes: 0,
            hits: 0,
            misses: 0,
            bandwidth_price: price,
            ewma_rtt_ms: 10.0,
            has_key,
        }
    }

    #[test]
    fn skips_unhealthy_and_over_capacity() {
        let nodes = vec![
            node("a", 0.1, false, 0, false),
            node("b", 5.0, true, 10, false),
            node("c", 1.0, true, 0, false),
        ];
        let chosen = choose("k", &nodes, &Weights::default(), 3).unwrap();
        assert_eq!(chosen.node_id, "c");
    }

    #[test]
    fn prefers_local_hit_hint() {
        let nodes = vec![
            node("cheap", 0.1, true, 0, false),
            node("warm", 1.0, true, 0, true),
        ];
        let mut weights = Weights::default();
        weights.hit = 100.0;
        weights.price = 1.0;
        let chosen = choose("obj", &nodes, &weights, 2).unwrap();
        assert_eq!(chosen.node_id, "warm");
    }

    #[test]
    fn replica_set_is_stable() {
        let ids = ["edge-a", "edge-b", "edge-c"];
        let a = replica_set("hot-key", &ids, 2);
        let b = replica_set("hot-key", &ids, 2);
        assert_eq!(a, b);
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn returns_none_when_none_viable() {
        let nodes = vec![node("a", 1.0, false, 0, false)];
        assert!(choose("k", &nodes, &Weights::default(), 1).is_none());
    }

    #[test]
    fn cheapest_among_replicas_when_no_hit() {
        let nodes = vec![
            node("a", 9.0, true, 0, false),
            node("b", 1.0, true, 0, false),
            node("c", 3.0, true, 0, false),
        ];
        let chosen = choose("only-price", &nodes, &Weights::default(), 3).unwrap();
        assert_eq!(chosen.node_id, "b");
    }

    fn node_rtt(id: &str, price: f64, rtt: f64, has_key: bool) -> NodeSnapshot {
        let mut n = node(id, price, true, 0, has_key);
        n.ewma_rtt_ms = rtt;
        n
    }

    #[test]
    fn cache_aware_choose_differs_from_cheapest_when_warm() {
        let nodes = vec![
            node_rtt("cheap", 0.1, 50.0, false),
            node_rtt("warm", 1.0, 10.0, true),
        ];
        let mut weights = Weights::default();
        weights.hit = 100.0;
        weights.price = 1.0;

        let cheapest = choose_cheapest(&nodes).unwrap();
        let nearest = choose_nearest(&nodes).unwrap();
        let hash = choose_hash_only("obj", &nodes, 2).unwrap();
        let product = choose("obj", &nodes, &weights, 2).unwrap();

        assert_eq!(cheapest.node_id, "cheap");
        assert_eq!(nearest.node_id, "warm");
        assert!(hash.node_id == "cheap" || hash.node_id == "warm");
        assert_eq!(product.node_id, "warm");
        assert_ne!(cheapest.node_id, product.node_id);
    }
}
