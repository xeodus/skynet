use prometheus::{
    Counter, Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder,
};
use std::time::Duration;

#[derive(Clone)]
pub struct EdgeMetrics {
    registry: Registry,
    bytes_served_total: IntCounter,
    request_duration_seconds: Histogram,
    cache_hits_total: IntCounter,
    cache_misses_total: IntCounter,
    origin_fetches_total: IntCounter,
    inflight_coalesced_total: IntCounter,
    cache_evictions_total: IntCounter,
    cache_bytes: IntGauge,
    inflight_requests: IntGauge,
    cost_units_total: Counter,
}

impl EdgeMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let bytes_served_total = IntCounter::new(
            "edge_bytes_served_total",
            "Total response bytes actually served by this edge node",
        )
        .unwrap();

        let buckets = vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

        let request_duration_seconds = Histogram::with_opts(
            HistogramOpts::new("edge_request_duration_seconds", "Edge request latency")
                .buckets(buckets),
        )
        .expect("valid histogram");

        let cache_hits_total =
            IntCounter::new("edge_cache_hits_total", "Cache hits").unwrap();
        let cache_misses_total =
            IntCounter::new("edge_cache_misses_total", "Cache misses that led origin fetch")
                .unwrap();
        let origin_fetches_total =
            IntCounter::new("edge_origin_fetches_total", "Origin fetches issued").unwrap();
        let inflight_coalesced_total = IntCounter::new(
            "edge_inflight_coalesced_total",
            "Requests that waited on an in-flight origin fetch",
        )
        .unwrap();
        let cache_evictions_total =
            IntCounter::new("edge_cache_evictions_total", "LRU evictions").unwrap();
        let cache_bytes = IntGauge::new("edge_cache_bytes", "Bytes currently stored in cache").unwrap();
        let inflight_requests =
            IntGauge::new("edge_inflight_requests", "In-flight client requests").unwrap();
        let cost_units_total = Counter::new(
            "edge_cost_units_total",
            "Synthetic bandwidth cost: bytes served times node bandwidth_price",
        )
        .unwrap();

        for metric in [
            Box::new(bytes_served_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(request_duration_seconds.clone()),
            Box::new(cache_hits_total.clone()),
            Box::new(cache_misses_total.clone()),
            Box::new(origin_fetches_total.clone()),
            Box::new(inflight_coalesced_total.clone()),
            Box::new(cache_evictions_total.clone()),
            Box::new(cache_bytes.clone()),
            Box::new(inflight_requests.clone()),
            Box::new(cost_units_total.clone()),
        ] {
            registry.register(metric).expect("register metric");
        }

        Self {
            registry,
            bytes_served_total,
            request_duration_seconds,
            cache_hits_total,
            cache_misses_total,
            origin_fetches_total,
            inflight_coalesced_total,
            cache_evictions_total,
            cache_bytes,
            inflight_requests,
            cost_units_total,
        }
    }

    pub fn add_bytes(&self, bytes: u64, bandwidth_price: f64) {
        self.bytes_served_total.inc_by(bytes);
        if bytes > 0 {
            self.cost_units_total.inc_by(bytes as f64 * bandwidth_price);
        }
    }

    pub fn observe_latency(&self, latency: Duration) {
        self.request_duration_seconds.observe(latency.as_secs_f64());
    }

    pub fn hit(&self) {
        self.cache_hits_total.inc();
    }

    pub fn miss(&self) {
        self.cache_misses_total.inc();
    }

    pub fn origin_fetch(&self) {
        self.origin_fetches_total.inc();
    }

    pub fn coalesced(&self) {
        self.inflight_coalesced_total.inc();
    }

    pub fn add_evictions(&self, n: u64) {
        self.cache_evictions_total.inc_by(n);
    }

    pub fn set_cache_bytes(&self, bytes: u64) {
        self.cache_bytes.set(bytes as i64);
    }

    pub fn inc_inflight(&self) {
        self.inflight_requests.inc();
    }

    pub fn dec_inflight(&self) {
        self.inflight_requests.dec();
    }

    pub fn inflight(&self) -> u64 {
        self.inflight_requests.get().max(0) as u64
    }

    pub fn hits(&self) -> u64 {
        self.cache_hits_total.get()
    }

    pub fn misses(&self) -> u64 {
        self.cache_misses_total.get()
    }

    pub fn render(&self) -> String {
        let mut buffer = Vec::new();
        TextEncoder
            .encode(&self.registry.gather(), &mut buffer)
            .expect("prometheus encoding shouldn't fail");

        String::from_utf8(buffer).expect("prometheus output should be utf-8")
    }
}

impl Default for EdgeMetrics {
    fn default() -> Self {
        Self::new()
    }
}
