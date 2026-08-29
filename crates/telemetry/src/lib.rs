use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, Registry, TextEncoder,
};
use std::time::Duration;

#[derive(Clone)]
pub struct EdgeMetrics {
    registry: Registry,
    bytes_served_total: IntCounter,
    request_duration_seconds: Histogram,
}

impl EdgeMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let bytes_served_total = IntCounter::new(
            "edge_bytes_served_total",
            "Total HTTP response bytes actually served by this edge node",
        )
        .expect("bytes counter should be valid");

        let buckets = vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];

        let request_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "edge_request_duration_seconds",
                "Time spent serving edge requests",
            )
            .buckets(buckets),
        )
        .expect("histogram should be valid");

        registry
            .register(Box::new(bytes_served_total.clone()))
            .expect("bytes counter should register");

        registry
            .register(Box::new(request_duration_seconds.clone()))
            .expect("histogram should register");

        Self {
            registry,
            bytes_served_total,
            request_duration_seconds,
        }
    }

    pub fn add_bytes(&self, bytes: u64) {
        self.bytes_served_total.inc_by(bytes);
    }

    pub fn observe_latency(&self, latency: Duration) {
        self.request_duration_seconds
            .observe(latency.as_secs_f64());
    }

    pub fn render(&self) -> String {
        let mut buffer = Vec::new();

        TextEncoder
            .encode(&self.registry.gather(), &mut buffer)
            .expect("prometheus encoding should not fail");

        String::from_utf8(buffer).expect("prometheus output should be utf-8")
    }
}

impl Default for EdgeMetrics {
    fn default() -> Self {
        Self::new()
    }
}
