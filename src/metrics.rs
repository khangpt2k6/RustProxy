use prometheus::{
    Encoder, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Registry, TextEncoder,
};
use std::sync::Arc;

pub struct Metrics {
    pub registry: Registry,
    pub requests_total: IntCounterVec,
    pub request_duration: HistogramVec,
    pub active_connections: IntGaugeVec,
    pub healthy_backends: IntGauge,
    pub proxy_errors: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let requests_total = IntCounterVec::new(
            prometheus::opts!("rustproxy_requests_total", "Total proxied requests"),
            &["backend", "status"],
        )
        .unwrap();

        let request_duration = HistogramVec::new(
            prometheus::histogram_opts!(
                "rustproxy_request_duration_seconds",
                "End-to-end proxy latency",
                vec![0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
            ),
            &["backend"],
        )
        .unwrap();

        let active_connections = IntGaugeVec::new(
            prometheus::opts!(
                "rustproxy_active_connections",
                "In-flight requests per backend"
            ),
            &["backend"],
        )
        .unwrap();

        let healthy_backends = IntGauge::new(
            "rustproxy_healthy_backends",
            "Number of backends currently passing health checks",
        )
        .unwrap();

        let proxy_errors = IntCounterVec::new(
            prometheus::opts!("rustproxy_errors_total", "Proxy-side errors"),
            &["kind"],
        )
        .unwrap();

        registry.register(Box::new(requests_total.clone())).unwrap();
        registry
            .register(Box::new(request_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(active_connections.clone()))
            .unwrap();
        registry
            .register(Box::new(healthy_backends.clone()))
            .unwrap();
        registry.register(Box::new(proxy_errors.clone())).unwrap();

        Arc::new(Self {
            registry,
            requests_total,
            request_duration,
            active_connections,
            healthy_backends,
            proxy_errors,
        })
    }

    pub fn render(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .unwrap();
        buf
    }
}
