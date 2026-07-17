use crate::config::HealthCheckConfig;
use crate::metrics::Metrics;
use crate::pool::BackendPool;
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::Uri;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

pub struct HealthChecker {
    pool: Arc<BackendPool>,
    metrics: Arc<Metrics>,
    cfg: HealthCheckConfig,
    client: Client<HttpConnector, Empty<Bytes>>,
}

impl HealthChecker {
    pub fn new(pool: Arc<BackendPool>, metrics: Arc<Metrics>, cfg: HealthCheckConfig) -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self {
            pool,
            metrics,
            cfg,
            client,
        }
    }

    pub async fn run(self) {
        let mut tick = tokio::time::interval(Duration::from_secs(self.cfg.interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            self.probe_all().await;
        }
    }

    async fn probe_all(&self) {
        let backends = self.pool.snapshot();
        let timeout = Duration::from_secs(self.cfg.timeout_secs);

        // probe everything concurrently so one slow backend can't delay the rest
        let handles: Vec<_> = backends
            .iter()
            .map(|b| {
                let url: Result<Uri, _> = format!("{}{}", b.addr, self.cfg.path).parse();
                let client = self.client.clone();
                tokio::spawn(async move {
                    let Ok(url) = url else { return false };
                    let req = hyper::Request::get(url)
                        .body(Empty::<Bytes>::new())
                        .expect("static request");
                    match tokio::time::timeout(timeout, client.request(req)).await {
                        Ok(Ok(resp)) => resp.status().is_success(),
                        _ => false,
                    }
                })
            })
            .collect();
        let mut results = Vec::with_capacity(handles.len());
        for h in handles {
            results.push(h.await.unwrap_or(false));
        }

        let mut healthy = 0i64;
        for (backend, ok) in backends.iter().zip(results) {
            match backend.record_probe(ok, self.cfg.fall, self.cfg.rise) {
                Some(true) => info!("backend {} is back up", backend.addr),
                Some(false) => warn!("backend {} marked down", backend.addr),
                None => {}
            }
            if backend.is_healthy() {
                healthy += 1;
            }
        }
        self.metrics.healthy_backends.set(healthy);
    }
}
