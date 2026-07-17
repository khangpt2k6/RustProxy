use crate::metrics::Metrics;
use crate::pool::{BackendPool, ConnGuard};
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::HeaderValue;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

type ProxyClient = Client<HttpConnector, Incoming>;

pub struct ProxyServer {
    pool: Arc<BackendPool>,
    metrics: Arc<Metrics>,
    client: ProxyClient,
}

impl ProxyServer {
    pub fn new(pool: Arc<BackendPool>, metrics: Arc<Metrics>) -> Arc<Self> {
        // http2 support in the client lets gRPC traffic pass through untouched
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            // keep plenty of warm upstream conns; churn under high concurrency
            // piles up TIME_WAIT sockets and tanks throughput
            .pool_max_idle_per_host(1024)
            .build_http();
        Arc::new(Self {
            pool,
            metrics,
            client,
        })
    }

    pub async fn run(self: Arc<Self>, listen: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(listen).await?;
        info!("proxy listening on {listen}");
        loop {
            let (stream, peer) = listener.accept().await?;
            stream.set_nodelay(true).ok();
            let io = TokioIo::new(stream);
            let this = Arc::clone(&self);
            tokio::spawn(async move {
                let svc = service_fn(move |req| {
                    let this = Arc::clone(&this);
                    async move { this.handle(req, peer).await }
                });
                if let Err(e) = http1::Builder::new()
                    .preserve_header_case(true)
                    .serve_connection(io, svc)
                    .with_upgrades()
                    .await
                {
                    debug!("connection from {peer} ended: {e}");
                }
            });
        }
    }

    async fn handle(
        &self,
        mut req: Request<Incoming>,
        peer: SocketAddr,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
        let start = Instant::now();

        let Some(backend) = self.pool.pick() else {
            warn!("no healthy backends available");
            self.metrics
                .proxy_errors
                .with_label_values(&["no_backend"])
                .inc();
            return Ok(status_response(StatusCode::SERVICE_UNAVAILABLE, "no healthy backends"));
        };

        let guard = ConnGuard::new(backend);
        let backend_addr = guard.backend().addr.clone();
        self.metrics
            .active_connections
            .with_label_values(&[&backend_addr])
            .inc();

        // rewrite the request URI to point at the chosen backend
        let path_and_query = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let target: Uri = match format!("{backend_addr}{path_and_query}").parse() {
            Ok(u) => u,
            Err(e) => {
                error!("bad target uri: {e}");
                self.metrics
                    .active_connections
                    .with_label_values(&[&backend_addr])
                    .dec();
                self.metrics
                    .proxy_errors
                    .with_label_values(&["bad_uri"])
                    .inc();
                return Ok(status_response(StatusCode::BAD_GATEWAY, "bad target uri"));
            }
        };
        *req.uri_mut() = target;

        // standard forwarding headers
        if let Ok(v) = HeaderValue::from_str(&peer.ip().to_string()) {
            req.headers_mut().append("x-forwarded-for", v);
        }

        let result = self.client.request(req).await;
        let elapsed = start.elapsed().as_secs_f64();

        self.metrics
            .active_connections
            .with_label_values(&[&backend_addr])
            .dec();
        self.metrics
            .request_duration
            .with_label_values(&[&backend_addr])
            .observe(elapsed);
        drop(guard);

        match result {
            Ok(resp) => {
                self.metrics
                    .requests_total
                    .with_label_values(&[&backend_addr, resp.status().as_str()])
                    .inc();
                Ok(resp.map(|b| b.boxed()))
            }
            Err(e) => {
                warn!("upstream {backend_addr} failed: {e}");
                self.metrics
                    .requests_total
                    .with_label_values(&[&backend_addr, "502"])
                    .inc();
                self.metrics
                    .proxy_errors
                    .with_label_values(&["upstream"])
                    .inc();
                Ok(status_response(StatusCode::BAD_GATEWAY, "upstream error"))
            }
        }
    }
}

fn status_response(
    status: StatusCode,
    msg: &'static str,
) -> Response<BoxBody<Bytes, hyper::Error>> {
    Response::builder()
        .status(status)
        .body(
            Full::new(Bytes::from_static(msg.as_bytes()))
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap()
}
