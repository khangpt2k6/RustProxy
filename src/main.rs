mod admin;
mod config;
mod health;
mod metrics;
mod pool;
mod proxy;

use clap::Parser;
use config::Config;
use health::HealthChecker;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics::Metrics;
use pool::BackendPool;
use proxy::ProxyServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Parser)]
#[command(name = "rustproxy", about = "high performance async reverse proxy")]
struct Args {
    /// path to the yaml config file
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let cfg = Config::load(&args.config)?;
    info!(
        "loaded config: {} backends, strategy {:?}",
        cfg.backends.len(),
        cfg.strategy
    );

    let pool = Arc::new(BackendPool::new(
        cfg.backends.iter().map(|b| b.addr.clone()).collect(),
        cfg.strategy,
    ));
    let metrics = Metrics::new();

    // health checker
    let checker = HealthChecker::new(Arc::clone(&pool), Arc::clone(&metrics), cfg.health_check);
    tokio::spawn(checker.run());

    // metrics endpoint
    let metrics_addr: SocketAddr = cfg.metrics_listen.parse()?;
    tokio::spawn(serve_metrics(Arc::clone(&metrics), metrics_addr));

    // gRPC admin
    let admin_addr: SocketAddr = cfg.admin_listen.parse()?;
    tokio::spawn(admin::run(Arc::clone(&pool), admin_addr));

    // main proxy loop
    let listen: SocketAddr = cfg.listen.parse()?;
    let server = ProxyServer::new(pool, metrics);
    server.run(listen).await
}

async fn serve_metrics(metrics: Arc<Metrics>, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("metrics listening on {addr}");
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let metrics = Arc::clone(&metrics);
                async move {
                    let resp = match req.uri().path() {
                        "/metrics" => Response::builder()
                            .header("content-type", "text/plain; version=0.0.4")
                            .body(Full::new(Bytes::from(metrics.render())))
                            .unwrap(),
                        _ => Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Full::new(Bytes::from_static(b"not found")))
                            .unwrap(),
                    };
                    Ok::<_, hyper::Error>(resp)
                }
            });
            http1::Builder::new().serve_connection(io, svc).await.ok();
        });
    }
}
