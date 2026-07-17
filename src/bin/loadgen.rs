//! bare-bones load generator so the proxy can be benched without external tools.
//!
//! usage: loadgen [url] [concurrency] [duration_secs]
//!   loadgen http://127.0.0.1:8080/ 512 15

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::Uri;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Stats {
    ok: AtomicU64,
    err: AtomicU64,
    // latency histogram in microseconds, fixed buckets
    lat_us: Vec<AtomicU64>,
}

const BUCKETS_US: &[u64] = &[
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, u64::MAX,
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url: Uri = args
        .next()
        .unwrap_or_else(|| "http://127.0.0.1:8080/".into())
        .parse()?;
    let concurrency: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(256);
    let duration = Duration::from_secs(args.next().and_then(|s| s.parse().ok()).unwrap_or(10));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(run(url, concurrency, duration))
}

async fn run(url: Uri, concurrency: usize, duration: Duration) -> Result<(), Box<dyn std::error::Error>> {
    println!("target={url} concurrency={concurrency} duration={duration:?}");

    let stats = Arc::new(Stats {
        ok: AtomicU64::new(0),
        err: AtomicU64::new(0),
        lat_us: BUCKETS_US.iter().map(|_| AtomicU64::new(0)).collect(),
    });

    let client: Client<HttpConnector, Empty<Bytes>> = Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(concurrency)
        .build_http();

    let start = Instant::now();
    let deadline = start + duration;

    let workers: Vec<_> = (0..concurrency)
        .map(|_| {
            let client = client.clone();
            let url = url.clone();
            let stats = Arc::clone(&stats);
            tokio::spawn(async move {
                while Instant::now() < deadline {
                    let t0 = Instant::now();
                    let req = hyper::Request::get(url.clone())
                        .body(Empty::<Bytes>::new())
                        .unwrap();
                    match client.request(req).await {
                        Ok(resp) => {
                            // drain the body so the connection can be reused
                            let _ = resp.into_body().collect().await;
                            let us = t0.elapsed().as_micros() as u64;
                            let idx = BUCKETS_US.iter().position(|b| us <= *b).unwrap();
                            stats.lat_us[idx].fetch_add(1, Ordering::Relaxed);
                            stats.ok.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            stats.err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for w in workers {
        w.await.ok();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let ok = stats.ok.load(Ordering::Relaxed);
    let err = stats.err.load(Ordering::Relaxed);
    println!("requests: {ok} ok, {err} err in {elapsed:.1}s");
    println!("throughput: {:.0} req/s", ok as f64 / elapsed);

    // rough percentiles from the histogram
    let counts: Vec<u64> = stats.lat_us.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    let total: u64 = counts.iter().sum();
    if total > 0 {
        for (label, q) in [("p50", 0.50), ("p90", 0.90), ("p99", 0.99)] {
            let target = (total as f64 * q) as u64;
            let mut acc = 0;
            for (i, c) in counts.iter().enumerate() {
                acc += c;
                if acc >= target {
                    let b = BUCKETS_US[i];
                    if b == u64::MAX {
                        println!("{label}: >1s");
                    } else {
                        println!("{label}: <={:.1}ms", b as f64 / 1000.0);
                    }
                    break;
                }
            }
        }
    }
    Ok(())
}
