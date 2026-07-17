//! tiny http server used to demo / load test the proxy.
//! responds with its own id so you can see load balancing in action.

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("PORT").ok())
        .and_then(|p| p.parse().ok())
        .unwrap_or(9001);
    let name = std::env::var("BACKEND_NAME").unwrap_or_else(|_| format!("backend-{port}"));

    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    let listener = TcpListener::bind(addr).await?;
    println!("{name} listening on {addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let name = name.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let name = name.clone();
                async move {
                    let resp = match req.uri().path() {
                        "/health" => Response::new(Full::new(Bytes::from_static(b"ok"))),
                        _ => Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "text/plain")
                            .body(Full::new(Bytes::from(format!(
                                "hello from {name} ({})\n",
                                req.uri().path()
                            ))))
                            .unwrap(),
                    };
                    Ok::<_, hyper::Error>(resp)
                }
            });
            http1::Builder::new().serve_connection(io, svc).await.ok();
        });
    }
}
