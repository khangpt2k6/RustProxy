//! small cli for poking the grpc admin api.
//!
//! usage:
//!   cargo run --example admin_client -- list
//!   cargo run --example admin_client -- add http://127.0.0.1:9004 [weight]
//!   cargo run --example admin_client -- remove http://127.0.0.1:9004
//!   cargo run --example admin_client -- weight http://127.0.0.1:9004 5

pub mod pb {
    tonic::include_proto!("rustproxy.admin");
}

use pb::admin_client::AdminClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "list".into());
    let endpoint =
        std::env::var("ADMIN_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".into());

    let mut client = AdminClient::connect(endpoint).await?;

    match cmd.as_str() {
        "list" => {
            let resp = client
                .list_backends(pb::ListBackendsRequest {})
                .await?
                .into_inner();
            for b in resp.backends {
                println!(
                    "{}  healthy={}  active={}  weight={}",
                    b.addr, b.healthy, b.active_connections, b.weight
                );
            }
        }
        "add" => {
            let addr = args.next().expect("add needs an addr");
            let weight = args.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let resp = client
                .add_backend(pb::AddBackendRequest { addr, weight })
                .await?
                .into_inner();
            println!("added: {}", resp.added);
        }
        "weight" => {
            let addr = args.next().expect("weight needs an addr");
            let weight = args
                .next()
                .and_then(|s| s.parse().ok())
                .expect("weight needs a number");
            let resp = client
                .set_weight(pb::SetWeightRequest { addr, weight })
                .await?
                .into_inner();
            println!("updated: {}", resp.updated);
        }
        "remove" => {
            let addr = args.next().expect("remove needs an addr");
            let resp = client
                .remove_backend(pb::RemoveBackendRequest { addr })
                .await?
                .into_inner();
            println!("removed: {}", resp.removed);
        }
        other => eprintln!("unknown command: {other} (use list|add|remove|weight)"),
    }
    Ok(())
}
