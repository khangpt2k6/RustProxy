//! small cli for poking the grpc admin api.
//!
//! usage:
//!   cargo run --example admin_client -- list
//!   cargo run --example admin_client -- add http://127.0.0.1:9004
//!   cargo run --example admin_client -- remove http://127.0.0.1:9004

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
                    "{}  healthy={}  active={}",
                    b.addr, b.healthy, b.active_connections
                );
            }
        }
        "add" => {
            let addr = args.next().expect("add needs an addr");
            let resp = client
                .add_backend(pb::AddBackendRequest { addr })
                .await?
                .into_inner();
            println!("added: {}", resp.added);
        }
        "remove" => {
            let addr = args.next().expect("remove needs an addr");
            let resp = client
                .remove_backend(pb::RemoveBackendRequest { addr })
                .await?
                .into_inner();
            println!("removed: {}", resp.removed);
        }
        other => eprintln!("unknown command: {other} (use list|add|remove)"),
    }
    Ok(())
}
