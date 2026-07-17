use crate::pool::BackendPool;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

pub mod pb {
    tonic::include_proto!("rustproxy.admin");
}

use pb::admin_server::{Admin, AdminServer};

pub struct AdminService {
    pool: Arc<BackendPool>,
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn list_backends(
        &self,
        _req: Request<pb::ListBackendsRequest>,
    ) -> Result<Response<pb::ListBackendsResponse>, Status> {
        let backends = self
            .pool
            .snapshot()
            .iter()
            .map(|b| pb::BackendInfo {
                addr: b.addr.clone(),
                healthy: b.is_healthy(),
                active_connections: b.active_conns() as u64,
            })
            .collect();
        Ok(Response::new(pb::ListBackendsResponse { backends }))
    }

    async fn add_backend(
        &self,
        req: Request<pb::AddBackendRequest>,
    ) -> Result<Response<pb::AddBackendResponse>, Status> {
        let addr = req.into_inner().addr;
        if !addr.starts_with("http://") && !addr.starts_with("https://") {
            return Err(Status::invalid_argument("addr must start with http:// or https://"));
        }
        let added = self.pool.add(addr.clone());
        if added {
            info!("admin: added backend {addr}");
        }
        Ok(Response::new(pb::AddBackendResponse { added }))
    }

    async fn remove_backend(
        &self,
        req: Request<pb::RemoveBackendRequest>,
    ) -> Result<Response<pb::RemoveBackendResponse>, Status> {
        let addr = req.into_inner().addr;
        let removed = self.pool.remove(&addr);
        if removed {
            info!("admin: removed backend {addr}");
        }
        Ok(Response::new(pb::RemoveBackendResponse { removed }))
    }
}

pub async fn run(pool: Arc<BackendPool>, listen: SocketAddr) -> anyhow::Result<()> {
    info!("gRPC admin listening on {listen}");
    Server::builder()
        .add_service(AdminServer::new(AdminService { pool }))
        .serve(listen)
        .await?;
    Ok(())
}
