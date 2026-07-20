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
                weight: b.weight(),
            })
            .collect();
        Ok(Response::new(pb::ListBackendsResponse { backends }))
    }

    async fn add_backend(
        &self,
        req: Request<pb::AddBackendRequest>,
    ) -> Result<Response<pb::AddBackendResponse>, Status> {
        let req = req.into_inner();
        let addr = req.addr;
        if !addr.starts_with("http://") && !addr.starts_with("https://") {
            return Err(Status::invalid_argument("addr must start with http:// or https://"));
        }
        // 0 means "caller didn't set one" -> default to an equal share
        let weight = if req.weight == 0 { 1 } else { req.weight };
        let added = self.pool.add(addr.clone(), weight);
        if added {
            info!("admin: added backend {addr} (weight {weight})");
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

    async fn set_weight(
        &self,
        req: Request<pb::SetWeightRequest>,
    ) -> Result<Response<pb::SetWeightResponse>, Status> {
        let req = req.into_inner();
        let updated = self.pool.set_weight(&req.addr, req.weight);
        if updated {
            info!("admin: set weight of {} to {}", req.addr, req.weight);
        }
        Ok(Response::new(pb::SetWeightResponse { updated }))
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
