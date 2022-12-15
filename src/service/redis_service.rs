use std::net::SocketAddr;

use async_trait::async_trait;
use tonic::{transport::Server, Response};

use crate::{
    protobuf::{
        self,
        redis_service::{
            redis_service_server::RedisServiceServer, GetRequest, GetResponse, RemoveRequest,
            RemoveResponse, SetRequest, SetResponse,
        },
    },
    store::state::State,
};

/// Used for processing requests from clients.
pub struct RedisService {
    state: State,
}

impl RedisService {
    pub fn new() -> Self {
        Self {
            state: State::new(),
        }
    }

    pub async fn serve(self, addr: SocketAddr) {
        tokio::spawn(
            Server::builder()
                .add_service(RedisServiceServer::new(self))
                .serve(addr),
        );
    }
}

#[async_trait]
impl protobuf::redis_service::redis_service_server::RedisService for RedisService {
    async fn get(
        &self,
        get_request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<GetResponse>, tonic::Status> {
        let get_request = get_request.into_inner();
        let entry = self.state.get(&get_request.key);
        Ok(Response::new(GetResponse { entry: entry }))
    }

    async fn set(
        &self,
        set_request: tonic::Request<SetRequest>,
    ) -> Result<tonic::Response<SetResponse>, tonic::Status> {
        let SetRequest { key, entry } = set_request.into_inner();
        let old_entry = self.state.set(
            key,
            entry.ok_or(tonic::Status::data_loss("Entry not exists."))?,
        );
        Ok(Response::new(SetResponse { entry: old_entry }))
    }

    async fn remove(
        &self,
        remove_request: tonic::Request<RemoveRequest>,
    ) -> Result<tonic::Response<RemoveResponse>, tonic::Status> {
        let RemoveRequest { key } = remove_request.into_inner();
        let old_entry = self.state.remove(key);
        Ok(Response::new(RemoveResponse { entry: old_entry }))
    }
}
