use async_trait::async_trait;
use tonic::Response;

use crate::{
    protobuf::{
        self,
        redis_service::{
            GetRequest, GetResponse, RemoveRequest, RemoveResponse, SetRequest, SetResponse,
        },
    },
    store::state::State,
};

struct RedisService {
    state: State,
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
