use std::{collections::HashMap, ops::Range};

use anyhow::{Context, Result};
use async_trait::async_trait;
use crc::{Crc, CRC_16_IBM_SDLC};
use tokio::sync::RwLock;
use tonic::{
    transport::{Channel, Endpoint},
    Request, Response, Status,
};

use crate::protobuf::{
    proxy_service::{
        proxy_manage_service_server::ProxyManageService, GetStateResponse, NodeInfo, RedisNodeInfo,
        State, SyncRequest,
    },
    redis_service::{
        redis_service_client::RedisServiceClient, redis_service_server::RedisService, GetRequest,
        GetResponse, RemoveRequest, RemoveResponse, SetRequest, SetResponse,
    },
};

const SLOTS_LENGTH: usize = 1024;

struct Slot {
    node_id: NodeId,
}

impl Slot {
    fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }
}

struct Router {
    slots: [Slot; SLOTS_LENGTH],
    redis_channels: HashMap<NodeId, Channel>,
}

impl Router {
    fn update_router(&mut self, redis_node_infos: Vec<RedisNodeInfo>) -> Result<()> {
        for redis_node_info in redis_node_infos {
            let slot_range = redis_node_info
                .slot_range
                .ok_or_else(|| Status::invalid_argument("SlotRange为空"))?;
            let NodeInfo { id, addr } = redis_node_info
                .node_info
                .ok_or_else(|| Status::invalid_argument("NodeInfo为空"))?;
            let channel = Endpoint::new(addr)
                .or(Err(Status::unavailable("与redis节点建立Channel失败")))?
                .connect_lazy();
            self.redis_channels.insert(id.clone(), channel);
            for i in Range::from(slot_range) {
                self.slots[i] = Slot::new(id.clone());
            }
        }
        Ok(())
    }

    fn get_redis_channel_by_key(&self, key: impl AsRef<str>) -> Result<Channel> {
        let crc = Crc::<u16>::new(&CRC_16_IBM_SDLC);
        let slot_index = crc.checksum(key.as_ref().as_bytes()) as usize % SLOTS_LENGTH;
        self.redis_channels
            .get(&self.slots[slot_index].node_id)
            .context("找不到NodeId对应的Channel")
            .cloned()
    }

    async fn forward_set(&self, request: SetRequest) -> Result<SetResponse> {
        let channel = self.get_redis_channel_by_key(&request.key)?;
        let mut client = RedisServiceClient::new(channel);
        Ok(client.set(Request::new(request)).await?.into_inner())
    }

    async fn forward_get(&self, request: GetRequest) -> Result<GetResponse> {
        let channel = self.get_redis_channel_by_key(&request.key)?;
        let mut client = RedisServiceClient::new(channel);
        Ok(client.get(Request::new(request)).await?.into_inner())
    }

    async fn forward_remove(&self, request: RemoveRequest) -> Result<RemoveResponse> {
        let channel = self.get_redis_channel_by_key(&request.key)?;
        let mut client = RedisServiceClient::new(channel);
        Ok(client.remove(Request::new(request)).await?.into_inner())
    }
}

type NodeId = String;
struct ProxyNode {
    state: RwLock<State>,
    router: RwLock<Router>,
}

#[async_trait]
impl ProxyManageService for ProxyNode {
    async fn expired(&self, _: Request<()>) -> Result<Response<()>, Status> {
        *self.state.write().await = State::Expired;
        Ok(Response::new(()))
    }

    async fn sync(&self, request: Request<SyncRequest>) -> Result<Response<()>, Status> {
        let mut state_write_guard = self.state.write().await;
        match *state_write_guard {
            State::Expired => {
                let redis_node_infos = request.into_inner().redis_node_infos;
                self.router
                    .write()
                    .await
                    .update_router(redis_node_infos)
                    .map_err(|e| Status::cancelled(format!("路由更新失败: {:?}", e)))?;
                *state_write_guard = State::Synced;
                Ok(Response::new(()))
            }
            State::Synced => Err(Status::failed_precondition(
                "当前proxy元数据是Synced状态, 如果需要更新元数据请先将节点设置为Expired",
            )),
        }
    }

    async fn get_state(&self, _: Request<()>) -> Result<Response<GetStateResponse>, tonic::Status> {
        Ok(Response::new(GetStateResponse {
            state: *self.state.read().await as i32,
        }))
    }
}

/// 客户端访问proxy节点和访问redis节点接口一致
#[async_trait]
impl RedisService for ProxyNode {
    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<SetResponse>, tonic::Status> {
        Ok(Response::new(
            self.router
                .read()
                .await
                .forward_set(request.into_inner())
                .await
                .map_err(|e| Status::cancelled(format!("SetRequest转发错误: {:?}", e)))?,
        ))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        Ok(Response::new(
            self.router
                .read()
                .await
                .forward_get(request.into_inner())
                .await
                .map_err(|e| Status::cancelled(format!("GetRequest转发错误: {:?}", e)))?,
        ))
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        Ok(Response::new(
            self.router
                .read()
                .await
                .forward_remove(request.into_inner())
                .await
                .map_err(|e| Status::cancelled(format!("RemoveRequest转发错误: {:?}", e)))?,
        ))
    }
}
