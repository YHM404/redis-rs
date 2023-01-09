use std::{collections::HashMap, net::SocketAddr, ops::Range, sync::Arc};

use anyhow::{Context, Result};
use async_trait::async_trait;
use crc::{Crc, CRC_16_IBM_SDLC};
use tokio::sync::RwLock;
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request, Response, Status,
};

use crate::protobuf::{
    proxy_service::{
        proxy_manage_service_server::{ProxyManageService, ProxyManageServiceServer},
        GetStateResponse, RedisNodeInfo, State, SyncRequest,
    },
    redis_service::{
        redis_service_client::RedisServiceClient,
        redis_service_server::{RedisService, RedisServiceServer},
        GetRequest, GetResponse, RemoveRequest, RemoveResponse, SetRequest, SetResponse,
    },
};

pub const SLOTS_LENGTH: usize = 1024;

#[derive(Debug, Clone)]
struct Slot {
    node_id: NodeId,
}

impl Slot {
    fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            node_id: "".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct Router {
    slots: [Slot; SLOTS_LENGTH],
    redis_channels: HashMap<NodeId, Channel>,
}

impl Router {
    fn new() -> Self {
        let slots = [(); SLOTS_LENGTH].map(|_| Slot::default());
        Self {
            slots,
            redis_channels: HashMap::new(),
        }
    }
    /// 更新路由信息
    fn update_router(&mut self, redis_node_infos: Vec<RedisNodeInfo>) -> Result<()> {
        for redis_node_info in redis_node_infos {
            let slot_range = redis_node_info
                .slot_range
                .ok_or_else(|| Status::invalid_argument("SlotRange为空"))?;
            let RedisNodeInfo { id, addr, .. } = redis_node_info;
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

    /// 拿到与key对应的redis节点之间的grpc channel
    fn get_redis_channel_by_key(&self, key: impl AsRef<str>) -> Result<Channel> {
        let crc = Crc::<u16>::new(&CRC_16_IBM_SDLC);
        let slot_index = crc.checksum(key.as_ref().as_bytes()) as usize % SLOTS_LENGTH;
        self.redis_channels
            .get(&self.slots[slot_index].node_id)
            .context("找不到NodeId对应的Channel")
            .cloned()
    }

    /// 转发客户端发来的set请求到对应的redis节点
    async fn forward_set(&self, request: SetRequest) -> Result<SetResponse> {
        let channel = self.get_redis_channel_by_key(&request.key)?;
        let mut client = RedisServiceClient::new(channel);
        Ok(client.set(Request::new(request)).await?.into_inner())
    }

    /// 转发客户端发来的get请求到对应的redis节点
    async fn forward_get(&self, request: GetRequest) -> Result<GetResponse> {
        let channel = self.get_redis_channel_by_key(&request.key)?;
        let mut client = RedisServiceClient::new(channel);
        Ok(client.get(Request::new(request)).await?.into_inner())
    }

    /// 转发客户端发来的remove请求到对应的redis节点
    async fn forward_remove(&self, request: RemoveRequest) -> Result<RemoveResponse> {
        let channel = self.get_redis_channel_by_key(&request.key)?;
        let mut client = RedisServiceClient::new(channel);
        Ok(client.remove(Request::new(request)).await?.into_inner())
    }
}

type NodeId = String;
#[derive(Debug, Clone)]
pub struct ProxyNode {
    state: Arc<RwLock<State>>,
    router: Arc<RwLock<Router>>,
}

impl ProxyNode {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::Expired)),
            router: Arc::new(RwLock::new(Router::new())),
        }
    }

    pub async fn serve(self, addr: SocketAddr) {
        tokio::spawn(
            Server::builder()
                .add_service(ProxyManageServiceServer::new(self.clone()))
                .add_service(RedisServiceServer::new(self))
                .serve(addr),
        );
    }
}

#[async_trait]
impl ProxyManageService for ProxyNode {
    /// 将该代理节点设置为过期状态，设置为过期状态的代理节点不向客户端提供服务
    async fn expired(&self, _: Request<()>) -> Result<Response<()>, Status> {
        *self.state.write().await = State::Expired;
        Ok(Response::new(()))
    }

    /// 更新代理节点的路由信息，并将状态更新为Sync，Sync状态的proxy节点可以正常响应客户端的请求
    async fn sync(&self, request: Request<SyncRequest>) -> Result<Response<()>, Status> {
        log::info!("收到sync请求, {:?}", request);
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
            State::Synced => Ok(Response::new(())),
        }
    }

    /// 获得当前proxy节点的状态
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
        match *self.state.read().await {
            State::Expired => Err(Status::failed_precondition(
                "代理节点是过期状态, 不能提供服务",
            )),
            State::Synced => Ok(Response::new(
                self.router
                    .read()
                    .await
                    .forward_set(request.into_inner())
                    .await
                    .map_err(|e| Status::cancelled(format!("SetRequest转发错误: {:?}", e)))?,
            )),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        match *self.state.read().await {
            State::Expired => Err(Status::failed_precondition(
                "代理节点是过期状态, 不能提供服务",
            )),
            State::Synced => Ok(Response::new(
                self.router
                    .read()
                    .await
                    .forward_get(request.into_inner())
                    .await
                    .map_err(|e| Status::cancelled(format!("GetRequest转发错误: {:?}", e)))?,
            )),
        }
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        match *self.state.read().await {
            State::Expired => Err(Status::failed_precondition(
                "代理节点是过期状态, 不能提供服务",
            )),
            State::Synced => Ok(Response::new(
                self.router
                    .read()
                    .await
                    .forward_remove(request.into_inner())
                    .await
                    .map_err(|e| Status::cancelled(format!("RemoveRequest转发错误: {:?}", e)))?,
            )),
        }
    }
}
