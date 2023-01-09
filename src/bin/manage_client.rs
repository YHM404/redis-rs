use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use etcd_client::{Client, GetOptions};
use prost::Message;
use redis_rs::{
    protobuf::proxy_service::{
        proxy_manage_service_client::ProxyManageServiceClient, ProxyNodeInfo, RedisNodeInfo,
        SlotRange, SyncRequest,
    },
    service::proxy_service::SLOTS_LENGTH,
};
use tonic::Request;

#[derive(Debug, Parser)]
#[command(name = "manage client")]
struct Cli {
    etcd_addr: String,
    #[command(subcommand)]
    sub_cmd: Cmd,
}

#[derive(Debug, Subcommand, Clone)]
enum Cmd {
    Add { id: String, addr: String },
    Other,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut manage_client = ManageClient::new(cli.etcd_addr).await?;

    match cli.sub_cmd {
        Cmd::Add { id, addr } => {
            manage_client
                .add_redis_node(RedisNodeInfo {
                    id,
                    addr,
                    slot_range: None,
                })
                .await?;
        }

        _ => {
            panic!("暂时不支持的命令")
        }
    }

    Ok(())
}

const REDIS_NODE_ID_PREFIX: &str = "REDIS_NODE";
const PROXY_NODE_ID_PREFIX: &str = "PROXY_NODE";

struct ManageClient {
    etcd_client: etcd_client::Client,
}

impl ManageClient {
    async fn new(etcd_service_endpoint: String) -> Result<Self> {
        Ok(Self {
            etcd_client: Client::connect([etcd_service_endpoint], None).await?,
        })
    }

    /// 向redis集群添加一个节点
    async fn add_redis_node(&mut self, redis_node_info: RedisNodeInfo) -> Result<()> {
        // TODO: 暂时不支持添加id重复的节点

        // 获取所有redis节点信息，并对slot进行再分配后重新注册到etcd
        let mut redis_node_infos = self.get_redis_node_infos().await?;
        redis_node_infos.push(redis_node_info);

        reallocate_slots(&mut redis_node_infos)?;
        for redis_node_info in redis_node_infos.iter_mut() {
            let node_id = redis_node_info.id.clone();
            self.etcd_client
                .put(
                    format!("{}:{}", REDIS_NODE_ID_PREFIX, node_id),
                    redis_node_info.encode_to_vec(),
                    None,
                )
                .await?;
        }

        let proxy_node_infos = self.get_proxy_node_infos().await?;
        // 向所有代理节点广播Expired通知
        let mut proxy_clients = Vec::with_capacity(proxy_node_infos.len());
        for proxy_node_info in proxy_node_infos {
            let addr = proxy_node_info.addr.clone();
            let mut proxy_client = ProxyManageServiceClient::connect(addr).await?;
            proxy_client.expired(Request::new(())).await?;
            proxy_clients.push(proxy_client);
        }

        // 向所有代理节点广播Sync请求
        for proxy_client in proxy_clients.iter_mut() {
            proxy_client
                .sync(Request::new(SyncRequest {
                    redis_node_infos: redis_node_infos.clone(),
                }))
                .await?;
        }
        Ok(())
    }

    async fn get_proxy_node_infos(&mut self) -> Result<Vec<ProxyNodeInfo>> {
        self.etcd_client
            .get(PROXY_NODE_ID_PREFIX, Some(GetOptions::new().with_prefix()))
            .await?
            .kvs()
            .into_iter()
            .map(|kv| ProxyNodeInfo::decode(kv.value()).context("ProxyNodeInfo反序列化错误"))
            .collect::<Result<Vec<ProxyNodeInfo>>>()
    }

    async fn get_redis_node_infos(&mut self) -> Result<Vec<RedisNodeInfo>> {
        self.etcd_client
            .get(REDIS_NODE_ID_PREFIX, Some(GetOptions::new().with_prefix()))
            .await?
            .kvs()
            .into_iter()
            .map(|kv| RedisNodeInfo::decode(kv.value()).context("RedisNodeInfo反序列化错误"))
            .collect::<Result<Vec<RedisNodeInfo>>>()
    }
}

/// 计算redis_nodes应该存放那些slot
fn reallocate_slots(redis_node_infos: &mut Vec<RedisNodeInfo>) -> Result<()> {
    if redis_node_infos.is_empty() {
        return Err(anyhow!("redis_node_infos不能为空"));
    }

    let step = (SLOTS_LENGTH / redis_node_infos.len()) as u64;
    println!("{}, {}", redis_node_infos.len(), step);
    let mut start_slot_index = 0;
    for redis_node_info in redis_node_infos.iter_mut() {
        redis_node_info.slot_range = Some(SlotRange {
            start: start_slot_index,
            end: start_slot_index + step,
        });
        start_slot_index += step;
    }
    redis_node_infos
        .last_mut()
        .context("redis_node_infos不能为空")?
        .slot_range
        .as_mut()
        .context("slot_range不能为空")?
        .end = SLOTS_LENGTH as u64;
    Ok(())
}
