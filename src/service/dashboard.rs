use std::collections::{btree_map, BTreeMap};

use anyhow::{anyhow, Context, Result};

use etcd_client::{Client, DeleteOptions, GetOptions};
use prost::Message;

use crate::{
    protobuf::{
        common::SlotsMapping,
        proxy_service::{
            proxy_manage_service_client::ProxyManageServiceClient, ProxyNodeInfo, RedisNodeInfo,
            SlotRange, SyncRequest,
        },
    },
    utils::get_redis_infos_from_etcd,
    PROXY_NODE_ID_PREFIX, REDIS_NODE_ID_PREFIX, SLOTS_LENGTH, SLOTS_MAPPING,
};
use tonic::Request;

pub struct Dashboard {
    etcd_client: etcd_client::Client,

    pub redis_infos: BTreeMap<String, RedisNodeInfo>,

    _slots_mapping: SlotsMapping,
}

impl Dashboard {
    pub async fn new(etcd_service_endpoint: String) -> Result<Self> {
        let mut etcd_client = Client::connect([etcd_service_endpoint], None).await?;
        let _slots_mapping = etcd_client
            .get(SLOTS_MAPPING, None)
            .await?
            .kvs()
            .get(0)
            .and_then(|kv| SlotsMapping::decode(kv.value()).ok())
            .unwrap_or_else(|| SlotsMapping::init()); // 初始化为默认值
        let redis_infos = get_redis_infos_from_etcd(&mut etcd_client).await?;
        Ok(Self {
            etcd_client,
            redis_infos,
            _slots_mapping,
        })
    }

    /// 向redis集群添加一个节点
    pub async fn add_redis_node(&mut self, redis_node_info: RedisNodeInfo) -> Result<()> {
        // 获取所有redis节点信息，并对slot进行再分配后重新注册到etcd
        let mut redis_node_infos = get_redis_infos_from_etcd(&mut self.etcd_client).await?;
        if let btree_map::Entry::Vacant(vacant) = redis_node_infos.entry(redis_node_info.id.clone())
        {
            vacant.insert(redis_node_info);
            reallocate_slots(&mut redis_node_infos)?;

            self.sync_all_proxy_nodes().await?;

            log::info!(
                "成功注册新的redis节点, 当前集群redis节点信息: {:?}",
                redis_node_infos
            );
            Ok(())
        } else {
            Err(anyhow!("节点已经注册过: {:?}", redis_node_info))
        }
    }

    pub async fn add_redis_node_v2(&mut self, _redis_node_info: RedisNodeInfo) -> Result<()> {
        todo!()
    }

    pub async fn remove_redis_node(&mut self, id: String) -> Result<()> {
        if let Some(removed_redis_node) = self.redis_infos.remove(&id) {
            self.etcd_client
                .delete(
                    format!("{}:{}", REDIS_NODE_ID_PREFIX, id),
                    Some(DeleteOptions::new().with_all_keys()),
                )
                .await?;

            reallocate_slots(&mut self.redis_infos)?;
            self.sync_all_proxy_nodes().await?;
            log::info!("redis节点已经从集群中移除: {:?}", removed_redis_node);
            Ok(())
        } else {
            Err(anyhow!("要移除的节点不存在: id = {:?}", id))
        }
    }

    pub async fn get_proxy_node_infos(&mut self) -> Result<BTreeMap<String, ProxyNodeInfo>> {
        self.etcd_client
            .get(PROXY_NODE_ID_PREFIX, Some(GetOptions::new().with_prefix()))
            .await?
            .kvs()
            .into_iter()
            .map(|kv| {
                Ok((
                    kv.key_str()?.to_string(),
                    ProxyNodeInfo::decode(kv.value()).context("ProxyNodeInfo反序列化错误")?,
                ))
            })
            .collect::<Result<BTreeMap<String, ProxyNodeInfo>>>()
    }

    async fn register_redis_nodes(&mut self) -> Result<()> {
        for (_, redis_node_info) in &self.redis_infos {
            let node_id = redis_node_info.id.clone();
            self.etcd_client
                .put(
                    format!("{}:{}", REDIS_NODE_ID_PREFIX, node_id),
                    redis_node_info.encode_to_vec(),
                    None,
                )
                .await?;
        }
        Ok(())
    }

    pub async fn sync_all_proxy_nodes(&mut self) -> Result<()> {
        let proxy_node_infos = self.get_proxy_node_infos().await?;
        // 向所有代理节点广播Expired通知
        let mut proxy_clients = Vec::with_capacity(proxy_node_infos.len());
        for (_, proxy_node_info) in proxy_node_infos {
            let addr = proxy_node_info.addr.clone();
            let mut proxy_client = ProxyManageServiceClient::connect(addr).await?;
            proxy_client.expired(Request::new(())).await?;
            proxy_clients.push(proxy_client);
        }

        self.register_redis_nodes().await?;

        // 向所有代理节点广播Sync请求
        for proxy_client in proxy_clients.iter_mut() {
            proxy_client
                .sync(Request::new(SyncRequest {
                    redis_node_infos: self.redis_infos.clone().into_values().collect(),
                }))
                .await?;
        }
        Ok(())
    }
}

/// 计算redis_nodes应该存放那些slot
fn reallocate_slots(redis_node_infos: &mut BTreeMap<String, RedisNodeInfo>) -> Result<()> {
    if redis_node_infos.is_empty() {
        return Err(anyhow!("redis_node_infos不能为空"));
    }

    let step = (SLOTS_LENGTH / redis_node_infos.len()) as u64;
    let mut start_slot_index = 0;
    for (_, redis_node_info) in redis_node_infos.iter_mut() {
        redis_node_info.slot_range = Some(SlotRange {
            start: start_slot_index,
            end: start_slot_index + step,
        });
        start_slot_index += step;
    }
    redis_node_infos
        .last_entry()
        .context("redis_node_infos不能为空")?
        .get_mut()
        .slot_range
        .as_mut()
        .context("slot_range不能为空")?
        .end = SLOTS_LENGTH as u64;
    Ok(())
}
