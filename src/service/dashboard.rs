use std::{
    collections::{btree_map, BTreeMap},
    fmt::format,
};

use anyhow::{anyhow, Context, Result};

use etcd_client::{Client, DeleteOptions, GetOptions};
use prost::Message;

use crate::{
    protobuf::{
        common::{self, slot::MigrateRoute, Slot},
        proxy_service::{
            proxy_manage_service_client::ProxyManageServiceClient, ProxyNodeInfo, RedisNodeInfo,
            SlotRange, SyncRequest,
        },
    },
    utils::{get_redis_infos_from_etcd, get_slots_mapping_from_etcd},
    PROXY_NODE_ID_PREFIX, REDIS_NODE_ID_PREFIX, SLOTS_LENGTH,
};
use tonic::Request;

pub struct Dashboard {
    etcd_client: etcd_client::Client,

    pub redis_infos: BTreeMap<String, RedisNodeInfo>,

    allocated_slots_mapping: BTreeMap<String, Vec<Slot>>,

    unallocated_slots_mapping: Vec<Slot>,
}

impl Dashboard {
    pub async fn new(etcd_service_endpoint: String) -> Result<Self> {
        let mut etcd_client = Client::connect([etcd_service_endpoint], None).await?;
        let (unallocated_slots_mapping, allocated_slots_mapping) =
            get_slots_mapping_from_etcd(&mut etcd_client).await?;

        let redis_infos = get_redis_infos_from_etcd(&mut etcd_client).await?;

        Ok(Self {
            etcd_client,
            redis_infos,
            allocated_slots_mapping,
            unallocated_slots_mapping,
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

    pub async fn add_redis_node_v2(&mut self, redis_node_info: RedisNodeInfo) -> Result<()> {
        self.redis_infos
            .insert(redis_node_info.id.clone(), redis_node_info);
        // TODO: 更新slotsmapping状态....
        let migrating_slots = self.balancing_slots()?;
        Ok(())
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

    // 将slot平均分配到所有redis节点, 并且返回需要迁移的slot队列
    fn balancing_slots(&mut self) -> Result<Vec<Slot>> {
        let redis_count = self.redis_infos.len();
        let expected_slots_count = redis_count.div_ceil(SLOTS_LENGTH);

        // TODO: MigratingRoute

        // redis节点的slots超过预期分配的数量的，会将多余的slots移到unallocated_slots_mapping里
        self.allocated_slots_mapping
            .iter_mut()
            .filter(|(_, slots)| slots.len() > expected_slots_count)
            .try_for_each(|(redis_id, slots)| {
                let mut out_slots = slots.split_off(expected_slots_count);
                out_slots.iter_mut().try_for_each(|slot| {
                    let from_endpoint = self
                        .redis_infos
                        .get(redis_id)
                        .context(format!("redis_id: {:?} 不存在", redis_id))?
                        .endpoint
                        .clone()
                        .context(format!("redis_id= {:?} 的 endpoint为空", redis_id))?;

                    slot.migrate_route = Some(MigrateRoute {
                        from: Some(from_endpoint),
                        to: None,
                    });
                    anyhow::Ok(())
                })?;
                self.unallocated_slots_mapping.extend(out_slots);
                anyhow::Ok(())
            })?;

        let mut migrating_slots = Vec::with_capacity(self.unallocated_slots_mapping.len());

        // redis节点的slots不足预期分配的数量的，会将从unallocated_slots_mappping里补足
        self.allocated_slots_mapping
            .values_mut()
            .filter(|slots| slots.len() < expected_slots_count)
            .try_for_each(|slots| {
                // safe assert
                assert!(expected_slots_count > slots.len());

                // 还差多少个slots才符合预期
                let in_number = expected_slots_count - slots.len();
                // 分配in_number个slots
                let mut in_slots = self
                    .unallocated_slots_mapping
                    .split_off(self.unallocated_slots_mapping.len() - in_number);

                in_slots.iter_mut().try_for_each(|slot| {
                    let to_endpoint = self
                        .redis_infos
                        .get(&slot.redis_id)
                        .context(format!("redis_id: {:?} 不存在", slot.redis_id))?
                        .endpoint
                        .clone();
                    if slot.migrate_route.is_none() {
                        slot.migrate_route = Some(MigrateRoute {
                            from: None,
                            to: to_endpoint,
                        });
                    } else {
                        slot.migrate_route
                            .as_mut()
                            .map(|route| route.to = to_endpoint);
                    };
                    // 需要迁移的slot状态要设置为Pending, 设置为Pending时，该slot只能读不能写
                    slot.set_state(common::slot::State::Pending);

                    // 把该slot加到待迁移队列里
                    migrating_slots.push(slot.clone());

                    anyhow::Ok(())
                })?;

                slots.extend(in_slots);

                anyhow::Ok(())
            })?;
        Ok(migrating_slots)
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

struct MigratingSlots {
    slots: Vec<Slot>,
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
