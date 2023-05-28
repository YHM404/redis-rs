use std::collections::{btree_map, BTreeMap};

use anyhow::{Context, Result};
use etcd_client::GetOptions;
use prost::Message;

use crate::{
    protobuf::{
        common::{slot, Slot, SlotsMapping},
        proxy_service::RedisNodeInfo,
    },
    REDIS_NODE_ID_PREFIX, SLOTS_MAPPING,
};

pub async fn get_redis_infos_from_etcd(
    etcd_client: &mut etcd_client::Client,
) -> Result<BTreeMap<String, RedisNodeInfo>> {
    etcd_client
        .get(REDIS_NODE_ID_PREFIX, Some(GetOptions::new().with_prefix()))
        .await?
        .kvs()
        .into_iter()
        .map(|kv| {
            let redis_node_info =
                RedisNodeInfo::decode(kv.value()).context("RedisNodeInfo反序列化错误")?;
            Ok((redis_node_info.id.clone(), redis_node_info))
        })
        .collect::<Result<BTreeMap<String, RedisNodeInfo>>>()
}

pub async fn get_slots_mapping_from_etcd(
    etcd_client: &mut etcd_client::Client,
) -> Result<(Vec<Slot>, BTreeMap<String, Vec<Slot>>)> {
    let slots_mapping = etcd_client
        .get(SLOTS_MAPPING, None)
        .await?
        .kvs()
        .get(0)
        .and_then(|kv| SlotsMapping::decode(kv.value()).ok())
        .unwrap_or_else(|| SlotsMapping::init())
        .slots;

    let (unallocated_slots_mapping, allocated_slots_mapping): (Vec<_>, Vec<_>) = slots_mapping
        .into_iter()
        .partition(|slot| slot.state() == slot::State::Unallocated);

    let allocated_slots_mapping =
        allocated_slots_mapping
            .into_iter()
            .fold(BTreeMap::new(), |mut map, slot| {
                match map.entry(slot.redis_id.clone()) {
                    btree_map::Entry::Vacant(vacant) => {
                        vacant.insert(vec![slot]);
                    }
                    btree_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().push(slot);
                    }
                }
                map
            });

    Ok((unallocated_slots_mapping, allocated_slots_mapping))
}
