use std::collections::BTreeMap;

use anyhow::{Context, Result};
use etcd_client::GetOptions;
use prost::Message;

use crate::{protobuf::proxy_service::RedisNodeInfo, REDIS_NODE_ID_PREFIX};

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
