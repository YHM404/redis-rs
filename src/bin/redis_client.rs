use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use etcd_client::GetOptions;
use log::info;
use prost::Message;
use redis_rs::{
    protobuf::{
        proxy_service::ProxyNodeInfo,
        redis_service::{
            redis_service_client::RedisServiceClient, Entry, GetRequest, RemoveRequest, SetRequest,
        },
    },
    PROXY_NODE_ID_PREFIX,
};

#[derive(Debug, Parser)]
#[command(name = "redis client")]
struct Cli {
    etcd_addr: String,
    #[command(subcommand)]
    sub_cmd: Cmd,
}

#[derive(Debug, Subcommand, Clone)]
enum Cmd {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()?;

    let cli = Cli::parse();

    let mut redis_client = RedisClient::new(cli.etcd_addr).await?;
    // 向etcd获取当前有哪些代理节点在提供服务
    let proxy_node_infos = redis_client.get_proxy_node_infos().await?;
    // 遍历所有的代理节点，并向代理节点发送对应请求
    // TODO: optimize（随机向代理节点发请求，而不是每次从第一个开始发）
    match cli.sub_cmd {
        Cmd::Get { key } => {
            for proxy_node_info in proxy_node_infos {
                // 连接proxy提供的转发redis请求的服务
                let mut proxy_svc_client =
                    RedisServiceClient::connect(proxy_node_info.addr).await?;
                if let Ok(resp) = proxy_svc_client.get(GetRequest { key: key.clone() }).await {
                    let value = resp
                        .into_inner()
                        .entry
                        .map(|entry| String::from_utf8(entry.value))
                        .transpose()?;
                    info!("Get key: {:?}, value: {:?}", key, value);
                    return Ok(());
                }
            }
            Err(anyhow!("Get key {:?} 失败", key))
        }
        Cmd::Set { key, value } => {
            for proxy_node_info in proxy_node_infos {
                // 连接proxy提供的转发redis请求的服务
                let mut proxy_svc_client =
                    RedisServiceClient::connect(proxy_node_info.addr).await?;
                if let Ok(_) = proxy_svc_client
                    .set(SetRequest {
                        key: key.clone(),
                        entry: Some(Entry {
                            value: value.as_bytes().to_vec(),
                        }),
                    })
                    .await
                {
                    info!("Set key: {:?}, value: {:?}", key, value);
                    return Ok(());
                }
            }
            Err(anyhow!("Set key {:?}, value: {:?} 失败", key, value))
        }
        Cmd::Remove { key } => {
            for proxy_node_info in proxy_node_infos {
                // 连接proxy提供的转发redis请求的服务
                let mut proxy_svc_client =
                    RedisServiceClient::connect(proxy_node_info.addr).await?;
                if let Ok(_) = proxy_svc_client
                    .remove(RemoveRequest { key: key.clone() })
                    .await
                {
                    info!("Remove key: {:?}", key);
                    return Ok(());
                }
            }
            Err(anyhow!("Remove key {:?} 失败", key))
        }
    }
}

struct RedisClient {
    etcd_client: etcd_client::Client,
}

impl RedisClient {
    async fn new(etcd_addr: String) -> Result<Self> {
        Ok(Self {
            etcd_client: etcd_client::Client::connect([etcd_addr], None).await?,
        })
    }
    async fn get_proxy_node_infos(&mut self) -> Result<Vec<ProxyNodeInfo>> {
        self.etcd_client
            .get(PROXY_NODE_ID_PREFIX, Some(GetOptions::new().with_prefix()))
            .await?
            .kvs()
            .into_iter()
            .map(|kv| Ok(ProxyNodeInfo::decode(kv.value()).context("ProxyNodeInfo反序列化错误")?))
            .collect::<Result<Vec<ProxyNodeInfo>>>()
    }
}
