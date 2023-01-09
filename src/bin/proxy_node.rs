use anyhow::Result;
use clap::Parser;
use prost::Message;
use redis_rs::{
    protobuf::proxy_service::ProxyNodeInfo, service::proxy_service::ProxyNode, PROXY_NODE_ID_PREFIX,
};

#[derive(Debug, Parser)]
#[command(name = "proxy node")]
struct Cli {
    // 代理节点的id
    id: String,
    // 代理节点向外暴露的grpc地址
    grpc_addr: String,
    // etcd的地址
    etcd_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()?;

    let cli = Cli::parse();
    log::info!("代理节点启动, id: {:?}, addr: {:?}", cli.id, cli.grpc_addr);

    let mut etcd_client = etcd_client::Client::connect([cli.etcd_addr], None).await?;
    etcd_client
        .put(
            format!("{}:{}", PROXY_NODE_ID_PREFIX, cli.id),
            ProxyNodeInfo {
                id: cli.id,
                addr: cli.grpc_addr.clone(),
            }
            .encode_to_vec(),
            None,
        )
        .await?;

    ProxyNode::new().serve(cli.grpc_addr.parse()?).await;

    Ok(())
}
