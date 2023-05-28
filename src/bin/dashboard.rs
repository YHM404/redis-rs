use anyhow::Result;
use clap::{Parser, Subcommand};
use redis_rs::{protobuf::proxy_service::RedisNodeInfo, service::dashboard::Dashboard};
use url::Url;

#[derive(Debug, Parser)]
#[command(name = "manage client")]
struct Cli {
    etcd_addr: String,
    #[command(subcommand)]
    sub_cmd: Cmd,
}

#[derive(Debug, Subcommand, Clone)]
enum Cmd {
    ShowProxy,
    ShowRedis,
    Sync,
    Add { id: String, addr: String },
    Remove { id: String },
    Other,
}

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()?;

    let cli = Cli::parse();
    let mut dashboard = Dashboard::new(cli.etcd_addr).await?;

    match cli.sub_cmd {
        Cmd::Add { id, addr } => {
            log::info!(
                "执行Add命令, 向集群中添加redis节点id: {:?}, addr: {:?}",
                id,
                addr
            );
            let url = Url::parse(&addr)?;
            dashboard
                .add_redis_node(RedisNodeInfo {
                    id,
                    endpoint: Some(url.try_into()?),
                    slot_range: None,
                })
                .await?;
        }

        Cmd::Remove { id } => {
            log::info!("执行Remove命令, 移除redis节点 id: {:?}", id);
            dashboard.remove_redis_node(id).await?;
        }

        Cmd::ShowProxy => {
            log::info!(
                "执行ShowProxy命令, 展示所有代理节点信息: \n {:?}",
                dashboard.get_proxy_node_infos().await?
            );
        }

        Cmd::ShowRedis => {
            log::info!(
                "执行ShowRedis命令, 展示所有Redis节点信息: \n {:?}",
                dashboard.redis_infos
            );
        }

        Cmd::Sync => {
            log::info!("执行Sync命令, 把redis节点信息同步给所有代理节点");
            dashboard.sync_all_proxy_nodes().await?;
        }
        _ => {
            log::error!("暂时不支持的命令")
        }
    }

    Ok(())
}
