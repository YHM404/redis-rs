use anyhow::Result;
use clap::{Parser, Subcommand};
use redis_rs::{protobuf::proxy_service::RedisNodeInfo, service::dashboard::Dashboard};

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
            dashboard
                .add_redis_node(RedisNodeInfo {
                    id,
                    addr,
                    slot_range: None,
                    slots: vec![],
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
                dashboard.get_redis_node_infos().await?
            );
        }

        Cmd::Sync => {
            log::info!("执行Sync命令, 把redis节点信息同步给所有代理节点");
            let mut redis_node_infos = dashboard.get_redis_node_infos().await?;
            dashboard
                .sync_all_proxy_nodes(&mut redis_node_infos)
                .await?;
        }
        _ => {
            log::error!("暂时不支持的命令")
        }
    }

    Ok(())
}
