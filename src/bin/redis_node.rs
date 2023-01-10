use anyhow::Result;
use clap::Parser;
use redis_rs::service::redis_service::RedisService;

#[derive(Debug, Parser)]
#[command(name = "redis node")]
struct Cli {
    id: String,
    grpc_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()?;

    let cli = Cli::parse();

    log::info!("redis节点启动, id: {:?}, addr: {:?}", cli.id, cli.grpc_addr);

    RedisService::new().serve(cli.grpc_addr.parse()?).await
}
