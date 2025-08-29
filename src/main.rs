use anyhow::Result;
use clap::Parser;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use codecrafters_redis::server::{Config, Server};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    #[clap(default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    dir: Option<PathBuf>,

    #[arg(long)]
    dbfilename: Option<PathBuf>,

    #[arg(long, value_parser = parse_replicaof)]
    replicaof: Option<SocketAddr>,
}

fn parse_replicaof(s: &str) -> Result<SocketAddr> {
    let parts: Vec<_> = s.split(" ").collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid replicaof format. Expected: 'host port'"));
    }

    let addr = parts[0];
    let port = parts[1];
    let full_address = format!("{}:{}", addr, port);

    let socket_addr = full_address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("Unable to resolve address"))?;

    Ok(socket_addr)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    println!("Starting Redis server on port {}", args.port);
    
    let config = Config {
        port: args.port,
        dir: args.dir,
        dbfilename: args.dbfilename,
        replicaof: args.replicaof,
    };
    
    let server = Server::new(config).await?;
    server.run().await?;
    
    Ok(())
}