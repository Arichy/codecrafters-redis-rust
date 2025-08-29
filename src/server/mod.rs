use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock, Mutex};
use tokio_util::codec::Framed;
use anyhow::Result;

use crate::connection::Connection;
use crate::message::MessageFramer;
use crate::store::Store;
use crate::replication::{ReplicationInfo, ReplicationState};
use crate::pubsub::PubSubManager;
use crate::blocking::BlockingCommandManager;

pub struct Config {
    pub port: u16,
    pub dir: Option<std::path::PathBuf>,
    pub dbfilename: Option<std::path::PathBuf>,
    pub replicaof: Option<SocketAddr>,
}

pub struct Server {
    config: Arc<RwLock<Config>>,
    store: Arc<Store>,
    replication_info: Arc<ReplicationInfo>,
    replication_state: Arc<Mutex<ReplicationState>>,
    pubsub_manager: Arc<PubSubManager>,
    blocking_manager: Arc<BlockingCommandManager>,
    command_tx: broadcast::Sender<crate::commands::CommandMessage>,
}

impl Server {
    pub async fn new(config: Config) -> Result<Self> {
        let config = Arc::new(RwLock::new(config));
        let store = Arc::new(Store::new());
        
        let role = if config.read().await.replicaof.is_some() {
            crate::replication::Role::Slave(config.read().await.replicaof.unwrap())
        } else {
            crate::replication::Role::Master
        };
        
        let replication_info = Arc::new(ReplicationInfo {
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
        });
        
        let replication_state = Arc::new(Mutex::new(ReplicationState::new()));
        let pubsub_manager = Arc::new(PubSubManager::new());
        let blocking_manager = Arc::new(BlockingCommandManager::new());
        let (command_tx, _) = broadcast::channel(100);
        
        // Load RDB file if specified
        if let Some(dbfilename) = config.read().await.dbfilename.as_ref() {
            let fullpath = if let Some(dir) = config.read().await.dir.as_ref() {
                dir.join(dbfilename)
            } else {
                dbfilename.clone()
            };
            
            if let Ok(rdb_content) = tokio::fs::read(&fullpath).await {
                store.load_from_rdb(&rdb_content).await?;
            }
        }
        
        Ok(Self {
            config,
            store,
            replication_info,
            replication_state,
            pubsub_manager,
            blocking_manager,
            command_tx,
        })
    }
    
    pub async fn run(&self) -> Result<()> {
        let port = self.config.read().await.port;
        
        // Start replication if slave
        if let crate::replication::Role::Slave(master_addr) = &self.replication_info.role {
            self.connect_to_master(*master_addr).await?;
        }
        
        // Start listening for connections
        let listener = TcpListener::bind(("127.0.0.1", port)).await?;
        println!("Redis server listening on port {}", port);
        
        while let Ok((stream, addr)) = listener.accept().await {
            self.handle_connection(stream, addr).await;
        }
        
        Ok(())
    }
    
    async fn handle_connection(&self, stream: TcpStream, addr: SocketAddr) {
        let socket = Framed::new(stream, MessageFramer);
        
        let connection = Connection::new(
            socket,
            addr,
            Arc::clone(&self.config),
            Arc::clone(&self.store),
            Arc::clone(&self.replication_info),
            Arc::clone(&self.replication_state),
            Arc::clone(&self.pubsub_manager),
            Arc::clone(&self.blocking_manager),
            self.command_tx.clone(),
        );
        
        tokio::spawn(async move {
            if let Err(e) = connection.run(false).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
    
    async fn connect_to_master(&self, master_addr: SocketAddr) -> Result<()> {
        let stream = TcpStream::connect(master_addr).await?;
        let socket = Framed::new(stream, MessageFramer);
        
        let connection = Connection::new(
            socket,
            master_addr,
            Arc::clone(&self.config),
            Arc::clone(&self.store),
            Arc::clone(&self.replication_info),
            Arc::clone(&self.replication_state),
            Arc::clone(&self.pubsub_manager),
            Arc::clone(&self.blocking_manager),
            self.command_tx.clone(),
        );
        
        tokio::spawn(async move {
            if let Err(e) = connection.run_as_slave().await {
                eprintln!("Replication error: {}", e);
            }
        });
        
        Ok(())
    }
}