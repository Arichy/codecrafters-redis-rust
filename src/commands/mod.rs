use std::sync::Arc;
use anyhow::Result;
use tokio::sync::{broadcast, RwLock, Mutex};

use crate::connection::{MessageReader, MessageWriter};
use crate::message::{Message, Array, BulkString, SimpleString, SimpleError, Integer};
use crate::store::Store;
use crate::server::Config;
use crate::replication::{ReplicationInfo, ReplicationState};
use crate::pubsub::PubSubManager;
use crate::blocking::BlockingCommandManager;

pub mod ping;
pub mod echo;
pub mod get;
pub mod set;
pub mod info;
pub mod config;
pub mod keys;
pub mod incr;
pub mod type_cmd;
pub mod xadd;
pub mod xrange;
pub mod xread;
pub mod rpush;
pub mod lpush;
pub mod lrange;
pub mod llen;
pub mod lpop;
pub mod blpop;
pub mod subscribe;
pub mod unsubscribe;
pub mod publish;
pub mod zadd;
pub mod zrank;
pub mod zrange;
pub mod zcard;
pub mod zscore;
pub mod zrem;
pub mod psync;
pub mod replconf;
pub mod wait_cmd;

#[derive(Debug, Clone)]
pub struct CommandMessage {
    pub peer_addr: String,
    pub message: Message,
}

pub struct CommandContext {
    pub config: Arc<RwLock<Config>>,
    pub store: Arc<Store>,
    pub replication_info: Arc<ReplicationInfo>,
    pub replication_state: Arc<Mutex<ReplicationState>>,
    pub pubsub_manager: Arc<PubSubManager>,
    pub blocking_manager: Arc<BlockingCommandManager>,
    pub command_tx: broadcast::Sender<CommandMessage>,
    pub selected_db_index: Arc<RwLock<usize>>,
    pub message_reader: MessageReader,
    pub message_writer: MessageWriter,
    pub peer_addr: String,
    pub is_slave: bool,
}

pub struct CommandDispatcher {
    config: Arc<RwLock<Config>>,
    store: Arc<Store>,
    replication_info: Arc<ReplicationInfo>,
    replication_state: Arc<Mutex<ReplicationState>>,
    pubsub_manager: Arc<PubSubManager>,
    blocking_manager: Arc<BlockingCommandManager>,
    command_tx: broadcast::Sender<CommandMessage>,
    selected_db_index: Arc<RwLock<usize>>,
}

impl CommandDispatcher {
    pub fn new(
        config: Arc<RwLock<Config>>,
        store: Arc<Store>,
        replication_info: Arc<ReplicationInfo>,
        replication_state: Arc<Mutex<ReplicationState>>,
        pubsub_manager: Arc<PubSubManager>,
        blocking_manager: Arc<BlockingCommandManager>,
        command_tx: broadcast::Sender<CommandMessage>,
        selected_db_index: Arc<RwLock<usize>>,
    ) -> Self {
        Self {
            config,
            store,
            replication_info,
            replication_state,
            pubsub_manager,
            blocking_manager,
            command_tx,
            selected_db_index,
        }
    }
    
    pub async fn dispatch(
        &self,
        message: Message,
        message_reader: MessageReader,
        message_writer: MessageWriter,
        peer_addr: String,
        is_slave: bool,
    ) -> Result<Option<Message>> {
        let commands = parse_message(&message);
        
        if commands.is_empty() {
            return Ok(None);
        }
        
        let cmd = commands[0].to_lowercase();
        let params = &commands[1..];
        
        let context = CommandContext {
            config: Arc::clone(&self.config),
            store: Arc::clone(&self.store),
            replication_info: Arc::clone(&self.replication_info),
            replication_state: Arc::clone(&self.replication_state),
            pubsub_manager: Arc::clone(&self.pubsub_manager),
            blocking_manager: Arc::clone(&self.blocking_manager),
            command_tx: self.command_tx.clone(),
            selected_db_index: Arc::clone(&self.selected_db_index),
            message_reader,
            message_writer,
            peer_addr: peer_addr.clone(),
            is_slave,
        };
        
        // Check if client is in pub/sub mode
        if self.pubsub_manager.is_subscribed(&peer_addr).await {
            static ALLOWED_IN_PUBSUB: &[&str] = &[
                "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ping", "quit"
            ];
            
            if !ALLOWED_IN_PUBSUB.contains(&cmd.as_str()) {
                return Ok(Some(Message::SimpleError(SimpleError {
                    string: format!("ERR Can't execute '{}' in subscribed mode", cmd),
                })));
            }
        }
        

        
        // Dispatch to appropriate command handler
        let result = match cmd.as_str() {
            "ping" => ping::handle(params, &context).await,
            "echo" => echo::handle(params, &context).await,
            "get" => get::handle(params, &context).await,
            "set" => set::handle(params, &context).await,
            "info" => info::handle(params, &context).await,
            "config" => config::handle(params, &context).await,
            "keys" => keys::handle(params, &context).await,
            "incr" => incr::handle(params, &context).await,
            "type" => type_cmd::handle(params, &context).await,
            "xadd" => xadd::handle(params, &context).await,
            "xrange" => xrange::handle(params, &context).await,
            "xread" => xread::handle(params, &context).await,
            "rpush" => rpush::handle(params, &context).await,
            "lpush" => lpush::handle(params, &context).await,
            "lrange" => lrange::handle(params, &context).await,
            "llen" => llen::handle(params, &context).await,
            "lpop" => lpop::handle(params, &context).await,
            "blpop" => blpop::handle(params, &context).await,
            "subscribe" => subscribe::handle(params, &context).await,
            "unsubscribe" => unsubscribe::handle(params, &context).await,
            "publish" => publish::handle(params, &context).await,
            "zadd" => zadd::handle(params, &context).await,
            "zrank" => zrank::handle(params, &context).await,
            "zrange" => zrange::handle(params, &context).await,
            "zcard" => zcard::handle(params, &context).await,
            "zscore" => zscore::handle(params, &context).await,
            "zrem" => zrem::handle(params, &context).await,
            "psync" => psync::handle(params, &context).await,
            "replconf" => replconf::handle(params, &context).await,
            "wait" => wait_cmd::handle(params, &context).await,
            _ => Ok(None),
        };
        
        // Propagate write commands to slaves if we're master
        if !is_slave && result.is_ok() {
            // Check if this is a write command
            let is_write_command = matches!(cmd.as_str(), 
                "set" | "del" | "incr" | "rpush" | "lpush" | "lpop" | 
                "xadd" | "zadd" | "zrem" | "sadd" | "srem"
            );
            
            if is_write_command {
                // Broadcast the command to all replicas
                let _ = self.command_tx.send(CommandMessage {
                    message: message.clone(),
                    peer_addr: peer_addr.to_string(),
                });
            }
        }
        
        result
    }
}

fn parse_message(message: &Message) -> Vec<&str> {
    match message {
        Message::Array(Array { items }) => items
            .iter()
            .filter_map(|item| match item {
                Message::BulkString(BulkString { string, .. }) => Some(string.as_str()),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

// Helper macro for parameter validation
#[macro_export]
macro_rules! get_param {
    ($index:expr, $params:expr, $cmd_name:expr) => {{
        let Some(value) = $params.get($index) else {
            return Ok(Some(Message::SimpleError(SimpleError {
                string: format!("{} requires more arguments", $cmd_name),
            })));
        };
        value
    }};
}