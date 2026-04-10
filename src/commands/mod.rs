use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::message::Message;
use crate::rdb::Database;
use crate::server::Server;

pub mod auth;
pub mod geo;
pub mod list;
pub mod pubsub;
pub mod server_cmds;
pub mod sorted_set;
pub mod stream;
pub mod string;
pub mod transaction;

/// Server configuration (read-only, shared via Arc)
#[derive(Debug)]
pub struct ServerConfig {
    pub dir: Option<PathBuf>,
    pub dbfilename: Option<PathBuf>,
}

/// Context for command execution
pub struct CommandContext {
    pub server: Server,
    pub client_id: String,
    pub selected_db: Arc<RwLock<usize>>,
    pub is_slave: bool,
    pub config: Arc<ServerConfig>,
}

/// Parse command from message
pub fn parse_command(message: &Message) -> Option<(String, Vec<String>)> {
    match message {
        Message::Array(arr) => {
            if arr.items.is_empty() {
                return None;
            }
            let mut parts = Vec::new();
            for item in &arr.items {
                if let Message::BulkString(bs) = item {
                    parts.push(bs.string.clone());
                }
            }
            if parts.is_empty() {
                return None;
            }
            let cmd = parts[0].to_lowercase();
            let args = parts[1..].to_vec();
            Some((cmd, args))
        }
        _ => None,
    }
}

/// Execute a command
pub async fn execute(
    ctx: &CommandContext,
    cmd: &str,
    args: &[String],
    message: &Message,
) -> Result<Option<Message>> {
    match cmd {
        // String commands
        "ping" => server_cmds::ping(ctx, args).await,
        "echo" => server_cmds::echo(ctx, args).await,
        "get" => string::get(ctx, args).await,
        "set" => string::set(ctx, args, message).await,
        "incr" => string::incr(ctx, args).await,

        // List commands
        "lpush" => list::lpush(ctx, args, message).await,
        "rpush" => list::rpush(ctx, args, message).await,
        "lpop" => list::lpop(ctx, args).await,
        "llen" => list::llen(ctx, args).await,
        "lrange" => list::lrange(ctx, args).await,
        "blpop" => list::blpop(ctx, args).await,

        // Stream commands
        "xadd" => stream::xadd(ctx, args, message).await,
        "xrange" => stream::xrange(ctx, args).await,
        "xread" => stream::xread(ctx, args).await,

        // Sorted set commands
        "zadd" => sorted_set::zadd(ctx, args).await,
        "zrange" => sorted_set::zrange(ctx, args).await,
        "zrank" => sorted_set::zrank(ctx, args).await,
        "zcard" => sorted_set::zcard(ctx, args).await,
        "zscore" => sorted_set::zscore(ctx, args).await,
        "zrem" => sorted_set::zrem(ctx, args).await,

        // Pub/Sub commands
        "subscribe" => pubsub::subscribe(ctx, args).await,
        "unsubscribe" => pubsub::unsubscribe(ctx, args).await,
        "publish" => pubsub::publish(ctx, args).await,

        // Server commands
        "info" => server_cmds::info(ctx, args).await,
        "config" => server_cmds::config(ctx, args).await,
        "keys" => server_cmds::keys(ctx, args).await,
        "type" => server_cmds::type_cmd(ctx, args).await,
        "replconf" => server_cmds::replconf(ctx, args, message).await,
        "psync" => server_cmds::psync(ctx, args).await,
        "wait" => server_cmds::wait(ctx, args, message).await,
        "command" => server_cmds::command(ctx, args).await,

        // Transaction commands are handled at a higher level

        // Geo commands
        "geoadd" => geo::add(ctx, args).await,
        "geopos" => geo::pos(ctx, args).await,
        "geodist" => geo::distance(ctx, args).await,
        "geosearch" => geo::search(ctx, args).await,

        "acl" => auth::acl_whoami(ctx, args).await,
        _ => Ok(None),
    }
}

impl CommandContext {
    /// Execute a closure with mutable access to the current database.
    pub async fn with_db_mut<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Database) -> Result<T>,
    {
        let mut rdb = self.server.rdb.write().await;
        let db_index = *self.selected_db.read().await;
        let db = rdb.get_db_mut(db_index)?;
        f(db)
    }

    /// Execute a closure with read access to the current database.
    pub async fn with_db<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Database) -> Result<T>,
    {
        let rdb = self.server.rdb.read().await;
        let db_index = *self.selected_db.read().await;
        let db = rdb.get_db(db_index)?;
        f(db)
    }
}
