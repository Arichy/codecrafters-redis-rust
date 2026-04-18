use eyre::Result;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, RwLock};

use crate::aof::AOF;
use crate::commands::auth::User;
use crate::message::Message;
use crate::rdb::Database;
use crate::server::Server;

pub mod auth;
pub mod geo;
pub mod list;
pub mod optimistic_locking;
pub mod pubsub;
pub mod server_cmds;
pub mod sorted_set;
pub mod stream;
pub mod string;
pub mod transaction;

/// Server configuration (read-only, shared via Arc)
#[derive(Debug)]
pub struct ServerConfig {
    pub dir: PathBuf,
    pub dbfilename: Option<PathBuf>,
    pub aof: Arc<AOF>,
}

/// Context for command execution
pub struct CommandContext {
    pub server: Server,
    pub client_id: String,
    pub selected_db: Arc<RwLock<usize>>,
    pub is_slave: bool,
    pub config: Arc<ServerConfig>,
    pub current_user: Option<Arc<User>>,
    pub in_transaction: bool,
    pub transaction_queue: VecDeque<Message>,
    pub is_dirty: Arc<AtomicBool>,
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

/// Execute a command (inner implementation - callable from within EXEC)
pub(crate) async fn execute_inner(
    ctx: &mut CommandContext,
    cmd: &str,
    args: &[String],
    message: &Message,
) -> Result<Option<Message>> {
    match cmd {
        // Transaction commands - MULTI/DISCARD handled directly, EXEC uses special path
        "multi" => Ok(Some(Message::new_simple_string("OK"))),
        "exec" => Ok(Some(Message::new_error(
            "ERR EXEC inside MULTI is not allowed",
        ))),
        "discard" => Ok(Some(Message::new_simple_string("OK"))),

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

        // Geo commands
        "geoadd" => geo::add(ctx, args).await,
        "geopos" => geo::pos(ctx, args).await,
        "geodist" => geo::distance(ctx, args).await,
        "geosearch" => geo::search(ctx, args).await,

        "acl" => auth::acl(ctx, args).await,
        "auth" => auth::auth(ctx, args).await,

        "watch" => optimistic_locking::watch(ctx, args).await,
        "unwatch" => optimistic_locking::unwatch(ctx).await,
        _ => Ok(None),
    }
}

/// Execute a command
pub async fn execute(
    ctx: &mut CommandContext,
    cmd: &str,
    args: &[String],
    message: &Message,
) -> Result<Option<Message>> {
    match cmd {
        // Transaction commands
        "multi" => transaction::multi(ctx).await,
        "exec" => transaction::exec(ctx).await,
        "discard" => transaction::discard(ctx).await,

        // All other commands go through execute_inner
        _ => execute_inner(ctx, cmd, args, message).await,
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
