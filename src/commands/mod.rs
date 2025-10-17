use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::message::Message;
use crate::server::Server;

pub mod string;
pub mod list;
pub mod stream;
pub mod sorted_set;
pub mod pubsub;
pub mod server_cmds;
pub mod transaction;

/// Context for command execution
pub struct CommandContext {
    pub server: Server,
    pub client_id: String,
    pub selected_db: Arc<RwLock<usize>>,
    pub is_slave: bool,
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
        _ => Ok(None),
    }
}
