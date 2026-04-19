use eyre::Result;

use crate::commands::CommandContext;
use crate::core::zset::{zset_add, zset_card, zset_range, zset_rank, zset_rem, zset_score};
use crate::message::{Message, SimpleError};

pub async fn zadd(ctx: &CommandContext, args: &[String], message: &Message) -> Result<Option<Message>> {
    if args.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zadd' command".to_string(),
        })));
    }

    let key = &args[0];
    let score: f64 = args[1].parse()?;
    let member = &args[2];

    let count = ctx.with_db_mut(|db| zset_add(db, key, member, score)).await?;

    // Skip all side effects during AOF replay
    if !ctx.is_replay {
        // Notify watchers if we actually added something
        if count > 0 {
            ctx.server.watchers.notify(key);
        }

        // Update replication offset and broadcast to replicas if this is a master
        if !ctx.is_slave {
            let mut repl = ctx.server.replication.write().await;
            if repl.is_master() {
                repl.acked_replica_count = 0;
                repl.expected_offset += message.length()?;
            }
            drop(repl);
            ctx.server.replicas.broadcast(message).await;

            // Write to AOF file
            if ctx.server.aof.appendonly {
                ctx.server.aof.write_to_current_aof_file(message.clone()).await?;
            }
        }
    }

    Ok(Some(Message::new_integer(count)))
}

pub async fn zrank(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zrank' command".to_string(),
        })));
    }

    let key = &args[0];
    let member = &args[1];

    let rank = ctx.with_db_mut(|db| zset_rank(db, key, member)).await?;

    match rank {
        Some(rank) => Ok(Some(Message::new_integer(rank as i64))),
        None => Ok(Some(Message::NullBulkString)),
    }
}

pub async fn zrange(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zrange' command".to_string(),
        })));
    }

    let key = &args[0];
    let start: i32 = args[1].parse()?;
    let end: i32 = args[2].parse()?;

    let result = ctx.with_db_mut(|db| zset_range(db, key, start, end)).await?;

    Ok(Some(Message::new_array(
        result.iter().map(|item| Message::new_bulk_string(item.clone())).collect()
    )))
}

pub async fn zcard(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zcard' command".to_string(),
        })));
    }

    let key = &args[0];

    let count = ctx.with_db_mut(|db| zset_card(db, key)).await?;

    Ok(Some(Message::new_integer(count as i64)))
}

pub async fn zscore(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zscore' command".to_string(),
        })));
    }

    let key = &args[0];
    let member = &args[1];

    let score = ctx.with_db_mut(|db| zset_score(db, key, member)).await?;

    match score {
        Some(score) => Ok(Some(Message::new_bulk_string(score.to_string()))),
        None => Ok(Some(Message::NullBulkString)),
    }
}

pub async fn zrem(ctx: &CommandContext, args: &[String], message: &Message) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zrem' command".to_string(),
        })));
    }

    let key = &args[0];
    let member = &args[1];

    let count = ctx.with_db_mut(|db| zset_rem(db, key, member)).await?;

    // Skip all side effects during AOF replay
    if !ctx.is_replay {
        // Notify watchers if we actually removed something
        if count > 0 {
            ctx.server.watchers.notify(key);
        }

        // Update replication offset and broadcast to replicas if this is a master
        if !ctx.is_slave {
            let mut repl = ctx.server.replication.write().await;
            if repl.is_master() {
                repl.acked_replica_count = 0;
                repl.expected_offset += message.length()?;
            }
            drop(repl);
            ctx.server.replicas.broadcast(message).await;

            // Write to AOF file
            if ctx.server.aof.appendonly {
                ctx.server.aof.write_to_current_aof_file(message.clone()).await?;
            }
        }
    }

    Ok(Some(Message::new_integer(count)))
}
