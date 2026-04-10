use anyhow::Result;

use crate::commands::CommandContext;
use crate::core::zset::{zset_add, zset_card, zset_range, zset_rank, zset_rem, zset_score};
use crate::message::{Integer, Message, SimpleError};

pub async fn zadd(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zadd' command".to_string(),
        })));
    }

    let key = &args[0];
    let score: f64 = args[1].parse()?;
    let member = &args[2];

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let count = zset_add(db, key, member, score)?;

    Ok(Some(Message::Integer(Integer { value: count })))
}

pub async fn zrank(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zrank' command".to_string(),
        })));
    }

    let key = &args[0];
    let member = &args[1];

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    match zset_rank(db, key, member)? {
        Some(rank) => Ok(Some(Message::Integer(Integer { value: rank as i64 }))),
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

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let result = zset_range(db, key, start, end)?;

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

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let count = zset_card(db, key)?;

    Ok(Some(Message::Integer(Integer { value: count as i64 })))
}

pub async fn zscore(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zscore' command".to_string(),
        })));
    }

    let key = &args[0];
    let member = &args[1];

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    match zset_score(db, key, member)? {
        Some(score) => Ok(Some(Message::new_bulk_string(score.to_string()))),
        None => Ok(Some(Message::NullBulkString)),
    }
}

pub async fn zrem(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'zrem' command".to_string(),
        })));
    }

    let key = &args[0];
    let member = &args[1];

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let count = zset_rem(db, key, member)?;

    Ok(Some(Message::Integer(Integer { value: count })))
}
