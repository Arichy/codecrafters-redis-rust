use anyhow::Result;

use crate::commands::CommandContext;
use crate::message::{Array, BulkString, Integer, Message, SimpleError};
use crate::rdb::zset::ZSet;
use crate::rdb::{SortedSet, Value, ValueType};

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

    let count = match db.get_sorted_set(key.clone()) {
        Ok(Some(sorted_set)) => {
            if sorted_set.zset.add(member.clone(), score) {
                1
            } else {
                0
            }
        }
        Ok(None) => {
            // Key expired or doesn't exist, create new
            db.map.remove(key);
            let mut zset = ZSet::new();
            zset.add(member.clone(), score);
            db.map.insert(
                key.clone(),
                Value {
                    expiry: None,
                    value: ValueType::SortedSet(SortedSet { zset }),
                },
            );
            1
        }
        Err(e) => {
            if let Ok(e) = e.downcast::<SimpleError>() {
                return Ok(Some(Message::SimpleError(e)));
            }
            return Ok(Some(Message::SimpleError(SimpleError {
                string: "ERR unknown error".to_string(),
            })));
        }
    };

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

    match db.get_sorted_set(key.clone()) {
        Ok(Some(sorted_set)) => {
            if let Some(rank) = sorted_set.zset.rank(member) {
                Ok(Some(Message::Integer(Integer { value: rank as i64 })))
            } else {
                Ok(Some(Message::BulkString(BulkString {
                    length: -1,
                    string: String::new(),
                })))
            }
        }
        _ => Ok(Some(Message::BulkString(BulkString {
            length: -1,
            string: String::new(),
        }))),
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

    let result = match db.get_sorted_set(key.clone()) {
        Ok(Some(sorted_set)) => sorted_set.zset.range(start, end),
        _ => vec![],
    };

    let items = result
        .iter()
        .map(|item| Message::BulkString(BulkString {
            length: item.len() as isize,
            string: item.to_string(),
        }))
        .collect();

    Ok(Some(Message::Array(Array { items })))
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

    let count = match db.get_sorted_set(key.clone()) {
        Ok(Some(sorted_set)) => sorted_set.zset.len(),
        _ => 0,
    };

    Ok(Some(Message::Integer(Integer {
        value: count as i64,
    })))
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

    match db.get_sorted_set(key.clone()) {
        Ok(Some(sorted_set)) => {
            if let Some(score) = sorted_set.zset.score(member) {
                Ok(Some(Message::BulkString(BulkString {
                    length: score.to_string().len() as isize,
                    string: score.to_string(),
                })))
            } else {
                Ok(Some(Message::BulkString(BulkString {
                    length: -1,
                    string: String::new(),
                })))
            }
        }
        _ => Ok(Some(Message::BulkString(BulkString {
            length: -1,
            string: String::new(),
        }))),
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

    let count = match db.get_sorted_set(key.clone()) {
        Ok(Some(sorted_set)) => {
            if sorted_set.zset.rem(member.clone()) {
                1
            } else {
                0
            }
        }
        _ => 0,
    };

    Ok(Some(Message::Integer(Integer {
        value: count as i64,
    })))
}
