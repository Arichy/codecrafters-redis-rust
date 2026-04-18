use eyre::{Context, Result};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::timeout;

use crate::commands::CommandContext;
use crate::message::{Message, SimpleError};
use crate::rdb::{ListValue, Value, ValueType};

pub async fn lpush(ctx: &CommandContext, args: &[String], message: &Message) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'lpush' command".to_string(),
        })));
    }

    let key = &args[0];
    let values: Vec<String> = args[1..].iter().map(|s| s.clone()).collect();

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let entry = db.map.entry(key.clone()).or_insert(Value {
        expiry: None,
        value: ValueType::ListValue(ListValue {
            list: VecDeque::new(),
        }),
    });

    let len = match &mut entry.value {
        ValueType::ListValue(list_value) => {
            for value in values {
                list_value.list.push_front(value);
            }
            list_value.list.len()
        }
        _ => {
            return Ok(Some(Message::SimpleError(SimpleError {
                string: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            })));
        }
    };

    drop(rdb);

    // Notify watchers that this key has changed
    ctx.server.watchers.notify(key);

    // Notify one waiting BLPOP client for this key
    ctx.server.blocking.notify_list_key(key).await;

    // Update replication offset and broadcast to replicas if this is a master
    if !ctx.is_slave {
        let mut repl = ctx.server.replication.write().await;
        if repl.is_master() {
            repl.acked_replica_count = 0;
            repl.expected_offset += message.length()?;
        }
        drop(repl);
        ctx.server.replicas.broadcast(message).await;
    }

    Ok(Some(Message::new_integer(len as i64)))
}

pub async fn rpush(ctx: &CommandContext, args: &[String], message: &Message) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'rpush' command".to_string(),
        })));
    }

    let key = &args[0];
    let values: Vec<String> = args[1..].iter().map(|s| s.clone()).collect();

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let entry = db.map.entry(key.clone()).or_insert(Value {
        expiry: None,
        value: ValueType::ListValue(ListValue {
            list: VecDeque::new(),
        }),
    });

    let len = match &mut entry.value {
        ValueType::ListValue(list_value) => {
            for value in values {
                list_value.list.push_back(value);
            }
            list_value.list.len()
        }
        _ => {
            return Ok(Some(Message::SimpleError(SimpleError {
                string: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            })));
        }
    };

    drop(rdb);

    // Notify watchers that this key has changed
    ctx.server.watchers.notify(key);

    // Notify one waiting BLPOP client for this key
    ctx.server.blocking.notify_list_key(key).await;

    // Update replication offset and broadcast to replicas if this is a master
    if !ctx.is_slave {
        let mut repl = ctx.server.replication.write().await;
        if repl.is_master() {
            repl.acked_replica_count = 0;
            repl.expected_offset += message.length()?;
        }
        drop(repl);
        ctx.server.replicas.broadcast(message).await;
    }

    Ok(Some(Message::new_integer(len as i64)))
}

pub async fn lpop(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'lpop' command".to_string(),
        })));
    }

    let key = &args[0];
    let count = if args.len() > 1 {
        args[1].parse::<usize>().unwrap_or(1)
    } else {
        1
    };

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let result = lpop_internal(db, key, count);
    drop(rdb);

    // Notify watchers if we actually popped something
    if !matches!(result, Message::NullBulkString) {
        ctx.server.watchers.notify(key);
    }

    Ok(Some(result))
}

pub async fn llen(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'llen' command".to_string(),
        })));
    }

    let key = &args[0];

    let rdb = ctx.server.rdb.read().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db(db_index)?;

    if let Some(value) = db.map.get(key) {
        match &value.value {
            ValueType::ListValue(list_value) => {
                Ok(Some(Message::new_integer(list_value.list.len() as i64)))
            }
            _ => Ok(Some(Message::SimpleError(SimpleError {
                string: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            }))),
        }
    } else {
        Ok(Some(Message::new_integer(0)))
    }
}

pub async fn lrange(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'lrange' command".to_string(),
        })));
    }

    let key = &args[0];
    let start = args[1].parse::<i32>()?;
    let end = args[2].parse::<i32>()?;

    let rdb = ctx.server.rdb.read().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db(db_index)?;

    let result = if let Some(value) = db.map.get(key) {
        match &value.value {
            ValueType::ListValue(list_value) => {
                // Convert VecDeque to Vec for easier indexing
                let list_vec: Vec<String> = list_value.list.iter().cloned().collect();
                index_list(&list_vec, start, end)
            }
            _ => vec![],
        }
    } else {
        vec![]
    };

    Ok(Some(Message::new_array(
        result.iter().map(|item| Message::new_bulk_string(item.to_string())).collect()
    )))
}

pub async fn blpop(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'blpop' command".to_string(),
        })));
    }

    let key = &args[0];
    let timeout_secs = args[1].parse::<f64>()?;

    // Register waiter FIRST to avoid race condition
    // (RPUSH could happen between checking and registering)
    let notify = ctx.server.blocking.register_list_waiter(key.clone()).await;

    // Then check if list already has elements
    {
        let mut rdb = ctx.server.rdb.write().await;
        let db_index = *ctx.selected_db.read().await;
        let db = rdb.get_db_mut(db_index)?;

        if let Some(value) = db.map.get(key) {
            if let ValueType::ListValue(list_value) = &value.value {
                if !list_value.list.is_empty() {
                    // List has elements, pop immediately
                    // Remove our waiter since we're not waiting anymore
                    ctx.server.blocking.remove_list_waiter(key, &notify).await;
                    let result = lpop_internal(db, key, 1);
                    return Ok(Some(Message::new_array(vec![
                        Message::new_bulk_string(key.clone()),
                        result,
                    ])));
                }
            }
        }
    }

    // List is empty, wait for notification or timeout
    let wait_result = if timeout_secs == 0.0 {
        // Block indefinitely
        notify.notified().await;
        Ok(())
    } else {
        // Block with timeout
        timeout(Duration::from_secs_f64(timeout_secs), notify.notified()).await
    };

    match wait_result {
        Ok(_) => {
            // Notified - try to pop
            let mut rdb = ctx.server.rdb.write().await;
            let db_index = *ctx.selected_db.read().await;
            let db = rdb.get_db_mut(db_index)?;

            let result = lpop_internal(db, key, 1);

            // Check if we actually got an element
            if matches!(result, Message::NullBulkString) {
                // List is still empty (race condition - another client got it)
                // Return null array since we timed out waiting
                Ok(Some(Message::NullArray))
            } else {
                Ok(Some(Message::new_array(vec![
                    Message::new_bulk_string(key.clone()),
                    result,
                ])))
            }
        }
        Err(_) => {
            // Timeout - clean up our waiter so RPUSH doesn't notify a dead waiter
            ctx.server.blocking.remove_list_waiter(key, &notify).await;
            Ok(Some(Message::NullArray))
        }
    }
}

// Helper function to pop from list
fn lpop_internal(db: &mut crate::rdb::Database, key: &str, count: usize) -> Message {
    if let Some(value) = db.map.get_mut(key) {
        match &mut value.value {
            ValueType::ListValue(list_value) => {
                let actual_count = count.min(list_value.list.len());
                let removed: Vec<String> = list_value.list.drain(..actual_count).collect();

                match removed.len() {
                    0 => Message::NullBulkString,
                    1 => Message::new_bulk_string(removed[0].clone()),
                    _ => Message::new_array(
                        removed.iter().map(|s| Message::new_bulk_string(s.to_string())).collect()
                    ),
                }
            }
            _ => Message::SimpleError(SimpleError {
                string: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            }),
        }
    } else {
        Message::NullBulkString
    }
}

// Helper function to index a list with Redis-style negative indices
fn index_list(slice: &[String], start: i32, end: i32) -> Vec<String> {
    let len = slice.len() as i32;
    if len == 0 {
        return vec![];
    }

    let start_idx = if start < 0 {
        (len + start).max(0) as usize
    } else {
        start.min(len) as usize
    };

    let end_idx = if end < 0 {
        (len + end).max(-1) as usize + 1
    } else {
        (end + 1).min(len) as usize
    };

    if start_idx >= end_idx {
        vec![]
    } else {
        slice[start_idx..end_idx].to_vec()
    }
}
