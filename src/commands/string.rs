use anyhow::Result;

use crate::commands::CommandContext;
use crate::message::{Message, SimpleError};
use crate::rdb::{StringValue, Value, ValueType};

pub async fn get(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'get' command".to_string(),
        })));
    }

    let key = &args[0];

    ctx.with_db(|db| {
        if let Some(value) = db.map.get(key) {
            if value.is_expired() {
                return Ok(Some(Message::NullBulkString));
            }

            match &value.value {
                ValueType::StringValue(StringValue { string }) => {
                    Ok(Some(Message::new_bulk_string(string.clone())))
                }
                _ => Ok(Some(Message::SimpleError(SimpleError {
                    string: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                }))),
            }
        } else {
            Ok(Some(Message::NullBulkString))
        }
    }).await
}

pub async fn set(ctx: &CommandContext, args: &[String], message: &Message) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'set' command".to_string(),
        })));
    }

    let key = &args[0];
    let value = &args[1];

    // Parse optional arguments
    let expiry = if args.len() > 2 {
        let option = args[2].to_lowercase();
        if option == "px" && args.len() > 3 {
            let timeout = args[3].parse::<u64>()?;
            let current_ts = chrono::Utc::now().timestamp_millis() as u64;
            Some(current_ts + timeout)
        } else if option == "ex" && args.len() > 3 {
            let timeout = args[3].parse::<u64>()?;
            let current_ts = chrono::Utc::now().timestamp_millis() as u64;
            Some(current_ts + timeout * 1000)
        } else {
            None
        }
    } else {
        None
    };

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    db.map.insert(
        key.clone(),
        Value {
            expiry,
            value: ValueType::StringValue(StringValue {
                string: value.clone(),
            }),
        },
    );

    drop(rdb);

    // Notify watchers that this key has changed
    ctx.server.watchers.notify(key);

    // Update replication offset and broadcast to replicas if this is a master
    if !ctx.is_slave {
        let mut repl = ctx.server.replication.write().await;
        if repl.is_master() {
            repl.acked_replica_count = 0;
            repl.expected_offset += message.length()?;
        }
        drop(repl);
        // Broadcast the command to all replicas
        ctx.server.replicas.broadcast(message).await;
    }

    if ctx.is_slave {
        Ok(None)
    } else {
        Ok(Some(Message::new_simple_string("OK")))
    }
}

pub async fn incr(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'incr' command".to_string(),
        })));
    }

    let key = &args[0];

    let result = ctx.with_db_mut(|db| {
        let next_int = if let Some(value) = db.map.get_mut(key) {
            if let ValueType::StringValue(StringValue { string }) = &value.value {
                match string.parse::<i64>() {
                    Ok(n) => {
                        if let Some(next) = n.checked_add(1) {
                            value.value = ValueType::StringValue(StringValue {
                                string: next.to_string(),
                            });
                            Some(next)
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            } else {
                None
            }
        } else {
            // Key doesn't exist, initialize to 1
            db.map.insert(
                key.clone(),
                Value {
                    expiry: None,
                    value: ValueType::StringValue(StringValue {
                        string: "1".to_string(),
                    }),
                },
            );
            Some(1)
        };

        if let Some(n) = next_int {
            Ok(Some(Message::new_integer(n)))
        } else {
            Ok(Some(Message::SimpleError(SimpleError {
                string: "ERR value is not an integer or out of range".to_string(),
            })))
        }
    }).await?;

    // Notify watchers if the key was modified
    if result.is_some() && !matches!(result, Some(Message::SimpleError(_))) {
        ctx.server.watchers.notify(key);
    }

    Ok(result)
}
