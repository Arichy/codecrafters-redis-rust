use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;

use crate::commands::CommandContext;
use crate::message::{Message, SimpleError};
use crate::rdb::{StreamId, StreamValue, Value, ValueType};

pub async fn xadd(
    ctx: &CommandContext,
    args: &[String],
    message: &Message,
) -> Result<Option<Message>> {
    if args.len() < 4 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'xadd' command".to_string(),
        })));
    }

    let key = &args[0];
    let id_str = &args[1];

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    // Parse or generate ID
    let id = if id_str == "*" {
        match db.generate_stream_id(key, None) {
            Ok(id) => id,
            Err(e) => {
                return Ok(Some(Message::SimpleError(SimpleError {
                    string: e.to_string(),
                })));
            }
        }
    } else {
        let splits: Vec<_> = id_str.split("-").collect();
        if splits.len() != 2 {
            return Ok(Some(Message::SimpleError(SimpleError {
                string: "ERR Invalid stream ID".to_string(),
            })));
        }
        let ts: i64 = splits[0].parse()?;
        let seq_str = splits[1];
        if seq_str == "*" {
            match db.generate_stream_id(key, Some(ts)) {
                Ok(id) => id,
                Err(e) => {
                    return Ok(Some(Message::SimpleError(SimpleError {
                        string: e.to_string(),
                    })));
                }
            }
        } else {
            let seq: usize = seq_str.parse()?;
            StreamId { ts, seq }
        }
    };

    if id <= (StreamId { ts: 0, seq: 0 }) {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR The ID specified in XADD must be greater than 0-0".to_string(),
        })));
    }

    // Convert ID to string before moving
    let id_string: String = id.clone().into();

    // Insert into stream - check last ID first before converting
    let entry = db.map.entry(key.clone()).or_insert(Value {
        expiry: None,
        value: ValueType::StreamValue(StreamValue { stream: vec![] }),
    });

    match &mut entry.value {
        ValueType::StreamValue(stream_value) => {
            if let Some(last) = stream_value.stream.last() {
                let last_id: StreamId = last.get("id").unwrap().parse()?;
                if id <= last_id {
                    return Ok(Some(Message::SimpleError(SimpleError {
                        string: "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string(),
                    })));
                }
            }

            // Create stream data from field-value pairs
            let mut stream_data = BTreeMap::new();
            stream_data.insert("id".to_string(), id_string.clone());
            for chunk in args[2..].chunks(2) {
                if chunk.len() == 2 {
                    stream_data.insert(chunk[0].clone(), chunk[1].clone());
                }
            }

            stream_value.stream.push(stream_data);
        }
        _ => {
            return Ok(Some(Message::SimpleError(SimpleError {
                string: "WRONGTYPE Operation against a key holding the wrong kind of value"
                    .to_string(),
            })));
        }
    }

    drop(rdb);

    // Notify watchers that this key has changed
    ctx.server.watchers.notify(key);

    // Notify all waiting XREAD clients for this key
    ctx.server.blocking.notify_stream_key(key).await;

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

    if ctx.is_slave {
        Ok(None)
    } else {
        Ok(Some(Message::new_bulk_string(id_string)))
    }
}

pub async fn xrange(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'xrange' command".to_string(),
        })));
    }

    let key = &args[0];
    let start_str = &args[1];
    let end_str = &args[2];

    let start = if start_str == "-" {
        (0, Some(0))
    } else {
        StreamId::parse_range_str(start_str)?
    };

    let end = if end_str == "+" {
        (i64::MAX, Some(usize::MAX))
    } else {
        StreamId::parse_range_str(end_str)?
    };

    let rdb = ctx.server.rdb.read().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db(db_index)?;

    let items = if let Some(value) = db.map.get(key) {
        if value.is_expired() {
            vec![]
        } else {
            match &value.value {
                ValueType::StreamValue(stream_value) => {
                    let start_id = StreamId {
                        ts: start.0,
                        seq: start.1.unwrap_or(0),
                    };
                    let end_id = StreamId {
                        ts: end.0,
                        seq: end.1.unwrap_or(usize::MAX),
                    };

                    let start_index = stream_value
                        .stream
                        .binary_search_by(|cur| {
                            cur.get("id")
                                .unwrap()
                                .parse::<StreamId>()
                                .unwrap()
                                .cmp(&start_id)
                        })
                        .unwrap_or_else(|x| x);

                    let end_index = stream_value
                        .stream
                        .binary_search_by(|cur| {
                            cur.get("id")
                                .unwrap()
                                .parse::<StreamId>()
                                .unwrap()
                                .cmp(&end_id)
                        })
                        .map_or_else(|x| x.saturating_sub(1), |x| x);

                    if start_index <= end_index {
                        stream_value.get_by_range(start_index..=end_index)
                    } else {
                        vec![]
                    }
                }
                _ => vec![],
            }
        }
    } else {
        vec![]
    };

    Ok(Some(Message::new_array(items)))
}

pub async fn xread(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'xread' command".to_string(),
        })));
    }

    // Parse BLOCK option
    let mut block_ms: Option<u64> = None;
    let mut streams_idx = 0;

    for (i, arg) in args.iter().enumerate() {
        if arg.to_lowercase() == "block" && i + 1 < args.len() {
            block_ms = Some(args[i + 1].parse()?);
        }
        if arg.to_lowercase() == "streams" {
            streams_idx = i + 1;
            break;
        }
    }

    if streams_idx == 0 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR 'streams' keyword is required".to_string(),
        })));
    }

    let key_ids = &args[streams_idx..];
    if key_ids.len() % 2 != 0 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.".to_string(),
        })));
    }

    let (keys, ids) = key_ids.split_at(key_ids.len() / 2);

    // If blocking, register waiters and wait
    if let Some(block_ms) = block_ms {
        // Try to read first
        let result = read_streams(ctx, keys, ids, false).await?;

        if !is_empty_result(&result) {
            return Ok(Some(result));
        }

        // No data, need to block
        // Create a single shared Notify for all stream keys
        let shared_notify = Arc::new(Notify::new());
        let mut notifies: Vec<(String, Arc<Notify>)> = Vec::new();
        for key in keys {
            ctx.server
                .blocking
                .register_stream_waiter_with_notify(key.to_string(), Arc::clone(&shared_notify))
                .await;
            notifies.push((key.to_string(), Arc::clone(&shared_notify)));
        }

        // Wait for notification or timeout
        let wait_result = if block_ms == 0 {
            tokio::select! {
                _ = shared_notify.notified() => Ok(()),
            }
        } else {
            let timeout_dur = Duration::from_millis(block_ms);
            match timeout(timeout_dur, shared_notify.notified()).await {
                Ok(()) => Ok(()),
                Err(_) => Err(()),
            }
        };

        // Clean up waiters
        for (key, notify) in notifies {
            ctx.server
                .blocking
                .remove_stream_waiter(&key, &notify)
                .await;
        }

        match wait_result {
            Ok(_) => {
                // Got notification, read again
                let result = read_streams(ctx, keys, ids, true).await?;
                Ok(Some(result))
            }
            Err(_) => {
                // Timeout
                Ok(Some(Message::NullArray))
            }
        }
    } else {
        // Non-blocking read
        let result = read_streams(ctx, keys, ids, false).await?;
        Ok(Some(result))
    }
}

async fn read_streams(
    ctx: &CommandContext,
    keys: &[String],
    ids: &[String],
    after_block: bool,
) -> Result<Message> {
    let rdb = ctx.server.rdb.read().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db(db_index)?;

    let mut result_streams = Vec::new();

    for (key, id_str) in keys.iter().zip(ids.iter()) {
        if let Some(value) = db.map.get(key) {
            if !value.is_expired() {
                if let ValueType::StreamValue(stream_value) = &value.value {
                    // Handle special '$' ID
                    let items = if id_str == "$" {
                        if after_block {
                            // After blocking, return the last entry
                            if let Some(last) = stream_value.stream.last() {
                                vec![StreamValue::get_message_by_record(last)]
                            } else {
                                vec![]
                            }
                        } else {
                            // '$' means: entries added after this call, so return nothing now
                            vec![]
                        }
                    } else {
                        // Parse the ID
                        let start = StreamId::parse_range_str(id_str)?;
                        let start_id = StreamId {
                            ts: start.0,
                            seq: start.1.unwrap_or(0),
                        };

                        // Find entries AFTER this ID
                        let start_index = stream_value
                            .stream
                            .binary_search_by(|cur| {
                                cur.get("id")
                                    .unwrap()
                                    .parse::<StreamId>()
                                    .unwrap()
                                    .cmp(&start_id)
                            })
                            .map(|x| x + 1) // If found exact match, start after it
                            .unwrap_or_else(|x| x); // If not found, x is the insertion point

                        if start_index < stream_value.stream.len() {
                            stream_value.get_by_range(start_index..)
                        } else {
                            vec![]
                        }
                    };

                    if !items.is_empty() {
                        result_streams.push(Message::new_array(vec![
                            Message::new_bulk_string(key.clone()),
                            Message::new_array(items),
                        ]));
                    }
                }
            }
        }
    }

    if result_streams.is_empty() {
        Ok(Message::NullBulkString)
    } else {
        Ok(Message::new_array(result_streams))
    }
}

fn is_empty_result(msg: &Message) -> bool {
    matches!(msg, Message::NullBulkString)
}
