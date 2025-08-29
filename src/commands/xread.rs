use std::collections::HashMap;
use anyhow::Result;
use tokio::time::{timeout, Duration};
use crate::message::{Message, Array, BulkString, SimpleError};
use crate::commands::{CommandContext, CommandMessage};
use crate::rdb::{StreamId, StreamValue};

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "XREAD requires at least 3 arguments".to_string(),
        })));
    }
    
    // Find STREAMS keyword
    let streams_pos = params.iter()
        .position(|p| p.to_lowercase() == "streams")
        .ok_or_else(|| anyhow::anyhow!("STREAMS keyword not found"))?;
    
    // Parse BLOCK option if present
    let block_timeout = params[..streams_pos].windows(2)
        .find(|w| w[0].to_lowercase() == "block")
        .and_then(|w| w[1].parse::<u64>().ok());
    
    // Parse keys and IDs
    let key_id_params = &params[streams_pos + 1..];
    if key_id_params.len() % 2 != 0 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "Unbalanced STREAMS list".to_string(),
        })));
    }
    
    let mid = key_id_params.len() / 2;
    let keys = &key_id_params[..mid];
    let ids = &key_id_params[mid..];
    let db_index = *ctx.selected_db_index.read().await;
    
    // First, try to read immediately
    let mut results = vec![];
    for (key, id_str) in keys.iter().zip(ids.iter()) {
        let entries = if *id_str == "$" {
            // $ means only new entries (none for immediate read)
            vec![]
        } else {
            // Parse the ID and get entries after it
            let (ts, seq_opt) = StreamId::parse_range_str(id_str)?;
            let start = StreamId { 
                ts, 
                seq: seq_opt.map(|s| s + 1).unwrap_or(0) 
            };
            let end = StreamId { ts: i64::MAX, seq: usize::MAX };
            
            ctx.store.xrange(db_index, key, start, end).await?
        };
        
        if !entries.is_empty() {
            let items: Vec<Message> = entries
                .into_iter()
                .map(|entry| StreamValue::get_message_by_record(&entry))
                .collect();
            
            results.push(Message::Array(Array {
                items: vec![
                    Message::BulkString(BulkString {
                        length: key.len() as isize,
                        string: key.to_string(),
                    }),
                    Message::Array(Array { items }),
                ],
            }));
        }
    }
    
    if !results.is_empty() || block_timeout.is_none() {
        return if results.is_empty() {
            Ok(Some(Message::BulkString(BulkString {
                length: -1,
                string: "".to_string(),
            })))
        } else {
            Ok(Some(Message::Array(Array { items: results })))
        };
    }
    
    // Handle blocking
    let mut rx = ctx.command_tx.subscribe();
    let timeout_duration = if block_timeout == Some(0) {
        None
    } else {
        block_timeout.map(Duration::from_millis)
    };
    
    let wait_future = async {
        loop {
            match rx.recv().await {
                Ok(CommandMessage { message, .. }) => {
                    if let Message::Array(Array { items }) = &message {
                        if items.len() >= 3 {
                            if let (
                                Message::BulkString(BulkString { string: cmd, .. }),
                                Message::BulkString(BulkString { string: stream_key, .. }),
                                Message::BulkString(BulkString { string: stream_id, .. }),
                            ) = (&items[0], &items[1], &items[2]) {
                                if cmd.to_lowercase() == "xadd" && keys.contains(&stream_key.as_str()) {
                                    return Some((stream_key.clone(), stream_id.clone()));
                                }
                            }
                        }
                    }
                }
                Err(_) => return None,
            }
        }
    };
    
    let new_entry = if let Some(duration) = timeout_duration {
        timeout(duration, wait_future).await.ok().flatten()
    } else {
        wait_future.await
    };
    
    if let Some((stream_key, _)) = new_entry {
        // Read the new entry
        let key_idx = keys.iter().position(|k| k == &stream_key.as_str()).unwrap();
        let id_str = ids[key_idx];
        
        let entries = if id_str == "$" {
            // Get only the last entry
            let start = StreamId { ts: 0, seq: 0 };
            let end = StreamId { ts: i64::MAX, seq: usize::MAX };
            let all_entries = ctx.store.xrange(db_index, &stream_key, start, end).await?;
            all_entries.into_iter().rev().take(1).collect()
        } else {
            let (ts, seq_opt) = StreamId::parse_range_str(id_str)?;
            let start = StreamId { 
                ts, 
                seq: seq_opt.map(|s| s + 1).unwrap_or(0) 
            };
            let end = StreamId { ts: i64::MAX, seq: usize::MAX };
            ctx.store.xrange(db_index, &stream_key, start, end).await?
        };
        
        if !entries.is_empty() {
            let items: Vec<Message> = entries
                .into_iter()
                .map(|entry| StreamValue::get_message_by_record(&entry))
                .collect();
            
            Ok(Some(Message::Array(Array {
                items: vec![Message::Array(Array {
                    items: vec![
                        Message::BulkString(BulkString {
                            length: stream_key.len() as isize,
                            string: stream_key,
                        }),
                        Message::Array(Array { items }),
                    ],
                })],
            })))
        } else {
            Ok(Some(Message::BulkString(BulkString {
                length: -1,
                string: "".to_string(),
            })))
        }
    } else {
        // Timed out
        Ok(Some(Message::BulkString(BulkString {
            length: -1,
            string: "".to_string(),
        })))
    }
}