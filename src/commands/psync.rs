use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::time::{timeout, Duration};
use crate::message::{Message, Array, BulkString, SimpleString};
use crate::commands::{CommandContext, CommandMessage};

pub async fn handle(_params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    // Send FULLRESYNC response
    let fullresync = format!(
        "FULLRESYNC {} {}",
        ctx.replication_info.master_replid,
        ctx.replication_info.master_repl_offset
    );
    
    let mut writer = ctx.message_writer.lock().await;
    writer.send(Message::SimpleString(SimpleString { string: fullresync })).await?;
    
    // Send RDB
    let rdb = ctx.store.get_rdb().await;
    writer.send(Message::RDB(rdb.clone())).await?;
    drop(rdb);
    drop(writer);
    
    // Update replication state
    {
        let mut state = ctx.replication_state.lock().await;
        state.add_replica();
    }
    
    // Subscribe to command broadcasts and forward to slave
    let mut rx = ctx.command_tx.subscribe();
    
    loop {
        match rx.recv().await {
            Ok(CommandMessage { message, .. }) => {
                // Check if this is a REPLCONF GETACK
                let is_get_ack = match &message {
                    Message::Array(Array { items }) => {
                        items.len() >= 3 &&
                        matches!(&items[0], Message::BulkString(BulkString { string, .. }) if string.to_lowercase() == "replconf") &&
                        matches!(&items[1], Message::BulkString(BulkString { string, .. }) if string.to_lowercase() == "getack") &&
                        matches!(&items[2], Message::BulkString(BulkString { string, .. }) if string == "*")
                    }
                    _ => false,
                };
                
                // Send message to slave
                let mut writer = ctx.message_writer.lock().await;
                writer.send(message.clone()).await?;
                drop(writer);
                
                if is_get_ack {
                    // Wait for ACK response
                    let mut reader = ctx.message_reader.lock().await;
                    match timeout(Duration::from_millis(500), reader.next()).await {
                        Ok(Some(Ok(resp))) => {
                            // Parse ACK response
                            if let Message::Array(Array { items }) = &resp {
                                if items.len() == 3 &&
                                   matches!(&items[0], Message::BulkString(BulkString { string, .. }) if string.to_lowercase() == "replconf") &&
                                   matches!(&items[1], Message::BulkString(BulkString { string, .. }) if string.to_lowercase() == "ack") {
                                    if let Message::BulkString(BulkString { string: offset_str, .. }) = &items[2] {
                                        if let Ok(actual_offset) = offset_str.parse::<usize>() {
                                            let expected_offset = {
                                                let state = ctx.replication_state.lock().await;
                                                state.expected_offset
                                            };
                                            
                                            eprintln!("DEBUG PSYNC ACK: actual_offset: {}, expected_offset: {}", actual_offset, expected_offset);
                                            if actual_offset >= expected_offset {
                                                let mut state = ctx.replication_state.lock().await;
                                                state.increment_ack();
                                                eprintln!("DEBUG PSYNC ACK: Incremented acks");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Some(Err(_))) | Ok(None) => {
                            // Connection error
                            let mut state = ctx.replication_state.lock().await;
                            state.remove_replica();
                            break;
                        }
                        Err(_) => {
                            // Timeout - continue
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
    
    Ok(None)
}