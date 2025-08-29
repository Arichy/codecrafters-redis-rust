use anyhow::Result;
use futures_util::SinkExt;
use crate::message::{Message, Array, BulkString, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "LPUSH requires at least 2 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let values: Vec<String> = params[1..].iter().map(|s| s.to_string()).collect();
    let db_index = *ctx.selected_db_index.read().await;
    
    // First, add values to the list
    match ctx.store.lpush(db_index, key, values).await {
        Ok(len) => {
            // Then check if there are clients waiting for this key
            while ctx.blocking_manager.has_waiting_clients(key).await {
                if let Some(client) = ctx.blocking_manager.pop_waiting_client(key).await {
                    // Try to pop a value for the waiting client
                    match ctx.store.lpop(db_index, key, 1).await {
                        Ok(popped_values) if !popped_values.is_empty() => {
                            let value = &popped_values[0];
                            let response = Message::Array(Array {
                                items: vec![
                                    Message::BulkString(BulkString {
                                        length: key.len() as isize,
                                        string: key.to_string(),
                                    }),
                                    Message::BulkString(BulkString {
                                        length: value.len() as isize,
                                        string: value.clone(),
                                    }),
                                ],
                            });
                            
                            let mut writer = client.writer.lock().await;
                            let _ = writer.send(response).await;
                        }
                        _ => {
                            // No more values in the list
                            break;
                        }
                    }
                }
            }
            
            Ok(Some(Message::Integer(Integer { value: len as i64 })))
        }
        Err(e) => {
            if e.to_string().contains("WRONGTYPE") {
                Ok(Some(Message::SimpleError(SimpleError {
                    string: e.to_string(),
                })))
            } else {
                Err(e)
            }
        }
    }
}