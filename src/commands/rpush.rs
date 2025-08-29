use anyhow::Result;
use futures_util::SinkExt;
use crate::message::{Message, Array, BulkString, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "RPUSH requires at least 2 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let mut values: Vec<String> = params[1..].iter().map(|s| s.to_string()).collect();
    let db_index = *ctx.selected_db_index.read().await;
    
    // Check if there are clients waiting for this key
    while !values.is_empty() && ctx.blocking_manager.has_waiting_clients(key).await {
        if let Some(client) = ctx.blocking_manager.pop_waiting_client(key).await {
            // Send the value directly to the waiting client
            let value = values.pop().unwrap();
            let response = Message::Array(Array {
                items: vec![
                    Message::BulkString(BulkString {
                        length: key.len() as isize,
                        string: key.to_string(),
                    }),
                    Message::BulkString(BulkString {
                        length: value.len() as isize,
                        string: value,
                    }),
                ],
            });
            
            let mut writer = client.writer.lock().await;
            let _ = writer.send(response).await;
        }
    }
    
    // Add any remaining values to the list
    if !values.is_empty() {
        match ctx.store.rpush(db_index, key, values).await {
            Ok(len) => Ok(Some(Message::Integer(Integer { value: len as i64 }))),
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
    } else {
        // All values were given to waiting clients
        // Return the current list length
        match ctx.store.llen(db_index, key).await {
            Ok(len) => Ok(Some(Message::Integer(Integer { value: len as i64 }))),
            Err(_) => Ok(Some(Message::Integer(Integer { value: 0 }))),
        }
    }
}