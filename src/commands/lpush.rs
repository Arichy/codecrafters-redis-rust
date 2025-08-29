use anyhow::Result;
use crate::message::{Message, Integer, SimpleError};
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
    
    match ctx.store.lpush(db_index, key, values).await {
        Ok(len) => {
            // Check if there are clients waiting for this key
            if ctx.blocking_manager.has_waiting_clients(key).await {
                if let Some((_, writer)) = ctx.blocking_manager.notify_waiting_clients(key).await {
                    // The blocking client will handle popping the value
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