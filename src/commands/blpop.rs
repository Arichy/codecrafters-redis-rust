use anyhow::Result;
use tokio::time::Duration;
use crate::message::{Message, Array, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "BLPOP requires at least 2 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let timeout_secs: f64 = params[1].parse()
        .map_err(|_| anyhow::anyhow!("Invalid timeout"))?;
    
    let db_index = *ctx.selected_db_index.read().await;
    
    // First, try to pop immediately
    let values = ctx.store.lpop(db_index, key, 1).await?;
    if !values.is_empty() {
        return Ok(Some(Message::Array(Array {
            items: vec![
                Message::BulkString(BulkString {
                    length: key.len() as isize,
                    string: key.to_string(),
                }),
                Message::BulkString(BulkString {
                    length: values[0].len() as isize,
                    string: values[0].clone(),
                }),
            ],
        })));
    }
    
    // If no value available, register as blocking client
    let notify = ctx.blocking_manager.add_blocking_client(
        &ctx.peer_addr,
        key,
        ctx.message_writer.clone(),
        timeout_secs,
    ).await;
    
    // Wait for notification or timeout
    let timeout_duration = if timeout_secs == 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(timeout_secs))
    };
    
    let notified = ctx.blocking_manager.wait_for_key(notify, timeout_duration).await;
    
    if notified {
        // Try to pop again after being notified
        let values = ctx.store.lpop(db_index, key, 1).await?;
        if !values.is_empty() {
            Ok(Some(Message::Array(Array {
                items: vec![
                    Message::BulkString(BulkString {
                        length: key.len() as isize,
                        string: key.to_string(),
                    }),
                    Message::BulkString(BulkString {
                        length: values[0].len() as isize,
                        string: values[0].clone(),
                    }),
                ],
            })))
        } else {
            // This shouldn't happen, but handle it gracefully
            Ok(Some(Message::BulkString(BulkString {
                length: -1,
                string: String::new(),
            })))
        }
    } else {
        // Timed out
        ctx.blocking_manager.remove_client(&ctx.peer_addr).await;
        Ok(Some(Message::BulkString(BulkString {
            length: -1,
            string: String::new(),
        })))
    }
}