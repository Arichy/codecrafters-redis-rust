use anyhow::Result;
use futures_util::SinkExt;
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
    eprintln!("DEBUG BLPOP: Registering client {} for key: {}", ctx.peer_addr, key);
    let notify = ctx.blocking_manager.add_blocking_client(
        &ctx.peer_addr,
        key,
        ctx.message_writer.clone(),
        timeout_secs,
    ).await;
    
    // Spawn a task to handle timeout
    let blocking_manager = ctx.blocking_manager.clone();
    let peer_addr = ctx.peer_addr.clone();
    let writer = ctx.message_writer.clone();
    let key = key.to_string();
    tokio::spawn(async move {
        let timeout_duration = if timeout_secs == 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(timeout_secs))
        };
        
        let notified = blocking_manager.wait_for_key(notify, timeout_duration).await;
        
        if !notified {
            eprintln!("DEBUG BLPOP: Client {} timed out for key: {}", peer_addr, key);
            // Timed out - send NIL response
            blocking_manager.remove_client(&peer_addr).await;
            let mut writer = writer.lock().await;
            let _ = writer.send(Message::BulkString(BulkString {
                length: -1,
                string: String::new(),
            })).await;
        } else {
            eprintln!("DEBUG BLPOP: Client {} was notified for key: {}", peer_addr, key);
        }
        // If notified, RPUSH/LPUSH already sent the response
    });
    
    // Return None to indicate no immediate response
    Ok(None)
}