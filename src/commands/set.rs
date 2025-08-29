use anyhow::Result;
use crate::message::{Message, SimpleString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "SET requires at least 2 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let value = params[1];
    let db_index = *ctx.selected_db_index.read().await;
    
    // Parse expiry if provided
    let expiry = if params.len() > 3 && params[2].to_lowercase() == "px" {
        let timeout_ms: u64 = params[3].parse()
            .map_err(|_| anyhow::anyhow!("Invalid timeout value"))?;
        let current_ts = chrono::Utc::now().timestamp_millis() as u64;
        Some(current_ts + timeout_ms)
    } else {
        None
    };
    
    ctx.store.set_string(db_index, key.to_string(), value.to_string(), expiry).await?;
    
    if !ctx.is_slave {
        Ok(Some(Message::SimpleString(SimpleString {
            string: "OK".to_string(),
        })))
    } else {
        Ok(None)
    }
}