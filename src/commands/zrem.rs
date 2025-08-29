use anyhow::Result;
use crate::message::{Message, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ZREM requires 2 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let member = params[1];
    
    let db_index = *ctx.selected_db_index.read().await;
    
    let removed = ctx.store.zrem(db_index, key, member.to_string()).await?;
    
    Ok(Some(Message::Integer(Integer {
        value: if removed { 1 } else { 0 },
    })))
}