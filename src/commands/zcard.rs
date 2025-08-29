use anyhow::Result;
use crate::message::{Message, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let key = get_param!(0, params, "ZCARD");
    
    let db_index = *ctx.selected_db_index.read().await;
    
    let count = ctx.store.zcard(db_index, key).await?;
    
    Ok(Some(Message::Integer(Integer {
        value: count as i64,
    })))
}