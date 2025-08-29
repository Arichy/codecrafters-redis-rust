use anyhow::Result;
use crate::message::{Message, Array, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let pattern = get_param!(0, params, "KEYS");
    let db_index = *ctx.selected_db_index.read().await;
    
    let keys = ctx.store.keys(db_index, pattern).await?;
    
    let items: Vec<Message> = keys
        .into_iter()
        .map(|key| Message::BulkString(BulkString {
            length: key.len() as isize,
            string: key,
        }))
        .collect();
    
    Ok(Some(Message::Array(Array { items })))
}