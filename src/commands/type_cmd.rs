use anyhow::Result;
use crate::message::{Message, SimpleString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let key = get_param!(0, params, "TYPE");
    let db_index = *ctx.selected_db_index.read().await;
    
    let type_str = ctx.store.get_type(db_index, key).await?;
    
    Ok(Some(Message::SimpleString(SimpleString {
        string: type_str,
    })))
}