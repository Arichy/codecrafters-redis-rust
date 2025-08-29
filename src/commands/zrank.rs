use anyhow::Result;
use crate::message::{Message, Integer, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ZRANK requires 2 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let member = params[1];
    
    let db_index = *ctx.selected_db_index.read().await;
    
    match ctx.store.zrank(db_index, key, member).await? {
        Some(rank) => Ok(Some(Message::Integer(Integer {
            value: rank as i64,
        }))),
        None => Ok(Some(Message::BulkString(BulkString {
            length: -1,
            string: "".to_string(),
        }))),
    }
}