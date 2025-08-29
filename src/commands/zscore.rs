use anyhow::Result;
use crate::message::{Message, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ZSCORE requires 2 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let member = params[1];
    
    let db_index = *ctx.selected_db_index.read().await;
    
    match ctx.store.zscore(db_index, key, member).await? {
        Some(score) => Ok(Some(Message::BulkString(BulkString {
            length: score.to_string().len() as isize,
            string: score.to_string(),
        }))),
        None => Ok(Some(Message::BulkString(BulkString {
            length: -1,
            string: "".to_string(),
        }))),
    }
}