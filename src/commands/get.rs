use anyhow::Result;
use crate::message::{Message, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let key = get_param!(0, params, "GET");
    let db_index = *ctx.selected_db_index.read().await;
    
    match ctx.store.get_string(db_index, key).await {
        Ok(Some(value)) => Ok(Some(Message::BulkString(BulkString {
            length: value.len() as isize,
            string: value,
        }))),
        Ok(None) => Ok(Some(Message::BulkString(BulkString {
            length: -1,
            string: "".to_string(),
        }))),
        Err(e) => {
            if e.to_string().contains("WRONGTYPE") {
                Ok(Some(Message::SimpleError(SimpleError {
                    string: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                })))
            } else {
                Err(e)
            }
        }
    }
}