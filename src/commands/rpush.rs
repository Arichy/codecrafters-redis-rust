use anyhow::Result;
use crate::message::{Message, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "RPUSH requires at least 2 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let values: Vec<String> = params[1..].iter().map(|s| s.to_string()).collect();
    let db_index = *ctx.selected_db_index.read().await;
    
    match ctx.store.rpush(db_index, key, values).await {
        Ok(len) => Ok(Some(Message::Integer(Integer { value: len as i64 }))),
        Err(e) => {
            if e.to_string().contains("WRONGTYPE") {
                Ok(Some(Message::SimpleError(SimpleError {
                    string: e.to_string(),
                })))
            } else {
                Err(e)
            }
        }
    }
}