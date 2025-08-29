use anyhow::Result;
use crate::message::{Message, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let key = get_param!(0, params, "LLEN");
    let db_index = *ctx.selected_db_index.read().await;
    
    match ctx.store.llen(db_index, key).await {
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