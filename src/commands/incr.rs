use anyhow::Result;
use crate::message::{Message, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let key = get_param!(0, params, "INCR");
    let db_index = *ctx.selected_db_index.read().await;
    
    match ctx.store.incr(db_index, key).await {
        Ok(value) => Ok(Some(Message::Integer(Integer { value }))),
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("WRONGTYPE") || error_msg.contains("not an integer") {
                Ok(Some(Message::SimpleError(SimpleError {
                    string: error_msg,
                })))
            } else {
                Err(e)
            }
        }
    }
}