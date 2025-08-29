use anyhow::Result;
use crate::message::{Message, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "PUBLISH requires 2 arguments".to_string(),
        })));
    }
    
    let channel = params[0];
    let message = params[1];
    
    let count = ctx.pubsub_manager.publish(channel, message).await?;
    
    Ok(Some(Message::Integer(Integer {
        value: count as i64,
    })))
}