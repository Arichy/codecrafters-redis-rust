use anyhow::Result;
use crate::message::{Message, Array, BulkString, SimpleString};
use crate::commands::CommandContext;

pub async fn handle(_params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if ctx.is_slave {
        return Ok(None);
    }
    
    if ctx.pubsub_manager.is_subscribed(&ctx.peer_addr).await {
        Ok(Some(Message::Array(Array {
            items: vec![
                Message::BulkString(BulkString {
                    length: 4,
                    string: "pong".to_string(),
                }),
                Message::BulkString(BulkString {
                    length: 0,
                    string: "".to_string(),
                }),
            ],
        })))
    } else {
        Ok(Some(Message::SimpleString(SimpleString {
            string: "PONG".to_string(),
        })))
    }
}