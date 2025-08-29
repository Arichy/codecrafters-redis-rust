use anyhow::Result;
use crate::message::{Message, Array, BulkString, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let channel = get_param!(0, params, "SUBSCRIBE");
    
    let count = ctx.pubsub_manager.subscribe(
        &ctx.peer_addr,
        channel,
        ctx.message_writer.clone(),
    ).await;
    
    Ok(Some(Message::Array(Array {
        items: vec![
            Message::BulkString(BulkString {
                length: 9,
                string: "subscribe".to_string(),
            }),
            Message::BulkString(BulkString {
                length: channel.len() as isize,
                string: channel.to_string(),
            }),
            Message::Integer(Integer {
                value: count as i64,
            }),
        ],
    })))
}