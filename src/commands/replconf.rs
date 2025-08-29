use anyhow::Result;
use crate::message::{Message, Array, BulkString, SimpleString};
use crate::commands::CommandContext;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.is_empty() {
        return Ok(Some(Message::SimpleString(SimpleString {
            string: "OK".to_string(),
        })));
    }
    
    if ctx.is_slave && params[0].to_lowercase() == "getack" {
        let offset = {
            let state = ctx.replication_state.lock().await;
            // Subtract the length of this REPLCONF GETACK message
            // Note: The actual message length calculation would need to be done properly
            state.offset.saturating_sub(37) // Approximate length of REPLCONF GETACK *
        };
        
        return Ok(Some(Message::Array(Array {
            items: vec![
                Message::BulkString(BulkString {
                    length: 8,
                    string: "REPLCONF".to_string(),
                }),
                Message::BulkString(BulkString {
                    length: 3,
                    string: "ACK".to_string(),
                }),
                Message::BulkString(BulkString {
                    length: offset.to_string().len() as isize,
                    string: offset.to_string(),
                }),
            ],
        })));
    }
    
    Ok(Some(Message::SimpleString(SimpleString {
        string: "OK".to_string(),
    })))
}