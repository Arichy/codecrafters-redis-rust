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
            state.offset
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
    
    // Handle ACK from replicas (when we're the master)
    if !ctx.is_slave && params.len() >= 2 && params[0].to_lowercase() == "ack" {
        if let Ok(replica_offset) = params[1].parse::<usize>() {
            let mut state = ctx.replication_state.lock().await;
            // Check if the replica has acknowledged up to the expected offset
            if replica_offset >= state.expected_offset {
                state.increment_ack();
            }
        }
    }
    
    Ok(Some(Message::SimpleString(SimpleString {
        string: "OK".to_string(),
    })))
}