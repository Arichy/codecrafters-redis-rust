use anyhow::Result;
use crate::message::{Message, BulkString, SimpleError};
use crate::commands::{CommandContext, CommandMessage};
use crate::rdb::StreamId;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 4 || params.len() % 2 != 0 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "XADD requires stream key, ID, and field-value pairs".to_string(),
        })));
    }
    
    let key = params[0];
    let id_str = params[1];
    let db_index = *ctx.selected_db_index.read().await;
    
    // Parse the ID
    let id = if id_str == "*" {
        ctx.store.generate_stream_id(db_index, key, None).await?
    } else {
        let parts: Vec<_> = id_str.split('-').collect();
        if parts.len() != 2 {
            return Ok(Some(Message::SimpleError(SimpleError {
                string: "ERR Invalid stream ID specified".to_string(),
            })));
        }
        
        let ts: i64 = parts[0].parse()
            .map_err(|_| anyhow::anyhow!("Invalid timestamp in ID"))?;
        
        if parts[1] == "*" {
            ctx.store.generate_stream_id(db_index, key, Some(ts)).await?
        } else {
            let seq: usize = parts[1].parse()
                .map_err(|_| anyhow::anyhow!("Invalid sequence in ID"))?;
            StreamId { ts, seq }
        }
    };
    
    // Parse fields
    let fields: Vec<(String, String)> = params[2..]
        .chunks(2)
        .map(|chunk| (chunk[0].to_string(), chunk[1].to_string()))
        .collect();
    
    // Add to stream
    match ctx.store.xadd(db_index, key, id.clone(), fields).await {
        Ok(id) => {
            let id_str: String = id.into();
            
            // TODO: Notify any waiting XREAD clients
            
            if !ctx.is_slave {
                Ok(Some(Message::BulkString(BulkString {
                    length: id_str.len() as isize,
                    string: id_str,
                })))
            } else {
                Ok(None)
            }
        }
        Err(e) => Ok(Some(Message::SimpleError(SimpleError {
            string: e.to_string(),
        }))),
    }
}