use anyhow::Result;
use crate::message::{Message, Array, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "LRANGE requires 3 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let start: i32 = params[1].parse()
        .map_err(|_| anyhow::anyhow!("Invalid start index"))?;
    let end: i32 = params[2].parse()
        .map_err(|_| anyhow::anyhow!("Invalid end index"))?;
    
    let db_index = *ctx.selected_db_index.read().await;
    
    match ctx.store.lrange(db_index, key, start, end).await {
        Ok(values) => {
            let items: Vec<Message> = values
                .into_iter()
                .map(|value| Message::BulkString(BulkString {
                    length: value.len() as isize,
                    string: value,
                }))
                .collect();
            
            Ok(Some(Message::Array(Array { items })))
        }
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