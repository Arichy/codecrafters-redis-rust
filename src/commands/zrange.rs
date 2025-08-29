use anyhow::Result;
use crate::message::{Message, Array, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ZRANGE requires 3 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let start: i32 = params[1].parse()
        .map_err(|_| anyhow::anyhow!("Invalid start index"))?;
    let end: i32 = params[2].parse()
        .map_err(|_| anyhow::anyhow!("Invalid end index"))?;
    
    let db_index = *ctx.selected_db_index.read().await;
    
    let members = ctx.store.zrange(db_index, key, start, end).await?;
    
    let items: Vec<Message> = members
        .into_iter()
        .map(|member| Message::BulkString(BulkString {
            length: member.len() as isize,
            string: member,
        }))
        .collect();
    
    Ok(Some(Message::Array(Array { items })))
}