use anyhow::Result;
use crate::message::{Message, Integer, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 3 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ZADD requires at least 3 arguments".to_string(),
        })));
    }
    
    let key = params[0];
    let score: f64 = params[1].parse()
        .map_err(|_| anyhow::anyhow!("Invalid score"))?;
    let member = params[2];
    
    let db_index = *ctx.selected_db_index.read().await;
    
    match ctx.store.zadd(db_index, key, member.to_string(), score).await {
        Ok(added) => Ok(Some(Message::Integer(Integer {
            value: if added { 1 } else { 0 },
        }))),
        Err(e) => {
            if e.to_string().contains("Wrong kind of value") {
                Ok(Some(Message::SimpleError(SimpleError {
                    string: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                })))
            } else {
                Err(e)
            }
        }
    }
}