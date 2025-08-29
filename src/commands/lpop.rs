use anyhow::Result;
use crate::message::{Message, Array, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let key = get_param!(0, params, "LPOP");
    let count = if params.len() > 1 {
        params[1].parse::<usize>()
            .map_err(|_| anyhow::anyhow!("Invalid count"))?
    } else {
        1
    };
    
    let db_index = *ctx.selected_db_index.read().await;
    
    match ctx.store.lpop(db_index, key, count).await {
        Ok(values) => {
            match values.len() {
                0 => Ok(Some(Message::BulkString(BulkString {
                    length: -1,
                    string: String::new(),
                }))),
                1 => Ok(Some(Message::BulkString(BulkString {
                    length: values[0].len() as isize,
                    string: values[0].clone(),
                }))),
                _ => {
                    let items: Vec<Message> = values
                        .into_iter()
                        .map(|value| Message::BulkString(BulkString {
                            length: value.len() as isize,
                            string: value,
                        }))
                        .collect();
                    
                    Ok(Some(Message::Array(Array { items })))
                }
            }
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