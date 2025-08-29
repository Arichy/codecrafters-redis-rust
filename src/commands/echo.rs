use anyhow::Result;
use crate::message::{Message, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], _ctx: &CommandContext) -> Result<Option<Message>> {
    let value = get_param!(0, params, "ECHO");
    
    Ok(Some(Message::BulkString(BulkString {
        length: value.len() as isize,
        string: value.to_string(),
    })))
}