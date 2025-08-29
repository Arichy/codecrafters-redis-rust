use anyhow::Result;
use crate::message::{Message, BulkString};
use crate::commands::CommandContext;
use crate::replication::Role;

pub async fn handle(_params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let mut info_lines = vec!["# Replication".to_string()];
    
    info_lines.push(format!(
        "role:{}",
        match &ctx.replication_info.role {
            Role::Master => "master",
            Role::Slave(_) => "slave",
        }
    ));
    
    info_lines.push(format!("master_replid:{}", ctx.replication_info.master_replid));
    info_lines.push(format!("master_repl_offset:{}", ctx.replication_info.master_repl_offset));
    
    let info_string = info_lines.join("\r\n");
    
    Ok(Some(Message::BulkString(BulkString {
        length: info_string.len() as isize,
        string: info_string,
    })))
}