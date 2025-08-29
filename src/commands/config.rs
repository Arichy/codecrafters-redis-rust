use anyhow::Result;
use crate::message::{Message, Array, BulkString, SimpleError};
use crate::commands::CommandContext;
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    let subcommand = get_param!(0, params, "CONFIG");
    
    match subcommand.to_lowercase().as_str() {
        "get" => {
            let config_keys = &params[1..];
            let mut config_entries = vec![];
            
            let config = ctx.config.read().await;
            
            for &config_key in config_keys {
                match config_key {
                    "dir" => {
                        if let Some(dir) = &config.dir {
                            config_entries.push((config_key, dir.to_string_lossy().to_string()));
                        }
                    }
                    "dbfilename" => {
                        if let Some(dbfilename) = &config.dbfilename {
                            config_entries.push((config_key, dbfilename.to_string_lossy().to_string()));
                        }
                    }
                    _ => {}
                }
            }
            
            let items: Vec<Message> = config_entries
                .into_iter()
                .flat_map(|(k, v)| {
                    vec![
                        Message::BulkString(BulkString {
                            length: k.len() as isize,
                            string: k.to_string(),
                        }),
                        Message::BulkString(BulkString {
                            length: v.len() as isize,
                            string: v,
                        }),
                    ]
                })
                .collect();
            
            Ok(Some(Message::Array(Array { items })))
        }
        _ => Ok(None),
    }
}