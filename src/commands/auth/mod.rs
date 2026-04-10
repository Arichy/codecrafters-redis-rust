use anyhow::{anyhow, Result};

use crate::{commands::CommandContext, message::Message};

pub async fn acl(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let subcommand = &args[0].to_lowercase();

    match subcommand.as_str() {
        "whoami" => whoami(ctx, args).await,
        "getuser" => getuser(ctx, args).await,
        other => Err(anyhow!("Unsupport subcommand: {other}")),
    }
}

async fn whoami(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    Ok(Some(Message::new_bulk_string("default".to_string())))
}

pub async fn getuser(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    Ok(Some(Message::new_array(vec![
        Message::new_bulk_string("flags".to_string()),
        Message::new_array(vec![]),
    ])))
}
