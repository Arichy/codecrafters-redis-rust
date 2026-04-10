use anyhow::Result;

use crate::{commands::CommandContext, message::Message};

pub async fn acl_whoami(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    Ok(Some(Message::new_bulk_string("default".to_string())))
}
