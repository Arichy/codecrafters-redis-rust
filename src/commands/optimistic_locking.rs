use eyre::Result;

use crate::{commands::CommandContext, message::Message};

pub async fn watch(ctx: &mut CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::new_error(
            "ERR wrong number of arguments for 'watch' command",
        )));
    }

    for key in args {
        ctx.server
            .watchers
            .register(key.to_string(), ctx.client_id.clone(), ctx.is_dirty.clone());
    }

    Ok(Some(Message::new_simple_string("OK")))
}

pub async fn unwatch(ctx: &mut CommandContext) -> Result<Option<Message>> {
    ctx.server.watchers.unwatch_all(&ctx.client_id);
    Ok(Some(Message::new_simple_string("OK")))
}
