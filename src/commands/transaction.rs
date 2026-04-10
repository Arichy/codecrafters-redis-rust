use std::sync::atomic::Ordering;

use anyhow::Result;

use crate::commands::CommandContext;
use crate::message::Message;

/// Handle MULTI command - start a transaction
pub async fn multi(ctx: &mut CommandContext) -> Result<Option<Message>> {
    ctx.in_transaction = true;
    ctx.transaction_queue.clear();
    // Reset dirty flag at transaction start - Release to ensure clean state for new transaction
    ctx.is_dirty.store(false, Ordering::Release);
    Ok(Some(Message::new_simple_string("OK")))
}

/// Handle DISCARD command - abort a transaction
pub async fn discard(ctx: &mut CommandContext) -> Result<Option<Message>> {
    if !ctx.in_transaction {
        return Ok(Some(Message::new_error("ERR DISCARD without MULTI")));
    }
    ctx.transaction_queue.clear();
    ctx.in_transaction = false;
    ctx.server.watchers.unwatch_all(&ctx.client_id);
    Ok(Some(Message::new_simple_string("OK")))
}

/// Handle EXEC command - execute all queued commands
pub async fn exec(ctx: &mut CommandContext) -> Result<Option<Message>> {
    if !ctx.in_transaction {
        return Ok(Some(Message::new_error("ERR EXEC without MULTI")));
    }

    // Drain queue first to avoid borrow conflict
    let queued_messages: Vec<Message> = ctx.transaction_queue.drain(..).collect();

    // Use Acquire to synchronize with Release store in notify()
    if ctx.is_dirty.load(Ordering::Acquire) {
        ctx.in_transaction = false;
        ctx.server.watchers.unwatch_all(&ctx.client_id);
        return Ok(Some(Message::NullArray));
    }

    let mut results = Vec::new();
    for queued_msg in queued_messages {
        if let Some((cmd, args)) = super::parse_command(&queued_msg) {
            // Use execute_inner to avoid recursion through execute -> exec -> execute
            if let Ok(Some(result)) = super::execute_inner(ctx, &cmd, &args, &queued_msg).await {
                results.push(result);
            }
        }
    }

    ctx.in_transaction = false;
    Ok(Some(Message::new_array(results)))
}
