use anyhow::Result;
use futures_util::SinkExt;

use crate::commands::CommandContext;
use crate::message::{Integer, Message, SimpleError};

pub async fn subscribe(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'subscribe' command".to_string(),
        })));
    }

    let channel = &args[0];

    // This will be handled at the connection level to get the writer
    // For now, return a placeholder

    Ok(Some(Message::new_array(vec![
        Message::new_bulk_string("subscribe".to_string()),
        Message::new_bulk_string(channel.clone()),
        Message::Integer(Integer { value: 1 }),
    ])))
}

pub async fn unsubscribe(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'unsubscribe' command".to_string(),
        })));
    }

    let channel = &args[0];

    let remaining = ctx
        .server
        .pubsub
        .unsubscribe(&ctx.client_id, channel)
        .await;

    Ok(Some(Message::new_array(vec![
        Message::new_bulk_string("unsubscribe".to_string()),
        Message::new_bulk_string(channel.clone()),
        Message::Integer(Integer {
            value: remaining as i64,
        }),
    ])))
}

pub async fn publish(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'publish' command".to_string(),
        })));
    }

    let channel = &args[0];
    let message_str = &args[1];

    let subscribers = ctx.server.pubsub.get_subscribers(channel).await;
    let count = subscribers.len();

    let message = Message::new_array(vec![
        Message::new_bulk_string("message".to_string()),
        Message::new_bulk_string(channel.clone()),
        Message::new_bulk_string(message_str.clone()),
    ]);

    for writer in subscribers {
        let mut writer_locked = writer.lock().await;
        let _ = writer_locked.send(message.clone()).await;
    }

    Ok(Some(Message::Integer(Integer {
        value: count as i64,
    })))
}
