use crate::{
    commands::{sorted_set::zadd, CommandContext},
    message::Message,
    rdb::Value,
};
use anyhow::{Context as AnyhowContext, Result};

pub async fn add(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let key = &args[0];
    let lo = &args[1];
    let la = &args[2];
    let member = &args[3];

    let zadd_args = &[key.to_string(), "0".to_string(), member.to_string()];

    zadd(ctx, args).await
}
