use crate::{
    commands::{geo::encode::encode, sorted_set::zadd, CommandContext},
    message::{Message, SimpleError},
    rdb::Value,
};
use anyhow::{Context as AnyhowContext, Result};

mod decode;
mod encode;

pub async fn add(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let key = &args[0];
    let lo = &args[1];
    let la = &args[2];
    let member = &args[3];

    let lo = match lo.parse::<f64>() {
        Ok(lo) if lo >= -180.0 && lo <= 180.0 => lo,
        _ => {
            return Ok(Some(Message::SimpleError(SimpleError {
                string: format!("ERR invalid longitude, latitude pair {lo}, {la}"),
            })))
        }
    };

    let la = match la.parse::<f64>() {
        Ok(la) if la >= -85.05112878 && la <= 85.05112878 => la,
        _ => {
            return Ok(Some(Message::SimpleError(SimpleError {
                string: format!("ERR invalid longitude, latitude pair {lo}, {la}"),
            })))
        }
    };

    let score = encode(la, lo);

    let zadd_args = &[key.to_string(), score.to_string(), member.to_string()];

    zadd(ctx, zadd_args).await
}
