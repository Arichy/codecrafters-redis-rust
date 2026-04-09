use crate::{
    commands::{
        geo::{decode::decode, encode::encode},
        sorted_set::{zadd, zscore},
        CommandContext,
    },
    message::{Array, BulkString, Message, SimpleError},
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

pub async fn pos(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let key = &args[0];
    let members = &args[1..];

    let mut items = Vec::with_capacity(members.len());

    for member in members {
        let zscore_args = &[key.clone(), member.clone()];
        match zscore(ctx, zscore_args).await? {
            Some(Message::BulkString(BulkString { length, string })) => {
                if length == -1 {
                    items.push(Message::NullArray);
                } else {
                    let score = string.parse::<u64>()?;
                    let coordinates = decode(score);
                    let la_str = coordinates.latitude.to_string();
                    let lo_str = coordinates.longitude.to_string();
                    items.push(Message::Array(Array {
                        items: vec![
                            Message::BulkString(BulkString {
                                length: lo_str.len() as isize,
                                string: lo_str,
                            }),
                            Message::BulkString(BulkString {
                                length: la_str.len() as isize,
                                string: la_str,
                            }),
                        ],
                    }));
                }
            }
            Some(msg) => return Ok(Some(msg)),
            None => unreachable!(),
        }
    }

    Ok(Some(Message::Array(Array { items })))
}
