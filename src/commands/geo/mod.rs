use crate::{
    commands::{
        geo::{
            decode::{decode, Coordinates},
            encode::encode,
        },
        sorted_set::{zadd, zrange, zscore},
        CommandContext,
    },
    message::{Array, BulkString, Message, SimpleError},
    rdb::Value,
};
use anyhow::{anyhow, Context as AnyhowContext, Result};

mod decode;
mod distance;
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

pub async fn distance(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let key = &args[0];
    let member1 = &args[1];
    let member2 = &args[2];

    let res = pos(ctx, args).await?.expect("Cannot be None.");
    let Message::Array(Array { mut items }) = res else {
        unreachable!();
    };

    let first = items.pop().unwrap();
    let second = items.pop().unwrap();

    let Message::Array(Array { items: co1 }) = first else {
        return Ok(Some(Message::NullArray));
    };
    let Message::Array(Array { items: co2 }) = second else {
        return Ok(Some(Message::NullArray));
    };

    let Message::BulkString(BulkString {
        length,
        string: lo1_str,
    }) = &co1[0]
    else {
        unreachable!()
    };
    let Message::BulkString(BulkString {
        length,
        string: la1_str,
    }) = &co1[1]
    else {
        unreachable!()
    };

    let Message::BulkString(BulkString {
        length,
        string: lo2_str,
    }) = &co2[0]
    else {
        unreachable!()
    };
    let Message::BulkString(BulkString {
        length,
        string: la2_str,
    }) = &co2[1]
    else {
        unreachable!()
    };

    let dist = distance::haversine(
        Coordinates {
            latitude: la1_str.parse()?,
            longitude: lo1_str.parse()?,
        },
        Coordinates {
            latitude: la2_str.parse()?,
            longitude: lo2_str.parse()?,
        },
    );

    let dist_str = dist.to_string();
    Ok(Some(Message::BulkString(BulkString {
        length: dist_str.len() as isize,
        string: dist_str,
    })))
}

pub async fn search(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let key = &args[0];
    let fromlonlat = &args[1];
    let lo = args[2].parse::<f64>()?;
    let la = args[3].parse::<f64>()?;
    let byradius = &args[4];
    let radius = args[5].parse::<f64>()?;
    let unit = &args[6];

    if !fromlonlat.eq_ignore_ascii_case("fromlonlat") {
        return Err(anyhow!("Only support FROMLONLAT"));
    }

    if !byradius.eq_ignore_ascii_case("byradius") {
        return Err(anyhow!("Only support BYRADIUS"));
    }

    let radius = match unit.as_str() {
        "m" | "M" => radius,
        "km" | "KM" => radius * 1000.0,
        _ => todo!(),
    };

    let mut ret = vec![];
    let Message::Array(Array { items }) =
        zrange(ctx, &[key.to_string(), "0".to_string(), "-1".to_string()])
            .await?
            .unwrap()
    else {
        unreachable!()
    };
    for item in &items {
        let Message::BulkString(BulkString {
            length,
            string: member,
        }) = item
        else {
            unreachable!();
        };

        let member_pos = pos(ctx, &[key.to_string(), member.to_string()])
            .await?
            .unwrap();

        let Message::Array(Array { items }) = member_pos else {
            unreachable!()
        };
        let member_pos = &items[0];
        let Message::Array(Array { items }) = member_pos else {
            unreachable!()
        };
        let Message::BulkString(BulkString {
            length,
            string: member_lo,
        }) = &items[0]
        else {
            unreachable!()
        };
        let Message::BulkString(BulkString {
            length,
            string: member_la,
        }) = &items[1]
        else {
            unreachable!()
        };

        let dist = distance::haversine(
            Coordinates {
                latitude: la,
                longitude: lo,
            },
            Coordinates {
                latitude: member_la.parse()?,
                longitude: member_lo.parse()?,
            },
        );

        if dist <= radius {
            ret.push(Message::BulkString(BulkString {
                length: member.len() as isize,
                string: member.to_string(),
            }));
        }
    }

    Ok(Some(Message::Array(Array { items: ret })))
}
