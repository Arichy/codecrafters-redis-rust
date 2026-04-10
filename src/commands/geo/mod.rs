use crate::{
    commands::{
        geo::{
            decode::{decode, Coordinates},
            encode::encode,
        },
        CommandContext,
    },
    core::zset::{zset_add, zset_range, zset_score},
    message::{Integer, Message, SimpleError},
};
use anyhow::{anyhow, Result};

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

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let count = zset_add(db, key, member, score as f64)?;

    Ok(Some(Message::Integer(Integer { value: count })))
}

pub async fn pos(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let key = &args[0];
    let members = &args[1..];

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let mut items = Vec::with_capacity(members.len());

    for member in members {
        match zset_score(db, key, member)? {
            Some(score) => {
                let score_int = score as u64;
                let coordinates = decode(score_int);
                items.push(Message::new_array(vec![
                    Message::new_bulk_string(coordinates.longitude.to_string()),
                    Message::new_bulk_string(coordinates.latitude.to_string()),
                ]));
            }
            None => {
                items.push(Message::NullArray);
            }
        }
    }

    Ok(Some(Message::new_array(items)))
}

pub async fn distance(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let key = &args[0];
    let member1 = &args[1];
    let member2 = &args[2];

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let score1 = zset_score(db, key, member1)?;
    let score2 = zset_score(db, key, member2)?;

    // Check if either member doesn't exist
    let Some(score1) = score1 else {
        return Ok(Some(Message::NullBulkString));
    };
    let Some(score2) = score2 else {
        return Ok(Some(Message::NullBulkString));
    };

    let co1 = decode(score1 as u64);
    let co2 = decode(score2 as u64);

    let dist = distance::haversine(
        Coordinates {
            latitude: co1.latitude,
            longitude: co1.longitude,
        },
        Coordinates {
            latitude: co2.latitude,
            longitude: co2.longitude,
        },
    );

    Ok(Some(Message::new_bulk_string(dist.to_string())))
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

    let mut rdb = ctx.server.rdb.write().await;
    let db_index = *ctx.selected_db.read().await;
    let db = rdb.get_db_mut(db_index)?;

    let members = zset_range(db, key, 0, -1)?;

    let mut ret = vec![];
    for member in &members {
        if let Some(score) = zset_score(db, key, member)? {
            let coordinates = decode(score as u64);

            let dist = distance::haversine(
                Coordinates {
                    latitude: la,
                    longitude: lo,
                },
                Coordinates {
                    latitude: coordinates.latitude,
                    longitude: coordinates.longitude,
                },
            );

            if dist <= radius {
                ret.push(Message::new_bulk_string(member.clone()));
            }
        }
    }

    Ok(Some(Message::new_array(ret)))
}
