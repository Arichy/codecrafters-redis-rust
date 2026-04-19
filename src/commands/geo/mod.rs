use crate::{
    commands::{
        geo::{
            decode::{decode, Coordinates},
            encode::encode,
        },
        CommandContext,
    },
    core::zset::{zset_add, zset_range, zset_score},
    message::{Message, SimpleError},
};
use eyre::{eyre, Result};

mod decode;
mod distance;
mod encode;

pub async fn add(ctx: &CommandContext, args: &[String], message: &Message) -> Result<Option<Message>> {
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

    let count = ctx.with_db_mut(|db| zset_add(db, key, member, score as f64)).await?;

    // Skip all side effects during AOF replay
    if !ctx.is_replay {
        // Notify watchers if we actually added something
        if count > 0 {
            ctx.server.watchers.notify(key);
        }

        // Update replication offset and broadcast to replicas if this is a master
        if !ctx.is_slave {
            let mut repl = ctx.server.replication.write().await;
            if repl.is_master() {
                repl.acked_replica_count = 0;
                repl.expected_offset += message.length()?;
            }
            drop(repl);
            ctx.server.replicas.broadcast(message).await;

            // Write to AOF file
            if ctx.server.aof.appendonly {
                ctx.server.aof.write_to_current_aof_file(message.clone()).await?;
            }
        }
    }

    Ok(Some(Message::new_integer(count)))
}

pub async fn pos(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let key = &args[0];
    let members = &args[1..];

    let items = ctx.with_db_mut(|db| {
        let mut items = Vec::with_capacity(members.len());
        for member in members {
            match zset_score(db, key, member)? {
                Some(score) => {
                    let coordinates = decode(score as u64);
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
        Ok(items)
    }).await?;

    Ok(Some(Message::new_array(items)))
}

pub async fn distance(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let key = &args[0];
    let member1 = &args[1];
    let member2 = &args[2];

    let result = ctx.with_db_mut(|db| {
        let score1 = zset_score(db, key, member1)?;
        let score2 = zset_score(db, key, member2)?;

        match (score1, score2) {
            (Some(s1), Some(s2)) => {
                let co1 = decode(s1 as u64);
                let co2 = decode(s2 as u64);
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
                Ok(Some(dist))
            }
            _ => Ok(None),
        }
    }).await?;

    match result {
        Some(dist) => Ok(Some(Message::new_bulk_string(dist.to_string()))),
        None => Ok(Some(Message::NullBulkString)),
    }
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
        return Err(eyre!("Only support FROMLONLAT"));
    }

    if !byradius.eq_ignore_ascii_case("byradius") {
        return Err(eyre!("Only support BYRADIUS"));
    }

    let radius = match unit.as_str() {
        "m" | "M" => radius,
        "km" | "KM" => radius * 1000.0,
        _ => todo!(),
    };

    let ret = ctx.with_db_mut(|db| {
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
        Ok(ret)
    }).await?;

    Ok(Some(Message::new_array(ret)))
}
