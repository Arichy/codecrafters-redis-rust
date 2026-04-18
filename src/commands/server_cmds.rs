use eyre::{Context, Result};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{timeout, Instant};

use crate::commands::CommandContext;
use crate::message::{Message, SimpleError};
use crate::rdb::ValueType;
use crate::server::Role;

pub async fn ping(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if ctx.is_slave {
        return Ok(None);
    }

    let is_subscribed = ctx.server.pubsub.is_subscribed(&ctx.client_id).await;

    if is_subscribed {
        Ok(Some(Message::new_array(vec![
            Message::new_bulk_string("pong".to_string()),
            Message::new_bulk_string("".to_string()),
        ])))
    } else {
        Ok(Some(Message::new_simple_string("PONG")))
    }
}

pub async fn echo(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'echo' command".to_string(),
        })));
    }

    let echo_str = &args[0];
    Ok(Some(Message::new_bulk_string(echo_str.clone())))
}

pub async fn info(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    let repl = ctx.server.replication.read().await;

    let mut info_list = vec!["# Replication".to_string()];
    info_list.push(format!(
        "role:{}",
        if repl.is_slave() { "slave" } else { "master" }
    ));
    info_list.push(format!("master_replid:{}", repl.master_replid));
    info_list.push(format!("master_repl_offset:{}", repl.master_repl_offset));

    let info_string = info_list.join("\r\n");

    Ok(Some(Message::new_bulk_string(info_string)))
}

pub async fn config(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'config' command".to_string(),
        })));
    }

    let subcommand = args[0].to_lowercase();
    match subcommand.as_str() {
        "get" => {
            if args.len() < 2 {
                return Ok(Some(Message::SimpleError(SimpleError {
                    string: "ERR wrong number of arguments for 'config get' command".to_string(),
                })));
            }

            let param = args[1].to_lowercase();
            let items: Vec<Message> = match param.as_str() {
                "dir" => {
                    let dir_str = ctx.config.dir.to_string_lossy().to_string();

                    vec![
                        Message::new_bulk_string("dir".to_string()),
                        Message::new_bulk_string(dir_str),
                    ]
                }
                "dbfilename" => {
                    let filename_str = ctx
                        .config
                        .dbfilename
                        .as_ref()
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_default();
                    vec![
                        Message::new_bulk_string("dbfilename".to_string()),
                        Message::new_bulk_string(filename_str),
                    ]
                }
                "appendonly" => {
                    let appendonly = ctx.config.aof.appendonly;
                    let appendonly = if appendonly { "yes" } else { "no" };

                    vec![
                        Message::new_bulk_string("appendonly".to_string()),
                        Message::new_bulk_string(appendonly.to_string()),
                    ]
                }
                "appenddirname" => {
                    let appenddirname = ctx.config.aof.appenddirname.clone();

                    vec![
                        Message::new_bulk_string("appenddirname".to_string()),
                        Message::new_bulk_string(appenddirname.to_string()),
                    ]
                }
                "appendfilename" => {
                    let appendfilename = ctx.config.aof.appendfilename.clone();

                    vec![
                        Message::new_bulk_string("appendfilename".to_string()),
                        Message::new_bulk_string(appendfilename.to_string()),
                    ]
                }
                "appendfsync" => {
                    let appendfsync = ctx.config.aof.appendfsync.clone();

                    vec![
                        Message::new_bulk_string("appendfsync".to_string()),
                        Message::new_bulk_string(appendfsync.to_string()),
                    ]
                }
                _ => vec![],
            };

            Ok(Some(Message::new_array(items)))
        }
        _ => Ok(Some(Message::SimpleError(SimpleError {
            string: format!("ERR unknown subcommand '{}'", subcommand),
        }))),
    }
}

pub async fn keys(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'keys' command".to_string(),
        })));
    }

    ctx.with_db(|db| {
        let items: Vec<Message> = db
            .map
            .keys()
            .map(|k| Message::new_bulk_string(k.clone()))
            .collect();
        Ok(Some(Message::new_array(items)))
    })
    .await
}

pub async fn type_cmd(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'type' command".to_string(),
        })));
    }

    let key = &args[0];

    ctx.with_db(|db| {
        let type_str = if let Some(value) = db.map.get(key) {
            match value.value {
                ValueType::StringValue(_) => "string",
                ValueType::ListValue(_) => "list",
                ValueType::StreamValue(_) => "stream",
                ValueType::SortedSet(_) => "zset",
            }
        } else {
            "none"
        };
        Ok(Some(Message::new_simple_string(type_str)))
    })
    .await
}

pub async fn replconf(
    ctx: &CommandContext,
    args: &[String],
    message: &Message,
) -> Result<Option<Message>> {
    if args.is_empty() {
        return Ok(Some(Message::new_simple_string("OK")));
    }

    let subcommand = args[0].to_lowercase();

    // Master receives GETACK from client (tester) - forward to replicas
    if !ctx.is_slave && subcommand == "getack" {
        ctx.server.replicas.broadcast(message).await;
        return Ok(Some(Message::new_simple_string("OK")));
    }

    // Slave side: master sent GETACK, generate ACK response
    if ctx.is_slave && subcommand == "getack" {
        let mut repl = ctx.server.replication.write().await;
        let offset = repl.slave_offset - message.length()?;
        drop(repl);

        return Ok(Some(Message::new_array(vec![
            Message::new_bulk_string("REPLCONF".to_string()),
            Message::new_bulk_string("ACK".to_string()),
            Message::new_bulk_string(offset.to_string()),
        ])));
    }

    // Master side: replica sent ACK with its offset
    if !ctx.is_slave && subcommand == "ack" && args.len() >= 2 {
        if let Ok(replica_offset) = args[1].parse::<usize>() {
            let mut repl = ctx.server.replication.write().await;
            if replica_offset >= repl.expected_offset {
                repl.acked_replica_count += 1;
            }
        }
        return Ok(None); // Don't send response to ACK
    }

    Ok(Some(Message::new_simple_string("OK")))
}

pub async fn psync(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    // This will be handled specially in the connection handler
    // because it needs to send RDB file
    Ok(None)
}

pub async fn wait(
    ctx: &CommandContext,
    args: &[String],
    message: &Message,
) -> Result<Option<Message>> {
    if args.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "ERR wrong number of arguments for 'wait' command".to_string(),
        })));
    }

    let numreplicas: usize = args[0].parse()?;
    let timeout_ms: u64 = args[1].parse()?;

    {
        let repl = ctx.server.replication.read().await;
        // No writes to wait for, or no replicas connected
        if repl.expected_offset == 0 {
            let count = repl.total_replica_count;
            drop(repl);
            return Ok(Some(Message::new_integer(count as i64)));
        }
        // Already fully acked
        if repl.acked_replica_count >= repl.total_replica_count && repl.total_replica_count > 0 {
            let count = repl.total_replica_count;
            drop(repl);
            return Ok(Some(Message::new_integer(count as i64)));
        }
    }

    // Send GETACK to all replicas to trigger their offset reports
    let getack_msg = Message::new_array(vec![
        Message::new_bulk_string("REPLCONF".to_string()),
        Message::new_bulk_string("GETACK".to_string()),
        Message::new_bulk_string("*".to_string()),
    ]);
    ctx.server.replicas.broadcast(&getack_msg).await;

    let wait_notify = ctx.server.replication.read().await.wait_notify.clone();

    let start = Instant::now();
    let timeout_duration = Duration::from_millis(timeout_ms);

    loop {
        let repl = ctx.server.replication.read().await;
        if repl.acked_replica_count >= numreplicas {
            let count = repl.acked_replica_count;
            drop(repl);
            return Ok(Some(Message::new_integer(count as i64)));
        }
        drop(repl);

        let remaining = timeout_duration
            .checked_sub(start.elapsed())
            .unwrap_or(Duration::ZERO);

        if remaining.is_zero() {
            let repl = ctx.server.replication.read().await;
            return Ok(Some(Message::new_integer(repl.acked_replica_count as i64)));
        }

        if timeout(remaining, wait_notify.notified()).await.is_err() {
            let repl = ctx.server.replication.read().await;
            return Ok(Some(Message::new_integer(repl.acked_replica_count as i64)));
        }
    }
}

pub async fn command(ctx: &CommandContext, args: &[String]) -> Result<Option<Message>> {
    Ok(Some(Message::new_simple_string("OK")))
}
