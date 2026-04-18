#![allow(unused_imports)]
#![allow(unused)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use eyre::{Context, ContextCompat, Result};
use bytes::Bytes;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use tokio::fs::{read, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio_util::codec::Framed;

use codecrafters_redis::aof::AOF;
use codecrafters_redis::commands::{self, CommandContext, ServerConfig};
use codecrafters_redis::message::{Integer, Message, MessageFramer, SimpleError, SimpleString};
use codecrafters_redis::rdb::RDB;
use codecrafters_redis::server::{ReplicationState, Role, Server};

type MessageReader =
    Arc<Mutex<futures_util::stream::SplitStream<Framed<TcpStream, MessageFramer>>>>;
type MessageWriter =
    Arc<Mutex<futures_util::stream::SplitSink<Framed<TcpStream, MessageFramer>, Message>>>;

#[derive(Debug, Clone)]
struct Args {
    port: u16,
    dir: PathBuf,
    dbfilename: Option<PathBuf>,
    replicaof: Option<SocketAddr>,
    aof: Arc<AOF>,
}

#[derive(Debug, Parser)]
struct CliArgs {
    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    dir: Option<PathBuf>,

    #[arg(long)]
    dbfilename: Option<PathBuf>,

    #[arg(long, value_parser = parse_replicaof)]
    replicaof: Option<SocketAddr>,

    #[arg(long, value_parser = parse_appendonly, num_args = 1, default_value = "no")]
    appendonly: bool,

    #[arg(long, default_value = "appendonlydir")]
    appenddirname: String,

    #[arg(long, default_value = "appendonly.aof")]
    appendfilename: String,

    #[arg(long, default_value = "everysec")]
    appendfsync: String,
}

fn parse_replicaof(s: &str) -> Result<SocketAddr> {
    let parts: Vec<_> = s.split(' ').collect();
    if parts.len() != 2 {
        return Err(eyre::eyre!("Invalid replicaof format"));
    }
    let addr = format!("{}:{}", parts[0], parts[1]);
    // Try direct parse first
    if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
        return Ok(socket_addr);
    }
    // Resolve hostname - prefer IPv4 address
    let addrs: Vec<SocketAddr> = std::net::ToSocketAddrs::to_socket_addrs(&addr)?.collect();
    if let Some(ipv4) = addrs.iter().find(|a| a.is_ipv4()) {
        return Ok(*ipv4);
    }
    addrs
        .into_iter()
        .next()
        .context("No addresses found for hostname")
}

fn parse_appendonly(s: &str) -> Result<bool> {
    if s.eq_ignore_ascii_case("yes") {
        Ok(true)
    } else if s.eq_ignore_ascii_case("no") {
        Ok(false)
    } else {
        Err(eyre::eyre!("Invalid value: {s}"))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let cli_args = CliArgs::parse();

    let dir = cli_args
        .dir
        .unwrap_or_else(|| std::env::current_dir().unwrap());

    let aof = Arc::new(AOF {
        dir: dir.clone(),
        appendonly: cli_args.appendonly,
        appenddirname: cli_args.appenddirname,
        appendfilename: cli_args.appendfilename,
        appendfsync: cli_args.appendfsync,
    });

    let args = Arc::new(RwLock::new(Args {
        port: cli_args.port,
        dir: dir.clone(),
        dbfilename: cli_args.dbfilename,
        replicaof: cli_args.replicaof,
        aof: aof.clone(),
    }));

    // Load RDB if specified
    let mut rdb = RDB::new();
    {
        let args_read = args.read().await;
        if let Some(dbfilename) = &args_read.dbfilename {
            let fullpath = dir.join(dbfilename);

            if let Ok(rdb_content) = read(&fullpath).await {
                if let Ok(loaded_rdb) = RDB::from_bytes(&mut Bytes::from(rdb_content)) {
                    rdb = loaded_rdb;
                }
            }
        }
    }

    // AOF
    {
        let args_read = args.read().await;
        if args_read.aof.appendonly {
            let aof_dir = args_read.aof.dir.join(&args_read.aof.appenddirname);
            tokio::fs::create_dir_all(&aof_dir).await?;

            let aof_filename = format!("{}.1.incr.aof", &args_read.aof.appendfilename);
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(aof_dir.join(PathBuf::from(&aof_filename)))
                .await?;

            let manifest_path = args_read.aof.manifest_path();
            let mut manifest_file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(aof_dir.join(PathBuf::from(manifest_path)))
                .await?;
            manifest_file
                .write_all(format!("file {} seq 1 type i", aof_filename).as_bytes())
                .await;
        }
    }

    // Create replication state
    let repl_state = {
        let args_read = args.read().await;
        if let Some(master_addr) = args_read.replicaof {
            ReplicationState::new_slave(master_addr)
        } else {
            ReplicationState::new_master()
        }
    };

    let is_slave = repl_state.is_slave();
    let master_addr = if let Role::Slave(addr) = &repl_state.role {
        Some(*addr)
    } else {
        None
    };

    // Create server
    let server = Server::new(rdb, aof, repl_state);

    // If slave, connect to master
    if let Some(master_addr) = master_addr {
        let server_clone = server.clone();
        let args_clone = Arc::clone(&args);
        tokio::spawn(async move {
            if let Err(e) = connect_to_master(master_addr, server_clone, args_clone).await {
                eprintln!("Failed to connect to master: {}", e);
            }
        });
    }

    // Start server
    let port = args.read().await.port;
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;

    println!("Redis server started on port {}", port);

    while let Ok((stream, _)) = listener.accept().await {
        let server_clone = server.clone();
        let args_clone = Arc::clone(&args);
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, server_clone, args_clone, false).await {
                eprintln!("Client error: {}", e);
            }
        });
    }

    Ok(())
}

/// Helper function to send a response message to the client
async fn send_response(writer: &MessageWriter, message: Message) -> Result<()> {
    let mut writer_locked = writer.lock().await;
    writer_locked.send(message).await?;
    Ok(())
}

/// Helper function to send a simple error response
async fn send_error(writer: &MessageWriter, error: String) -> Result<()> {
    send_response(writer, Message::SimpleError(SimpleError { string: error })).await
}

/// Handle subscribe mode command filtering
async fn check_subscribe_mode(
    server: &Server,
    writer: &MessageWriter,
    peer_addr: &str,
    cmd: &str,
) -> Result<bool> {
    let in_subscribe_mode = server.pubsub.is_subscribed(peer_addr).await;
    if in_subscribe_mode {
        let allowed = [
            "subscribe",
            "unsubscribe",
            "psubscribe",
            "punsubscribe",
            "ping",
            "quit",
        ];
        if !allowed.contains(&cmd) {
            send_error(
                writer,
                format!("ERR Can't execute '{}' in subscribed mode", cmd),
            )
            .await?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Handle PSYNC command (replication)
async fn handle_psync(writer: &MessageWriter, server: &Server, peer_addr: &str) -> Result<()> {
    let repl = server.replication.read().await;
    let fullresync = format!(
        "FULLRESYNC {} {}",
        repl.master_replid, repl.master_repl_offset
    );
    drop(repl);

    send_response(
        writer,
        Message::SimpleString(SimpleString { string: fullresync }),
    )
    .await?;

    let rdb = server.rdb.read().await;
    send_response(writer, Message::RDB(rdb.clone())).await?;
    drop(rdb);

    // Register replica writer and update counts
    server
        .replicas
        .register(peer_addr.to_string(), Arc::clone(writer))
        .await;

    let mut repl = server.replication.write().await;
    repl.total_replica_count += 1;
    repl.acked_replica_count = 0;

    Ok(())
}

/// Handle SUBSCRIBE command
async fn handle_subscribe(
    writer: &MessageWriter,
    server: &Server,
    peer_addr: &str,
    channel: &str,
) -> Result<()> {
    server
        .pubsub
        .subscribe(
            peer_addr.to_string(),
            channel.to_string(),
            Arc::clone(writer),
        )
        .await;

    let channels = server.pubsub.get_client_channels(peer_addr).await;
    send_response(
        writer,
        Message::new_array(vec![
            Message::new_bulk_string("subscribe".to_string()),
            Message::new_bulk_string(channel.to_string()),
            Message::Integer(Integer {
                value: channels.len() as i64,
            }),
        ]),
    )
    .await
}

async fn handle_client(
    stream: TcpStream,
    server: Server,
    args: Arc<RwLock<Args>>,
    is_slave: bool,
) -> Result<()> {
    handle_client_with_framed(Framed::new(stream, MessageFramer), server, args, is_slave).await
}

async fn handle_client_with_framed(
    framed: Framed<TcpStream, MessageFramer>,
    server: Server,
    args: Arc<RwLock<Args>>,
    is_slave: bool,
) -> Result<()> {
    let peer_addr = framed.get_ref().peer_addr()?.to_string();
    let (writer, reader) = framed.split();
    let writer = Arc::new(Mutex::new(writer));
    let reader = Arc::new(Mutex::new(reader));

    // Read config once at connection setup
    let config = {
        let args_read = args.read().await;
        Arc::new(ServerConfig {
            dir: args_read.dir.clone(),
            dbfilename: args_read.dbfilename.clone(),
            aof: args_read.aof.clone(),
        })
    };

    let selected_db = Arc::new(RwLock::new(0_usize));

    let default_user = server
        .users
        .get("default")
        .expect("Must have default user")
        .value()
        .clone();

    let current_user = if default_user.passwords.is_empty() {
        Some(default_user)
    } else {
        None
    };

    let mut ctx = CommandContext {
        server: server.clone(),
        client_id: peer_addr.clone(),
        selected_db: Arc::clone(&selected_db),
        is_slave,
        config: Arc::clone(&config),
        current_user,
        in_transaction: false,
        transaction_queue: VecDeque::new(),
        is_dirty: Arc::new(AtomicBool::new(false)),
    };

    loop {
        let message = {
            let mut reader_locked = reader.lock().await;
            reader_locked.next().await
        };

        let message = match message {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => {
                eprintln!("Read error: {}", e);
                break;
            }
            None => {
                // Client disconnected
                server.pubsub.disconnect_client(&peer_addr).await;
                break;
            }
        };

        // Update slave offset if this is a slave connection
        if is_slave {
            let mut repl = server.replication.write().await;
            repl.slave_offset += message.length()?;
        }

        // Parse command
        let (cmd, args_vec) = match commands::parse_command(&message) {
            Some(parsed) => parsed,
            None => continue,
        };

        if matches!(ctx.current_user, None) {
            let allow_list = ["auth"];
            if !allow_list.contains(&cmd.to_lowercase().as_str()) {
                send_error(&writer, "NOAUTH Authentication required.".to_string()).await;
                continue;
            }
        }

        // Check if in subscribe mode
        if check_subscribe_mode(&server, &writer, &peer_addr, &cmd).await? {
            continue;
        }

        if ctx.in_transaction && cmd.as_str() == "watch" {
            send_error(&writer, "ERR WATCH inside MULTI is not allowed".to_string()).await?;
        }

        // If in transaction and not MULTI/EXEC/DISCARD, queue the command
        if ctx.in_transaction && !matches!(cmd.as_str(), "multi" | "exec" | "discard") {
            ctx.transaction_queue.push_back(message.clone());
            send_response(&writer, Message::new_simple_string("QUEUED")).await?;
            continue;
        }

        // Special handling for PSYNC (replication)
        if cmd == "psync" {
            handle_psync(&writer, &server, &peer_addr).await?;
            continue;
        }

        // Special handling for SUBSCRIBE
        if cmd == "subscribe" && !args_vec.is_empty() {
            let channel = &args_vec[0];
            handle_subscribe(&writer, &server, &peer_addr, channel).await?;
            continue;
        }

        // Execute command
        match commands::execute(&mut ctx, &cmd, &args_vec, &message).await {
            Ok(Some(response)) => {
                send_response(&writer, response).await?;
            }
            Ok(None) => {
                // No response needed (e.g., slave processing SET)
            }
            Err(e) => {
                eprintln!("Command error: {}", e);
                send_error(&writer, format!("{}", e)).await?;
            }
        }
    }

    Ok(())
}

async fn connect_to_master(
    master_addr: SocketAddr,
    server: Server,
    args: Arc<RwLock<Args>>,
) -> Result<()> {
    let stream = TcpStream::connect(master_addr).await?;
    let framed = Framed::new(stream, MessageFramer);
    let (mut writer, mut reader) = framed.split();

    // Send PING
    writer
        .send(Message::new_array(vec![Message::new_bulk_string(
            "PING".to_string(),
        )]))
        .await?;
    reader.next().await;

    // Send REPLCONF listening-port
    let port = args.read().await.port;
    writer
        .send(Message::new_array(vec![
            Message::new_bulk_string("REPLCONF".to_string()),
            Message::new_bulk_string("listening-port".to_string()),
            Message::new_bulk_string(port.to_string()),
        ]))
        .await?;
    reader.next().await;

    // Send REPLCONF capa psync2
    writer
        .send(Message::new_array(vec![
            Message::new_bulk_string("REPLCONF".to_string()),
            Message::new_bulk_string("capa".to_string()),
            Message::new_bulk_string("psync2".to_string()),
        ]))
        .await?;
    reader.next().await;

    // Send PSYNC
    writer
        .send(Message::new_array(vec![
            Message::new_bulk_string("PSYNC".to_string()),
            Message::new_bulk_string("?".to_string()),
            Message::new_bulk_string("-1".to_string()),
        ]))
        .await?;
    reader.next().await; // FULLRESYNC response
    reader.next().await; // RDB file

    // Continue listening to master's commands
    let framed = reader.reunite(writer)?;
    handle_client_with_framed(framed, server, args, true).await?;

    Ok(())
}
