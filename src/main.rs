#![allow(unused_imports)]
#![allow(unused)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use anyhow::Context;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::Parser;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use codecrafters_redis::message::{
    Array, BulkString, Integer, Message, MessageFramer, SimpleError, SimpleString,
};
use codecrafters_redis::rdb::zset::ZSet;
use codecrafters_redis::rdb::{
    Database, ListValue, SortedSet, StreamId, StreamValue, StringValue, Value, ValueType, RDB,
};
use codecrafters_redis::utils;
use tokio::fs::read;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::sync::{Notify, RwLockWriteGuard};
use tokio::time::{timeout, Instant};
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;
use tokio_util::codec::Framed;

type MessageReader = Arc<Mutex<SplitStream<Framed<TcpStream, MessageFramer>>>>;
type MessageWriter = Arc<Mutex<SplitSink<Framed<TcpStream, MessageFramer>, Message>>>;

static REGISTER_MAP: LazyLock<Mutex<HashMap<String, (Instant, Message)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// channel_name => client_ip
static SUBSCRIBE_MAP: LazyLock<Mutex<HashMap<String, HashMap<String, MessageWriter>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// client_ip => channel set
static SUBSCRIBE_CLIENT_MAP: LazyLock<Mutex<HashMap<String, HashSet<String>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    #[clap(default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    dir: Option<PathBuf>,

    #[arg(long)]
    dbfilename: Option<PathBuf>,

    #[arg(long, value_parser = parse_replicaof)]
    replicaof: Option<SocketAddr>,
}

fn parse_replicaof(s: &str) -> anyhow::Result<SocketAddr> {
    let parts: Vec<_> = s.split(" ").collect();
    if parts.len() != 2 {
        return Err(anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Invalid replicaof.",
        )));
    }

    let addr = parts[0];
    let port = parts[1];

    let full_addrss = format!("{addr}:{port}");

    let socket_addr = full_addrss
        .to_socket_addrs()?
        .next()
        .context("Unable to resolve address")?;

    Ok(socket_addr)
}

fn get_rdb() -> &'static RwLock<RDB> {
    static _RDB: OnceLock<RwLock<RDB>> = OnceLock::new();

    _RDB.get_or_init(|| RwLock::new(RDB::new()))
}

#[derive(Debug, Clone)]
enum Role {
    Master,
    Slave(SocketAddr),
}

#[derive(Debug, Clone)]
struct Info {
    role: Role,
    master_replid: String,
    master_repl_offset: isize,
}

#[derive(Debug, Clone)]
struct State {
    // slave states
    offset: usize,
    // master states
    total_replica_count: usize,
    acked_replica_count: usize,
    expected_offset: usize,
    wait_notify: Arc<Notify>,
}

macro_rules! get_param {
    ($index: expr, $params: expr,$cmd_name: expr) => {{
        let Some(value) = $params.get($index) else {
            return Ok(Some(Message::SimpleError(SimpleError {
                string: format!("{} must have 1 argument.", $cmd_name),
            })));
        };

        value
    }};
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let role = if let Some(replicaof) = args.replicaof {
        Role::Slave(replicaof)
    } else {
        Role::Master
    };

    let info = Info {
        role,
        master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        master_repl_offset: 0,
    };
    let info = Arc::new(info);

    let state = State {
        offset: 0,
        total_replica_count: 0,
        acked_replica_count: 0,
        expected_offset: 0,
        wait_notify: Arc::new(Notify::new()),
    };
    let state = Arc::new(Mutex::new(state));

    // println!("{args:?}");
    if let Some(dbfilename) = args.dbfilename.as_ref() {
        let fullpath = if let Some(dir) = args.dir.as_ref() {
            &dir.join(dbfilename)
        } else {
            dbfilename
        };
        let rdb_content = read(fullpath).await;
        if let Ok(rdb_content) = rdb_content {
            let read_rdb = RDB::from_bytes(&mut Bytes::from(rdb_content))?;
            let mut rdb = get_rdb().write().await;
            *rdb = read_rdb;
        }
    }

    let port = args.port;

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let (tx, mut rx) = broadcast::channel(100);

    tokio::spawn(async move {
        loop {
            match rx.recv().await.unwrap() {
                Message::Array(Array { items }) => {
                    let peer_addr = match &items[0] {
                        Message::BulkString(BulkString {
                            string: peer_addr, ..
                        }) => peer_addr,
                        _ => unreachable!(),
                    };

                    let msg = &items[1];
                    if let Message::Array(Array { items }) = msg {
                        match &items[0] {
                            Message::BulkString(BulkString { string: cmd, .. }) => {
                                match cmd.to_lowercase().as_str() {
                                    "blpop" => {
                                        REGISTER_MAP.lock().await.insert(
                                            peer_addr.to_string(),
                                            (Instant::now(), msg.clone()),
                                        );
                                    }

                                    // "subscribe" => {
                                    //     let mut subscribe_map = SUBSCRIBE_MAP.lock().await;

                                    //     let Message::BulkString(BulkString {
                                    //         string: chan_name,
                                    //         ..
                                    //     }) = items.get(1).expect("channel name must exist")
                                    //     else {
                                    //         unreachable!("channel name must exist");
                                    //     };

                                    //     let client_entry =
                                    //         subscribe_map.entry(chan_name.to_string()).or_default();
                                    //     client_entry
                                    //         .entry(peer_addr.to_string())
                                    //         .or_insert_with(|| Instant::now());
                                    // }
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    });

    let args = Arc::new(RwLock::new(args));

    if let Role::Slave(addr) = &info.role {
        let stream: TcpStream = TcpStream::connect(addr).await?;

        handle_connection_to_master(
            stream,
            Arc::clone(&args),
            Arc::clone(&info),
            Arc::clone(&state),
            tx.clone(),
        )
        .await?;
    }

    let listener = TcpListener::bind(("127.0.0.1", port)).await?;

    while let Ok((stream, _)) = listener.accept().await {
        let socket: Framed<TcpStream, MessageFramer> = Framed::new(stream, MessageFramer);
        let args = Arc::clone(&args);
        let info = Arc::clone(&info);
        let tx = tx.clone();
        let state = Arc::clone(&state);

        // this is for listening to messages from client
        tokio::spawn(async move {
            if let Err(e) = handler(socket, args, info, state, tx, false).await {
                println!("Err: {}", e);
            }
        });
    }

    Ok(())
}

async fn handler(
    mut socket: Framed<TcpStream, MessageFramer>,
    args: Arc<RwLock<Args>>,
    info: Arc<Info>,
    state: Arc<Mutex<State>>,
    tx: Sender<Message>,
    is_slave: bool,
) -> anyhow::Result<()> {
    let peer_addr = socket.get_ref().peer_addr()?.to_string();

    let (message_writer, message_reader) = socket.split();
    let message_writer = Arc::new(Mutex::new(message_writer));
    let message_reader = Arc::new(Mutex::new(message_reader));

    let selected_db_index = Arc::new(RwLock::new(0 as usize));
    let mut transaction_queue = VecDeque::new();

    loop {
        let message = {
            let mut message_reader = message_reader.lock().await;
            message_reader.next().await
        };

        if message.is_none() {
            // client disconnected, clear map
            let (mut subscribe_client_map, mut subscribe_map) =
                tokio::join!(SUBSCRIBE_CLIENT_MAP.lock(), SUBSCRIBE_MAP.lock());

            if let Some(set) = subscribe_client_map.remove(&peer_addr) {
                for chan_name in set {
                    // subscribe_map.entry(chan_name).and_modify(|map| {
                    //     map.remove(&peer_addr);
                    // });

                    if let Some(map) = subscribe_map.get_mut(&chan_name) {
                        map.remove(&peer_addr);
                        if map.is_empty() {
                            subscribe_map.remove(&chan_name);
                        }
                    }
                }
            }

            let mut register_map = REGISTER_MAP.lock().await;
            register_map.remove(&peer_addr);

            break Ok(());
        }
        let message = message.unwrap()?;

        let commands = parse_message(&message);

        println!("{message:?}");

        match commands[..] {
            [cmd, ref params @ ..] => {
                let message_length = message.length()?;

                if is_slave {
                    // messages must be from master
                    let mut state = state.lock().await;

                    state.offset += message_length;
                }

                match cmd.to_ascii_lowercase().as_str() {
                    "command" => {
                        let mut message_writer = message_writer.lock().await;
                        message_writer
                            .send(Message::SimpleString(SimpleString {
                                string: "Not supported yet.".to_string(),
                            }))
                            .await?
                    }

                    "multi" => {
                        // use MULTI message as a signal that it's in a transaction
                        transaction_queue.push_back(message);
                        let mut message_writer = message_writer.lock().await;
                        message_writer
                            .send(Message::SimpleString(SimpleString {
                                string: "OK".to_string(),
                            }))
                            .await?;
                        continue;
                    }
                    "exec" => {
                        // clear queue
                        let mut resp_message_list = vec![];

                        if transaction_queue.len() == 0 {
                            let mut message_writer = message_writer.lock().await;
                            message_writer
                                .send(Message::SimpleError(SimpleError {
                                    string: "ERR EXEC without MULTI".to_string(),
                                }))
                                .await?;

                            continue;
                        }

                        while let Some(message) = transaction_queue.pop_front() {
                            let commands = parse_message(&message);
                            if commands[0] != "multi" && commands[0] != "exec" {
                                match handle_message(
                                    Arc::clone(&message_reader),
                                    Arc::clone(&message_writer),
                                    peer_addr.clone(),
                                    message,
                                    &selected_db_index,
                                    // &mut socket,
                                    &args,
                                    &info,
                                    &state,
                                    &tx,
                                    is_slave,
                                )
                                .await
                                {
                                    Ok(Some(resp_message)) => {
                                        resp_message_list.push(resp_message);
                                    }
                                    _ => {}
                                }
                            }
                        }

                        let mut message_writer = message_writer.lock().await;
                        message_writer
                            .send(Message::Array(Array {
                                items: resp_message_list,
                            }))
                            .await?;

                        continue;
                    }

                    "discard" => {
                        if transaction_queue.len() == 0 {
                            let mut message_writer = message_writer.lock().await;
                            message_writer
                                .send(Message::SimpleError(SimpleError {
                                    string: "ERR DISCARD without MULTI".to_string(),
                                }))
                                .await?;

                            continue;
                        }

                        transaction_queue.clear();
                        let mut message_writer = message_writer.lock().await;
                        message_writer
                            .send(Message::SimpleString(SimpleString {
                                string: "OK".to_string(),
                            }))
                            .await?;
                        continue;
                    }

                    "psync" => {
                        let fullresync = format!(
                            "FULLRESYNC {} {}",
                            info.master_replid, info.master_repl_offset
                        );
                        let mut message_writer = message_writer.lock().await;
                        message_writer
                            .send(Message::SimpleString(SimpleString { string: fullresync }))
                            .await?;

                        let rdb = get_rdb().read().await;

                        message_writer.send(Message::RDB(rdb.clone())).await?;
                        drop(rdb);

                        {
                            let mut state = state.lock().await;
                            state.total_replica_count += 1;
                            state.acked_replica_count += 1;
                        }

                        let mut rx = tx.subscribe();
                        while let Ok(ref message) = rx.recv().await {
                            // println!("[message to propagate]: {message:?}");

                            let is_get_ack = match &message {
                                Message::Array(Array { items }) => match &items[..] {
                                    [Message::BulkString(BulkString {
                                        length: _,
                                        string: replconf,
                                    }), Message::BulkString(BulkString {
                                        length: _,
                                        string: getack,
                                    }), Message::BulkString(BulkString {
                                        length: _,
                                        string: star,
                                    }), ..]
                                        if replconf.as_str().to_lowercase() == "replconf"
                                            && getack.as_str().to_lowercase() == "getack"
                                            && star.as_str() == "*" =>
                                    {
                                        true
                                    }
                                    _ => false,
                                },
                                _ => false,
                            };

                            message_writer.send(message.clone()).await?;

                            let mut message_reader = message_reader.lock().await;

                            if is_get_ack {
                                // let resp = socket.next().await;
                                let resp = match timeout(
                                    Duration::from_millis(500),
                                    message_reader.next(),
                                )
                                .await
                                {
                                    Ok(Some(Ok(message))) => message,
                                    Ok(Some(Err(e))) => {
                                        println!("Error while reading for ACK: {}", e);
                                        continue;
                                    }
                                    Ok(None) => {
                                        println!("Socket closed by slave.");
                                        let mut state = state.lock().await;
                                        state.total_replica_count -= 1;
                                        break;
                                    }
                                    Err(_) => {
                                        println!("Timed out waiting for ACK from slave.");
                                        continue;
                                    }
                                };

                                let actual_offset: usize = match &resp {
                                    Message::Array(Array { items }) => match &items[..] {
                                        [Message::BulkString(BulkString {
                                            length: _,
                                            string: replconf,
                                        }), Message::BulkString(BulkString {
                                            length: _,
                                            string: ack,
                                        }), Message::BulkString(BulkString {
                                            length: _,
                                            string: offset,
                                        })] if replconf.as_str().to_lowercase() == "replconf"
                                            && ack.as_str().to_lowercase() == "ack" =>
                                        {
                                            offset.parse().context("Invalid offset from ACK.")?
                                        }
                                        _ => continue,
                                    },
                                    _ => continue,
                                };

                                let expected_offset = {
                                    let state = state.lock().await;
                                    state.expected_offset
                                };

                                // println!("actual: {actual_offset}, get_ack_msg_length: {},expected: {expected_offset}",message.length()?);

                                if actual_offset + message.length()? == expected_offset {
                                    let mut state: tokio::sync::MutexGuard<'_, State> =
                                        state.lock().await;
                                    state.acked_replica_count += 1;

                                    // println!("ACKED, count: {}", state.acked_replica_count);
                                    state.wait_notify.notify_waiters();
                                }

                                // println!("ACK resp: {resp:?}");
                            }
                        }
                    }

                    _ => {}
                }
            }
            _ => {}
        }

        if transaction_queue.len() > 0 {
            transaction_queue.push_back(message);
            let mut message_writer = message_writer.lock().await;
            message_writer
                .send(Message::SimpleString(SimpleString {
                    string: "QUEUED".to_string(),
                }))
                .await?;
            continue;
        }

        let cmd = if commands.len() > 0 {
            commands[0].to_lowercase()
        } else {
            "".to_string()
        };

        let message_to_send = if ["set", "xadd"].contains(&cmd.as_str()) {
            Some(message.clone())
        } else {
            None
        };

        let registered_message_to_send = if ["lpush", "rpush"].contains(&cmd.as_str()) {
            let now = Instant::now();
            let mut max = Duration::from_nanos(0);

            let mut target = None;

            let mut register_map = REGISTER_MAP.lock().await;

            for (k, (ts, msg)) in register_map.iter() {
                if let Message::Array(Array { items }) = msg {
                    if let Message::BulkString(BulkString { string: cmd, .. }) = &items[0] {
                        if cmd.to_lowercase() == "blpop" {
                            let duration = now - *ts;
                            if duration > max {
                                max = duration;
                                target = Some((k, msg));
                            }
                        }
                    }
                }
            }

            let key_to_remove = target.as_ref().map(|(k, _)| k.to_string());

            let ret = if let Some((k, msg)) = target {
                Some(Message::new_array(vec![
                    Message::new_bulk_string(k.to_string()),
                    msg.clone(),
                ]))
            } else {
                None
            };

            if let Some(key_to_remove) = key_to_remove {
                register_map.remove(&key_to_remove);
            }

            ret
        } else {
            None
        };

        println!("handle message last: {message:?}");
        match handle_message(
            Arc::clone(&message_reader),
            Arc::clone(&message_writer),
            peer_addr.clone(),
            message,
            &selected_db_index,
            // &mut socket,
            &args,
            &info,
            &state,
            &tx,
            is_slave,
        )
        .await
        {
            Ok(Some(resp_message)) => {
                let mut message_writer = message_writer.lock().await;
                message_writer.send(resp_message).await?;
                if let Some(message) = message_to_send {
                    tx.send(message)?;
                }
                if let Some(msg) = registered_message_to_send {
                    tx.send(msg)?;
                }
            }
            Ok(None) => {}
            Err(_) => {}
        };
    }
}

fn parse_message(message: &Message) -> Vec<&str> {
    let commands = match &message {
        Message::Array(Array { items }) => items
            .into_iter()
            .map(|item| match item {
                Message::BulkString(BulkString { length: _, string }) => string,
                _ => "",
            })
            .collect(),
        _ => vec![],
    };

    commands
}

async fn handle_message(
    message_reader: MessageReader,
    message_writer: MessageWriter,
    peer_addr: String,
    message: Message,
    selected_db_index: &Arc<RwLock<usize>>,
    // socket: &mut Framed<TcpStream, MessageFramer>,
    args: &Arc<RwLock<Args>>,
    info: &Arc<Info>,
    state: &Arc<Mutex<State>>,
    tx: &Sender<Message>,
    is_slave: bool,
) -> anyhow::Result<Option<Message>> {
    let commands = parse_message(&message);

    async fn with_db<F, R>(selected_db_index: &Arc<RwLock<usize>>, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut Database) -> R,
    {
        let mut rdb = get_rdb().write().await;
        let index = selected_db_index.read().await;
        let db = rdb.get_db_mut(*index)?;

        Ok(f(db))
    }

    match commands[..] {
        [cmd, ref params @ ..] => {
            let message_length = message.length()?;

            let in_subscribe_mode = {
                let subscribe_client_map = SUBSCRIBE_CLIENT_MAP.lock().await;
                subscribe_client_map.contains_key(&peer_addr)
            };

            let cmd_lower_case = cmd.to_ascii_lowercase();
            if in_subscribe_mode {
                static ALLOWED: [&str; 6] = [
                    "subscribe",
                    "unsubscribe",
                    "psubscribe",
                    "punsubscribe",
                    "ping",
                    "quit",
                ];

                if !ALLOWED.contains(&cmd_lower_case.as_str()) {
                    return Ok(Some(Message::SimpleError(SimpleError {
                        string: format!("ERR Can't execute '{cmd_lower_case}' in subscribed mode"),
                    })));
                }
            }

            // if is_slave {
            //     // messages must be from master
            //     let mut state = state.lock().await;

            //     state.offset += message_length;
            // }

            match cmd_lower_case.as_str() {
                "ping" => {
                    if is_slave {
                        return Ok(None);
                    }
                    if in_subscribe_mode {
                        return Ok(Some(Message::new_array(vec![
                            Message::new_bulk_string("pong".to_string()),
                            Message::new_bulk_string("".to_string()),
                        ])));
                    } else {
                        return Ok(Some(Message::SimpleString(SimpleString {
                            string: "PONG".to_string(),
                        })));
                    }
                }

                "echo" => {
                    if params.len() == 0 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "ECHO must have an argument.".to_string(),
                        })));
                    }

                    let echo_string = params[0];
                    return Ok(Some(Message::BulkString(BulkString {
                        length: echo_string.len() as isize,
                        string: echo_string.to_string(),
                    })));
                }

                "set" => {
                    if params.len() < 2 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "SET must have at least 2 arguments.".to_string(),
                        })));
                    }

                    let k = params[0];
                    let v = params[1];

                    let mut rdb = get_rdb().write().await;
                    let index = Arc::clone(&selected_db_index);
                    let index = index.read().await;
                    let db = rdb
                        .get_db_mut(*index)
                        .context(format!("DB {} does not exist.", *index))?;

                    let expiry = if params.len() > 2 {
                        let extra_cmd = &params[2];
                        let extra_param = &params[3];

                        match extra_cmd.to_lowercase().as_ref() {
                            "px" => {
                                let timeout =
                                    extra_param.parse::<u64>().context("Invalid timeout.")?;
                                let current_ts = chrono::Utc::now().timestamp_millis() as u64;
                                Some(current_ts + timeout)
                            }
                            _ => {
                                todo!();
                            }
                        }
                    } else {
                        None
                    };

                    db.map.insert(
                        k.to_string(),
                        Value {
                            expiry,
                            value: ValueType::StringValue(StringValue {
                                string: v.to_string(),
                            }),
                        },
                    );
                    drop(rdb);

                    let resp_message = if !is_slave {
                        Some(Message::SimpleString(SimpleString {
                            string: "OK".to_string(),
                        }))
                    } else {
                        None
                    };

                    // sync to all slaves
                    // println!("tx send: {message:?}");
                    {
                        let mut state = state.lock().await;
                        state.acked_replica_count = 0;
                    }

                    {
                        let mut state = state.lock().await;
                        state.expected_offset += message_length;
                    }
                    // tx.send(message.clone())?;

                    return Ok(resp_message);
                }

                "get" => {
                    if params.len() < 1 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "GET must have 1 argument.".to_string(),
                        })));
                    }
                    let k = params[0].to_string();

                    let rdb = get_rdb().read().await;
                    let index = Arc::clone(&selected_db_index);
                    let index = index.read().await;
                    let db = rdb.get_db(*index)?;
                    let v = db.map.get(&k);

                    match v {
                        Some(v) if !v.is_expired() => match &v.value {
                            ValueType::StringValue(StringValue { string }) => {
                                Ok(Some(Message::BulkString(BulkString {
                                    length: string.len() as isize,
                                    string: string.to_string(),
                                })))
                            }
                            _=>{
                                Ok(Some(Message::SimpleError(SimpleError { string: "WRONGTYPE Operation against a key holding the wrong kind of value".to_string() })))
                            }
                        },
                        _ => Ok(Some(Message::BulkString(BulkString {
                            length: -1,
                            string: "".to_string(),
                        }))),
                    }
                }

                "config" => {
                    if params.len() < 1 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "CONFIG must have a subcommand.".to_string(),
                        })));
                    }
                    let config_cmd = params[0];
                    match config_cmd.to_lowercase().as_str() {
                        "get" => {
                            let config_keys = &params[1..];

                            let mut config_entries = vec![];

                            for &config_key in config_keys {
                                match config_key {
                                    "dir" => {
                                        config_entries
                                            .push((config_key, args.read().await.dir.clone()));
                                    }
                                    "dbfilename" => {
                                        config_entries.push((
                                            config_key,
                                            args.read().await.dbfilename.clone(),
                                        ));
                                    }
                                    _ => {}
                                }
                            }

                            let items = config_entries
                                .into_iter()
                                .filter_map(|(k, v)| {
                                    let v = v?.to_string_lossy().to_string();
                                    let k = Message::BulkString(BulkString {
                                        length: k.len() as isize,
                                        string: k.to_string(),
                                    });

                                    let v = Message::BulkString(BulkString {
                                        length: v.len() as isize,
                                        string: v,
                                    });

                                    Some([k, v])
                                })
                                .flatten()
                                .collect();

                            Ok(Some(Message::Array(Array { items })))
                        }
                        _ => Ok(None),
                    }
                }

                "keys" => {
                    if params.len() < 1 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "KEYS must have at least 1 argument.".to_string(),
                        })));
                    }

                    let keys_pattern = params[0];
                    let index = Arc::clone(&selected_db_index);
                    let index = index.read().await;

                    let rdb = get_rdb().read().await;
                    let all_keys = rdb.get_db(*index)?.map.keys();

                    Ok(Some(Message::Array(Array {
                        items: all_keys
                            .map(|k| {
                                Message::BulkString(BulkString {
                                    length: k.len() as isize,
                                    string: k.to_string(),
                                })
                            })
                            .collect(),
                    })))
                }

                "info" => {
                    let mut info_list = vec!["# Replication".to_string()];
                    info_list.push(format!(
                        "role:{}",
                        if let Role::Slave(_) = info.role {
                            "slave"
                        } else {
                            "master"
                        }
                    ));
                    info_list.push(format!("master_replid:{}", info.master_replid));
                    info_list.push(format!("master_repl_offset:{}", info.master_repl_offset));

                    let info_string = info_list.join("\r\n");

                    Ok(Some(Message::BulkString(BulkString {
                        length: info_string.len() as isize,
                        string: info_string.to_string(),
                    })))
                }

                "replconf" => {
                    if params.len() > 0 {
                        let rest = &params[0..];
                        if is_slave {
                            if rest[0].to_lowercase() == "getack" {
                                let offset = {
                                    let state = state.lock().await;
                                    (state.offset - message_length).to_string()
                                };

                                return Ok(Some(Message::Array(Array {
                                    items: vec![
                                        Message::BulkString(BulkString {
                                            length: 8,
                                            string: "REPLCONF".to_string(),
                                        }),
                                        Message::BulkString(BulkString {
                                            length: 3,
                                            string: "ACK".to_string(),
                                        }),
                                        Message::BulkString(BulkString {
                                            length: offset.len() as isize,
                                            string: offset,
                                        }),
                                    ],
                                })));
                            }
                        }
                    }

                    Ok(Some(Message::SimpleString(SimpleString {
                        string: "OK".to_string(),
                    })))
                }

                "wait" => {
                    if params.len() < 2 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "WAIT must have at least 2 arguments.".to_string(),
                        })));
                    }

                    let repl = "REPLCONF";
                    let get_ack = "GETACK";
                    let get_ack_msg = Message::Array(Array {
                        items: vec![
                            Message::BulkString(BulkString {
                                length: repl.len() as isize,
                                string: repl.to_string(),
                            }),
                            Message::BulkString(BulkString {
                                length: get_ack.len() as isize,
                                string: get_ack.to_string(),
                            }),
                            Message::BulkString(BulkString {
                                length: 1,
                                string: "*".to_string(),
                            }),
                        ],
                    });

                    {
                        let mut state = state.lock().await;
                        state.expected_offset += get_ack_msg.length()?;
                    }
                    tx.send(get_ack_msg)?;

                    let numreplicas: usize = params[0].parse().context("Invalid numreplicas.")?;
                    let timeout_value = params[1];

                    let timeout_duration =
                        Duration::from_millis(timeout_value.parse().context("Invalid timeout.")?);

                    let start_time = Instant::now();

                    let value;

                    loop {
                        {
                            let acked_count = {
                                let state = state.lock().await;
                                state.acked_replica_count
                            };

                            if acked_count >= numreplicas {
                                value = acked_count;
                                break;
                            }
                        }

                        let remaining_time = timeout_duration
                            .checked_sub(start_time.elapsed())
                            .unwrap_or_else(|| Duration::from_millis(0));

                        if remaining_time.is_zero() {
                            let state = state.lock().await;
                            value = state.acked_replica_count;
                            break;
                        }

                        let wait_notify = {
                            let state = state.lock().await;
                            state.wait_notify.clone()
                        };

                        if timeout(remaining_time, wait_notify.notified())
                            .await
                            .is_err()
                        {
                            let state = state.lock().await;
                            value = state.acked_replica_count;
                            break;
                        }
                    }

                    Ok(Some(Message::Integer(Integer {
                        value: value as i64,
                    })))
                }

                "type" => {
                    if params.len() < 1 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "TYPE must have 1 arguments.".to_string(),
                        })));
                    }

                    let k = params[0];

                    let rdb = get_rdb().read().await;
                    let index = selected_db_index.read().await;
                    let db = rdb.get_db(*index)?;

                    let type_str = if let Some(v) = db.map.get(k) {
                        match v.value {
                            ValueType::StreamValue(_) => "stream".to_string(),
                            ValueType::StringValue(_) => "string".to_string(),
                            ValueType::ListValue(_) => "list".to_string(),
                            ValueType::SortedSet(_) => "zset".to_string(),
                        }
                    } else {
                        "none".to_string()
                    };

                    Ok(Some(Message::SimpleString(SimpleString {
                        string: type_str,
                    })))
                }

                "xadd" => {
                    if params.len() < 2 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "XADD: Not enough params.".to_string(),
                        })));
                    }

                    let stream_key = params[0];

                    let mut rdb = get_rdb().write().await;
                    let index = selected_db_index.read().await;
                    let db = rdb.get_db_mut(*index)?;

                    let id = params[1];

                    let id = if id == "*" {
                        match db.generate_stream_id(stream_key, None) {
                            Ok(id) => id,
                            Err(err) => {
                                return Ok(Some(Message::SimpleError(SimpleError {
                                    string: err.to_string(),
                                })));
                            }
                        }
                    } else {
                        let splits: Vec<_> = id.split("-").collect();
                        let ts: i64 = splits[0].parse().unwrap();
                        let seq = splits[1];
                        if seq == "*" {
                            match db.generate_stream_id(stream_key, Some(ts)) {
                                Ok(id) => id,
                                Err(err) => {
                                    return Ok(Some(Message::SimpleError(SimpleError {
                                        string: err.to_string(),
                                    })));
                                }
                            }
                        } else {
                            let seq: usize = seq.parse().unwrap();
                            StreamId { ts, seq }
                        }
                    };

                    if id <= (StreamId { ts: 0, seq: 0 }) {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "ERR The ID specified in XADD must be greater than 0-0"
                                .to_string(),
                        })));
                    }

                    let v = db.map.get_mut(stream_key);

                    let create_stream_data = || {
                        let id = id.clone();
                        params[2..].chunks_exact(2).fold(
                            BTreeMap::from([("id".to_string(), id.into())]),
                            |mut acc, chunk| {
                                acc.insert(chunk[0].to_string(), chunk[1].to_string());
                                acc
                            },
                        )
                    };

                    match v {
                        Some(value) => {
                            if value.is_expired() {
                                db.map.remove(stream_key);

                                let stream_data = create_stream_data();

                                let value = Value {
                                    expiry: None,
                                    value: ValueType::StreamValue(StreamValue {
                                        stream: vec![stream_data],
                                    }),
                                };

                                db.map.insert(stream_key.to_string(), value);
                            } else {
                                match &mut value.value {
                                    ValueType::StreamValue(StreamValue { stream }) => {
                                        let last = stream.last().unwrap();
                                        let last_id: StreamId =
                                            last.get("id").unwrap().parse().unwrap();
                                        if id <= last_id {
                                            return Ok(Some(Message::SimpleError(SimpleError {   string: "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()})));
                                        } else {
                                            let stream_data = params[2..].chunks_exact(2).fold(
                                                BTreeMap::from([(
                                                    "id".to_string(),
                                                    id.clone().into(),
                                                )]),
                                                |mut acc, chunk| {
                                                    acc.insert(
                                                        chunk[0].to_string(),
                                                        chunk[1].to_string(),
                                                    );
                                                    acc
                                                },
                                            );

                                            stream.push(stream_data);
                                        }
                                    }
                                    _ => {
                                        unreachable!()
                                    }
                                }
                            }
                        }
                        None => {
                            let stream_data = create_stream_data();

                            let value = Value {
                                expiry: None,
                                value: ValueType::StreamValue(StreamValue {
                                    stream: vec![stream_data],
                                }),
                            };

                            db.map.insert(stream_key.to_string(), value);
                        }
                    }

                    let id: String = id.into();

                    let resp_message = if !is_slave {
                        Some(Message::BulkString(BulkString {
                            length: id.len() as isize,
                            string: id,
                        }))
                    } else {
                        None
                    };

                    {
                        let mut state = state.lock().await;
                        state.acked_replica_count = 0;
                    }

                    {
                        let mut state = state.lock().await;
                        state.expected_offset += message_length;
                    }
                    // print!("tx send: {message:?}");
                    // tx.send(message.clone())?;

                    return Ok(resp_message);
                }

                "xrange" => {
                    if params.len() < 3 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "XADD: Not enough params.".to_string(),
                        })));
                    }

                    let stream_key = params[0];
                    let start = params[1];
                    let end = params[2];

                    let start = if start == "-" {
                        (0, Some(0))
                    } else {
                        StreamId::parse_range_str(start)?
                    };

                    let end = if end == "+" {
                        (i64::MAX, Some(usize::MAX))
                    } else {
                        StreamId::parse_range_str(end)?
                    };

                    let mut rdb = get_rdb().write().await;
                    let index = selected_db_index.read().await;
                    let db = rdb.get_db_mut(*index)?;

                    let v = db.map.get(stream_key);

                    let mut vec: Vec<Message> = vec![];

                    match v {
                        Some(
                            v @ Value {
                                expiry: _,
                                value: ValueType::StreamValue(stream_value @ StreamValue { stream }),
                            },
                        ) if !v.is_expired() => {
                            let start_index =
                                match stream.binary_search_by(|cur: &BTreeMap<String, String>| {
                                    let start_id = StreamId {
                                        ts: start.0,
                                        seq: if start.1.is_some() {
                                            start.1.unwrap()
                                        } else {
                                            0
                                        },
                                    };
                                    cur.get("id")
                                        .unwrap()
                                        .parse::<StreamId>()
                                        .unwrap()
                                        .cmp(&start_id)
                                }) {
                                    Ok(n) => n,
                                    Err(n) => n,
                                };

                            let end_index =
                                match stream.binary_search_by(|cur: &BTreeMap<String, String>| {
                                    let end_id = StreamId {
                                        ts: end.0,
                                        seq: if end.1.is_some() {
                                            end.1.unwrap()
                                        } else {
                                            usize::MAX
                                        },
                                    };
                                    cur.get("id")
                                        .unwrap()
                                        .parse::<StreamId>()
                                        .unwrap()
                                        .cmp(&end_id)
                                }) {
                                    Ok(n) => n,
                                    Err(n) => n - 1,
                                };

                            if end_index >= start_index {
                                vec = stream_value.get_by_range(start_index..=end_index);
                            }
                        }

                        _ => {}
                    }

                    Ok(Some(Message::Array(Array { items: vec })))
                }

                "xread" => {
                    if params.len() < 3 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "XREAD: Not enough params.".to_string(),
                        })));
                    }

                    let streams_start = params
                        .iter()
                        .position(|param| param.to_ascii_lowercase() == "streams");

                    if streams_start.is_none() {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "XREAD: STREAMS not found.".to_string(),
                        })));
                    }
                    let streams_start = streams_start.unwrap() + 1;

                    let blocking = if let Some(i) = params[..streams_start]
                        .iter()
                        .position(|param| param.to_ascii_lowercase() == "block")
                    {
                        if let Ok(blocking) = params[i + 1].parse::<u64>() {
                            Some(blocking)
                        } else {
                            return Ok(Some(Message::SimpleError(
                                (SimpleError {
                                    string: "XREAD: Invalid block param.".to_string(),
                                }),
                            )));
                        }
                    } else {
                        None
                    };

                    let key_ids = &params[streams_start..];
                    let (keys, ids) = key_ids.split_at(key_ids.len() / 2);
                    let key_id_pairs: Vec<_> = keys.iter().zip(ids.iter()).collect();

                    let mut new_id_during_blocking: HashMap<String, String> = HashMap::new();

                    if let Some(blocking) = blocking {
                        let mut rx = tx.subscribe();
                        if blocking == 0 {
                            loop {
                                match rx.recv().await {
                                    Ok(Message::Array(Array { items })) => {
                                        match &items[..] {
                                            [Message::BulkString(BulkString {
                                                length: _,
                                                string: xadd,
                                            }), Message::BulkString(BulkString {
                                                length: _,
                                                string: added_key,
                                            }), Message::BulkString(BulkString {
                                                length: _,
                                                string: added_id,
                                            }), ..]
                                                if xadd.to_lowercase() == "xadd"
                                                    && keys.contains(&added_key.as_str()) =>
                                            {
                                                new_id_during_blocking.insert(
                                                    added_key.to_string(),
                                                    added_id.to_string(),
                                                );

                                                break;
                                            }
                                            _ => {
                                                // other messages
                                                continue;
                                            }
                                        }
                                    }
                                    _ => continue,
                                }
                            }
                        } else {
                            loop {
                                let tmp = timeout(Duration::from_millis(blocking), rx.recv()).await;
                                match tmp {
                                    Ok(Ok(Message::Array(Array { items }))) => {
                                        match &items[..] {
                                            [Message::BulkString(BulkString {
                                                length: _,
                                                string: xadd,
                                            }), Message::BulkString(BulkString {
                                                length: _,
                                                string: added_key,
                                            }), Message::BulkString(BulkString {
                                                length: _,
                                                string: added_id,
                                            }), ..]
                                                if xadd.to_lowercase() == "xadd"
                                                    && keys.contains(&added_key.as_str()) =>
                                            {
                                                // release
                                                new_id_during_blocking.insert(
                                                    added_key.to_string(),
                                                    added_id.to_string(),
                                                );
                                                break;
                                            }
                                            _ => {
                                                // other messages
                                                continue;
                                            }
                                        }
                                    }
                                    Ok(Err(_recv_error)) => {
                                        todo!()
                                    }
                                    Err(_) => {
                                        // timeout
                                        break;
                                    }
                                    _ => {
                                        // other messages
                                        continue;
                                    }
                                }
                            }
                        }
                    }

                    let mut rdb = get_rdb().write().await;
                    let index = selected_db_index.read().await;
                    let db = rdb.get_db_mut(*index)?;

                    let multi_streams: Vec<_> = key_id_pairs
                        .iter()
                        .filter_map(|(&k, &id)| {
                            let v = db.map.get(k);
                            let single_stream = match v {
                                Some(
                                    v @ Value {
                                        expiry: _,
                                        value:
                                            ValueType::StreamValue(
                                                stream_value @ StreamValue { stream },
                                            ),
                                    },
                                ) if !v.is_expired() => {
                                    if blocking.is_some() && id == "$" {
                                        if let Some(_new_id) = new_id_during_blocking.get(k) {
                                            // only return new record
                                            // because this redis cannot add multi records simultaneously, only return the last one
                                            let last = stream_value.stream.last();
                                            match last {
                                                Some(record) => {
                                                    let single_stream: Vec<Message> =
                                                        vec![StreamValue::get_message_by_record(
                                                            record,
                                                        )];

                                                    return Some(Message::Array(Array {
                                                        items: vec![
                                                            Message::BulkString(BulkString {
                                                                length: k.len() as isize,
                                                                string: k.to_string(),
                                                            }),
                                                            Message::Array(Array {
                                                                items: single_stream,
                                                            }),
                                                        ],
                                                    }));
                                                }
                                                None => {
                                                    return None;
                                                }
                                            }
                                        } else {
                                            return None;
                                        }
                                    }

                                    let start = if id == "-" {
                                        (0, Some(0))
                                    } else {
                                        StreamId::parse_range_str(id).unwrap()
                                    };
                                    let start_index = match stream.binary_search_by(
                                        |cur: &BTreeMap<String, String>| {
                                            let start_id = StreamId {
                                                ts: start.0,
                                                seq: if start.1.is_some() {
                                                    start.1.unwrap()
                                                } else {
                                                    0
                                                },
                                            };
                                            cur.get("id")
                                                .unwrap()
                                                .parse::<StreamId>()
                                                .unwrap()
                                                .cmp(&start_id)
                                        },
                                    ) {
                                        Ok(n) => n + 1,
                                        Err(n) => n,
                                    };

                                    stream_value.get_by_range(start_index..)
                                }

                                _ => {
                                    vec![]
                                }
                            };

                            if single_stream.len() == 0 {
                                None
                            } else {
                                Some(Message::Array(Array {
                                    items: vec![
                                        Message::BulkString(BulkString {
                                            length: k.len() as isize,
                                            string: k.to_string(),
                                        }),
                                        Message::Array(Array {
                                            items: single_stream,
                                        }),
                                    ],
                                }))
                            }
                        })
                        .collect();

                    let resp = (if multi_streams.len() == 0 {
                        Message::BulkString(BulkString {
                            length: -1,
                            string: "".to_string(),
                        })
                    } else {
                        Message::Array(Array {
                            items: multi_streams,
                        })
                    });

                    Ok(Some(resp))
                }

                "incr" => {
                    if params.len() < 1 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "INCR: Not enough params.".to_string(),
                        })));
                    }

                    let key = params[0];
                    let mut rdb = get_rdb().write().await;
                    let index = selected_db_index.read().await;
                    let db = rdb.get_db_mut(*index)?;

                    let next_int = match db.map.get_mut(key) {
                        Some(v) => {
                            let next_int =
                                if let ValueType::StringValue(StringValue { string }) = &v.value {
                                    if let Ok(next_int) = string.parse::<i64>() {
                                        next_int.checked_add(1)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };

                            if let Some(next_int) = next_int {
                                *v = Value {
                                    expiry: v.expiry,
                                    value: ValueType::StringValue(StringValue {
                                        string: next_int.to_string(),
                                    }),
                                };
                                Some(next_int)
                            } else {
                                None
                            }
                        }
                        _ => {
                            db.map.insert(
                                key.to_string(),
                                Value {
                                    expiry: None,
                                    value: ValueType::StringValue(StringValue {
                                        string: "1".to_string(),
                                    }),
                                },
                            );
                            Some(1)
                        }
                    };

                    if let Some(next_int) = next_int {
                        Ok(Some(Message::Integer(Integer { value: next_int })))
                    } else {
                        Ok(Some(Message::SimpleError(SimpleError {
                            string: "ERR value is not an integer or out of range".to_string(),
                        })))
                    }
                }

                "rpush" => {
                    if params.len() < 2 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "RPUSH must have at least 2 arguments.".to_string(),
                        })));
                    }

                    let k = params[0];
                    let mut value_list = params[1..]
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<VecDeque<_>>();

                    let mut rdb = get_rdb().write().await;
                    let index = Arc::clone(&selected_db_index);
                    let index = index.read().await;
                    let db = rdb
                        .get_db_mut(*index)
                        .context(format!("DB {} does not exist.", *index))?;

                    // db.map
                    //     .insert(k.to_string(), ValueType::ListValue(ListValue {}));

                    let list = db.map.entry(k.to_string()).or_insert(Value {
                        expiry: None,
                        value: ValueType::ListValue(ListValue {
                            list: VecDeque::new(),
                        }),
                    });

                    let len = match &mut list.value {
                        ValueType::ListValue(list_value) => {
                            list_value.list.append(&mut value_list);
                            list_value.list.len()
                        }
                        _ => {
                            unreachable!()
                        }
                    };

                    Ok(Some(Message::Integer(Integer { value: len as i64 })))
                }

                "lrange" => {
                    if params.len() < 3 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "LRANGE must have at least 3 arguments.".to_string(),
                        })));
                    }

                    let k = params[0];

                    let mut rdb = get_rdb().write().await;
                    let index = Arc::clone(&selected_db_index);
                    let index = index.read().await;
                    let db = rdb
                        .get_db_mut(*index)
                        .context(format!("DB {} does not exist.", *index))?;

                    let start = params[1].parse::<i32>().unwrap();
                    let end = params[2].parse::<i32>().unwrap();

                    let result = {
                        let list = db.map.get_mut(k);
                        match list {
                            Some(list) => match &mut list.value {
                                ValueType::ListValue(list_value) => {
                                    list_value.list.make_contiguous();
                                    utils::index(list_value.list.as_slices().0, start, end)
                                }
                                _ => &vec![],
                            },
                            None => &vec![],
                        }
                    };

                    let items = result
                        .iter()
                        .map(|item| {
                            Message::BulkString(BulkString {
                                length: item.len() as isize,
                                string: item.to_string(),
                            })
                        })
                        .collect();

                    Ok(Some(Message::Array(Array { items })))
                }

                "lpush" => {
                    if params.len() < 2 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "LPUSH must have at least 2 arguments.".to_string(),
                        })));
                    }

                    let k = params[0];
                    let mut value_list = params[1..]
                        .iter()
                        .map(|s| s.to_string())
                        .rev()
                        .collect::<VecDeque<_>>();

                    let mut rdb = get_rdb().write().await;
                    let index = Arc::clone(&selected_db_index);
                    let index = index.read().await;
                    let db = rdb
                        .get_db_mut(*index)
                        .context(format!("DB {} does not exist.", *index))?;

                    // db.map
                    //     .insert(k.to_string(), ValueType::ListValue(ListValue {}));

                    let list = db.map.entry(k.to_string()).or_insert(Value {
                        expiry: None,
                        value: ValueType::ListValue(ListValue {
                            list: VecDeque::new(),
                        }),
                    });

                    let len = match &mut list.value {
                        ValueType::ListValue(list_value) => {
                            // list_value.list.append(&mut value_list);
                            value_list.append(&mut list_value.list);
                            list_value.list = value_list;
                            list_value.list.len()
                        }
                        _ => {
                            unreachable!()
                        }
                    };

                    Ok(Some(Message::Integer(Integer { value: len as i64 })))
                }

                "llen" => {
                    if params.len() < 1 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "LLEN must have at least 1 arguments.".to_string(),
                        })));
                    }

                    let k = params[0];

                    let mut rdb = get_rdb().write().await;
                    let index = Arc::clone(&selected_db_index);
                    let index = index.read().await;
                    let db = rdb
                        .get_db_mut(*index)
                        .context(format!("DB {} does not exist.", *index))?;

                    if let Some(value) = db.map.get(k) {
                        match &value.value {
                            ValueType::ListValue(list_value) => {
                                Ok(Some(Message::Integer(Integer {
                                    value: list_value.list.len() as i64,
                                })))
                            }
                            _ => Ok(Some(Message::SimpleError(SimpleError::new_wrongtype()))),
                        }
                    } else {
                        Ok(Some(Message::Integer(Integer { value: 0 })))
                    }
                }

                "lpop" => {
                    if params.len() < 1 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "LPOP must have at least 1 arguments.".to_string(),
                        })));
                    }

                    let k = params[0];
                    let count = params
                        .get(1)
                        .map_or(1, |count| count.parse::<usize>().unwrap());

                    let mut rdb = get_rdb().write().await;
                    let index = Arc::clone(&selected_db_index);
                    let index = index.read().await;
                    let db = rdb
                        .get_db_mut(*index)
                        .context(format!("DB {} does not exist.", *index))?;

                    Ok(Some(lpop(db, k, count).0))
                }

                "blpop" => {
                    if params.len() < 2 {
                        return Ok(Some(Message::SimpleError(SimpleError {
                            string: "BLPOP must have at least 2 arguments.".to_string(),
                        })));
                    }

                    let k = params[0];
                    let blocking = params[1].parse::<f64>().unwrap();

                    // Register blocking. Use Message for convenience.
                    {
                        let register_msg = Message::new_array(vec![
                            Message::new_bulk_string(peer_addr.clone()),
                            message.clone(),
                        ]);
                        tx.send(register_msg).unwrap();
                    }

                    let mut rx = tx.subscribe();
                    if blocking == 0.0 {
                        loop {
                            match rx.recv().await {
                                Ok(Message::Array(Array { items })) => {
                                    let Message::BulkString(BulkString {
                                        string: target_peer_addr,
                                        ..
                                    }) = &items[0]
                                    else {
                                        unreachable!()
                                    };
                                    if &peer_addr == target_peer_addr {
                                        // didn't check if message is LPUSH/RPUSH
                                        break;
                                    }
                                }
                                _ => continue,
                            }
                        }

                        let mut rdb = get_rdb().write().await;
                        let index = Arc::clone(&selected_db_index);
                        let index = index.read().await;
                        let db = rdb
                            .get_db_mut(*index)
                            .context(format!("DB {} does not exist.", *index))?;

                        let (popped_msg, count) = lpop(db, k, 1);

                        if count == 0 {
                            Ok(Some(Message::BulkString(BulkString {
                                length: -1,
                                string: String::new(),
                            })))
                        } else {
                            Ok(Some(Message::Array(Array {
                                items: vec![
                                    Message::BulkString(BulkString {
                                        length: k.len() as isize,
                                        string: k.to_string(),
                                    }),
                                    popped_msg,
                                ],
                            })))
                        }
                    } else {
                        let inserted = loop {
                            match timeout(
                                Duration::from_millis((blocking * 1000.0).trunc() as u64),
                                rx.recv(),
                            )
                            .await
                            {
                                Ok(Ok(Message::Array(Array { items }))) => {
                                    let Message::BulkString(BulkString {
                                        string: target_peer_addr,
                                        ..
                                    }) = &items[0]
                                    else {
                                        unreachable!()
                                    };
                                    if &peer_addr == target_peer_addr {
                                        // didn't check if message is LPUSH/RPUSH
                                        break true;
                                    }
                                }
                                Ok(Err(_recv_error)) => {
                                    todo!()
                                }
                                Err(_) => {
                                    // timeout
                                    break false;
                                }
                                _ => {
                                    // other messages
                                    continue;
                                }
                            }
                        };

                        let mut rdb = get_rdb().write().await;
                        let index = Arc::clone(&selected_db_index);
                        let index = index.read().await;
                        let db = rdb
                            .get_db_mut(*index)
                            .context(format!("DB {} does not exist.", *index))?;

                        let (popped_msg, count) = lpop(db, k, 1);
                        if count == 0 {
                            Ok(Some(Message::BulkString(BulkString {
                                length: -1,
                                string: String::new(),
                            })))
                        } else {
                            Ok(Some(Message::Array(Array {
                                items: vec![
                                    Message::BulkString(BulkString {
                                        length: k.len() as isize,
                                        string: k.to_string(),
                                    }),
                                    popped_msg,
                                ],
                            })))
                        }
                    }
                }

                "subscribe" => {
                    let chan_name = get_param!(0, params, "PUBLISH");
                    // let subscribe_msg =
                    //     Message::new_array(vec![Message::new_bulk_string(peer_addr.clone()), mess]);
                    let mut subscribe_map = SUBSCRIBE_MAP.lock().await;

                    let client_entry = subscribe_map.entry(chan_name.to_string()).or_default();

                    client_entry
                        .entry(peer_addr.to_string())
                        .or_insert_with(|| message_writer);

                    let mut subscribe_client_map = SUBSCRIBE_CLIENT_MAP.lock().await;
                    let set = subscribe_client_map
                        .entry(peer_addr.to_string())
                        .and_modify(|set| {
                            set.insert(chan_name.to_string());
                        })
                        .or_insert_with(|| HashSet::from([chan_name.to_string()]));

                    Ok(Some(Message::new_array(vec![
                        Message::new_bulk_string("subscribe".to_string()),
                        Message::new_bulk_string(chan_name.to_string()),
                        Message::Integer(Integer {
                            value: set.len() as i64,
                        }),
                    ])))
                }

                "unsubscribe" => {
                    let chan_name = *get_param!(0, params, "UNSUBSCRIBE");
                    let (mut subscribe_client_map, mut subscribe_map) =
                        tokio::join!(SUBSCRIBE_CLIENT_MAP.lock(), SUBSCRIBE_MAP.lock());

                    let mut remaining_channels_count = 0;
                    if let Some(set) = subscribe_client_map.get_mut(&peer_addr) {
                        if set.remove(chan_name) {
                            if let Some(map) = subscribe_map.get_mut(chan_name) {
                                map.remove(&peer_addr);
                                if map.is_empty() {
                                    subscribe_map.remove(chan_name);
                                }
                            }
                        }

                        remaining_channels_count = set.len();

                        if set.is_empty() {
                            subscribe_client_map.remove(&peer_addr);
                        }
                    }

                    Ok(Some(Message::new_array(vec![
                        Message::new_bulk_string("unsubscribe".to_string()),
                        Message::new_bulk_string(chan_name.to_string()),
                        Message::Integer(Integer {
                            value: remaining_channels_count as i64,
                        }),
                    ])))
                }

                "publish" => {
                    let chan_name = get_param!(0, params, "PUBLISH");
                    let value = get_param!(1, params, "PUBLISH");

                    let mut subscribe_map = SUBSCRIBE_MAP.lock().await;

                    let client_entry = subscribe_map.entry(chan_name.to_string()).or_default();
                    println!("{:?}", client_entry.keys());
                    let client_count = client_entry.len();

                    for (_, message_writer) in client_entry {
                        // client_ip
                        let mut message_writer = message_writer.lock().await;
                        message_writer
                            .send(Message::new_array(vec![
                                Message::new_bulk_string("message".to_string()),
                                Message::new_bulk_string(chan_name.to_string()),
                                Message::new_bulk_string(value.to_string()),
                            ]))
                            .await;
                    }

                    Ok(Some(Message::Integer(Integer {
                        value: client_count as i64,
                    })))
                }

                "zadd" => {
                    with_db(selected_db_index, |db| {
                        let key = get_param!(0, params, "ZADD");
                        let score = get_param!(1, params, "ZADD");
                        let member = get_param!(2, params, "ZADD");

                        let score = score.parse::<f64>().unwrap();

                        let count = match db.get_sorted_set(key.to_string()) {
                            Ok(Some(sorted_set)) => {
                                if sorted_set.zset.add(member.to_string(), score) {
                                    1
                                } else {
                                    0
                                }
                            }
                            Ok(None) => {
                                db.map.remove(*key);
                                let mut zset = ZSet::new();
                                zset.add(member.to_string(), score);
                                let mut zset_value = Value {
                                    expiry: None,
                                    value: ValueType::SortedSet(SortedSet { zset }),
                                };

                                db.map.insert(key.to_string(), zset_value);
                                1
                            }
                            Err(e) => {
                                if let Ok(e) = e.downcast::<SimpleError>() {
                                    return Ok(Some(Message::SimpleError(e)));
                                }
                                0
                            }
                        };

                        Ok(Some(Message::Integer(Integer { value: count })))
                    })
                    .await?
                }

                "zrank" => {
                    let key = get_param!(0, params, "ZADD");
                    let member = get_param!(1, params, "ZADD");

                    with_db(selected_db_index, |db| {
                        match db.get_sorted_set(key.to_string()) {
                            Ok(Some(sorted_set)) => {
                                if let Some(rank) = sorted_set.zset.rank(*member) {
                                    return Ok(Some(Message::Integer(Integer {
                                        value: rank as i64,
                                    })));
                                }
                            }
                            _ => {}
                        }

                        Ok(Some(Message::BulkString(BulkString {
                            length: -1,
                            string: "".to_string(),
                        })))
                    })
                    .await?
                }

                "zrange" => {
                    let key = get_param!(0, params, "ZRANGE");
                    let start = get_param!(1, params, "ZRANGE");
                    let end = get_param!(2, params, "ZRANGE");
                    let start = start.parse::<i32>().unwrap();
                    let end = end.parse::<i32>().unwrap();

                    with_db(selected_db_index, |db| {
                        let result = match db.get_sorted_set(key.to_string()) {
                            Ok(Some(sorted_set)) => sorted_set.zset.range(start, end),
                            _ => vec![],
                        };

                        let items = result
                            .iter()
                            .map(|item| {
                                Message::BulkString(BulkString {
                                    length: item.len() as isize,
                                    string: item.to_string(),
                                })
                            })
                            .collect();

                        Ok(Some(Message::Array(Array { items })))
                    })
                    .await?
                }

                "zcard" => {
                    let key = get_param!(0, params, "ZCARD");

                    with_db(selected_db_index, |db| {
                        let count = match db.get_sorted_set(key.to_string()) {
                            Ok(Some(sorted_set)) => sorted_set.zset.len(),
                            _ => 0,
                        };

                        Ok(Some(Message::Integer(Integer {
                            value: count as i64,
                        })))
                    })
                    .await?
                }

                "zscore" => {
                    let key = get_param!(0, params, "ZSCORE");
                    let member = get_param!(1, params, "ZSCORE");

                    with_db(selected_db_index, |db| {
                        match db.get_sorted_set(key.to_string()) {
                            Ok(Some(sorted_set)) => {
                                if let Some(score) = sorted_set.zset.score(member) {
                                    return Ok(Some(Message::new_bulk_string(score.to_string())));
                                }
                            }
                            _ => {}
                        }

                        Ok(Some(Message::BulkString(BulkString {
                            length: -1,
                            string: "".to_string(),
                        })))
                    })
                    .await?
                }

                "zrem" => {
                    let key = get_param!(0, params, "ZSCORE");
                    let member = get_param!(1, params, "ZSCORE");

                    with_db(selected_db_index, |db| {
                        let count = match db.get_sorted_set(key.to_string()) {
                            Ok(Some(sorted_set)) => {
                                if sorted_set.zset.rem(member.to_string()) {
                                    1
                                } else {
                                    0
                                }
                            }
                            _ => 0,
                        };

                        Ok(Some(Message::Integer(Integer {
                            value: count as i64,
                        })))
                    })
                    .await?
                }

                _ => {
                    // Ok(Some(Message::SimpleError(SimpleError {
                    //     string: "Unsupported command.".to_string(),
                    // })))
                    Ok(None)
                }
            }
        }
        _ => return Ok(None),
    }
}

async fn handle_connection_to_master(
    stream: TcpStream,
    args: Arc<RwLock<Args>>,
    info: Arc<Info>,
    state: Arc<Mutex<State>>,
    tx: Sender<Message>,
) -> anyhow::Result<()> {
    let mut socket: Framed<TcpStream, MessageFramer> = Framed::new(stream, MessageFramer);
    let ping = "PING";
    socket
        .send(Message::Array(Array {
            items: vec![Message::BulkString(BulkString {
                length: ping.len() as isize,
                string: ping.to_string(),
            })],
        }))
        .await?;

    let _ = socket.next().await.context("PING should have response.")?;

    let repl_conf = "REPLCONF";
    let listening_port = "listening-port";
    let port = {
        let args = args.read().await;
        args.port.to_string()
    };
    socket
        .send(Message::Array(Array {
            items: vec![
                Message::BulkString(BulkString {
                    length: repl_conf.len() as isize,
                    string: repl_conf.to_string(),
                }),
                Message::BulkString(BulkString {
                    length: listening_port.len() as isize,
                    string: listening_port.to_string(),
                }),
                Message::BulkString(BulkString {
                    length: port.len() as isize,
                    string: port,
                }),
            ],
        }))
        .await?;
    let resp = socket
        .next()
        .await
        .expect("First REPLCONF")
        .context("First REPLCONF")?;
    if let Message::SimpleString(SimpleString { string }) = resp {
        anyhow::ensure!(string == "OK");
    } else {
        return Err(anyhow::Error::msg("Invalid first REPLCONF"));
    }

    let capa = "capa";
    let psync2 = "psync2";
    socket
        .send(Message::Array(Array {
            items: vec![
                Message::BulkString(BulkString {
                    length: repl_conf.len() as isize,
                    string: repl_conf.to_string(),
                }),
                Message::BulkString(BulkString {
                    length: capa.len() as isize,
                    string: capa.to_string(),
                }),
                Message::BulkString(BulkString {
                    length: psync2.len() as isize,
                    string: psync2.to_string(),
                }),
            ],
        }))
        .await?;
    let resp = socket
        .next()
        .await
        .expect("Second REPLCONF")
        .context("Second REPLCONF")?;
    if let Message::SimpleString(SimpleString { string }) = resp {
        anyhow::ensure!(string == "OK");
    } else {
        return Err(anyhow::Error::msg("Invalid second REPLCONF"));
    }

    let psync = "PSYNC";
    socket
        .send(Message::Array(Array {
            items: vec![
                Message::BulkString(BulkString {
                    length: psync.len() as isize,
                    string: psync.to_string(),
                }),
                Message::BulkString(BulkString {
                    length: 1,
                    string: "?".to_string(),
                }),
                Message::BulkString(BulkString {
                    length: 2,
                    string: "-1".to_string(),
                }),
            ],
        }))
        .await?;
    let resp = socket
        .next()
        .await
        .expect("Second REPLCONF")
        .context("Second REPLCONF")?;
    if let Message::SimpleString(SimpleString { string }) = resp {
        println!("{string}");
    } else {
        return Err(anyhow::Error::msg("Invalid second REPLCONF"));
    }

    // this is for listening to messages forwarded from master
    tokio::spawn(async move {
        if let Err(e) = handler(socket, args, info, state, tx, true).await {
            println!("Err: {}", e);
        }
    });

    Ok(())
}

fn lpop(db: &mut Database, key: &str, count: usize) -> (Message, usize) {
    if let Some(value) = db.map.get_mut(key) {
        match &mut value.value {
            ValueType::ListValue(list_value) => {
                list_value.list.make_contiguous();

                let count = count.min(list_value.list.len());
                let removed = list_value.list.drain(..count).collect::<Vec<_>>();
                // let res = list_value.list.pop_front();
                match removed.len() {
                    0 => (
                        Message::BulkString(BulkString {
                            length: -1,
                            string: String::new(),
                        }),
                        0,
                    ),
                    1 => (
                        Message::BulkString(BulkString {
                            length: removed[0].len() as isize,
                            string: removed[0].clone(),
                        }),
                        1,
                    ),
                    n => {
                        let items = removed
                            .iter()
                            .map(|s| {
                                Message::BulkString(BulkString {
                                    length: s.len() as isize,
                                    string: s.to_string(),
                                })
                            })
                            .collect();

                        (Message::Array(Array { items }), n)
                    }
                }
            }
            _ => (Message::SimpleError(SimpleError::new_wrongtype()), 0),
        }
    } else {
        (Message::Integer(Integer { value: 0 }), 0)
    }
}
