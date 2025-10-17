# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Redis clone implementation in Rust, built as part of the CodeCrafters "Build Your Own Redis" challenge. The implementation supports a substantial subset of Redis functionality including replication, pub/sub, streams, sorted sets, lists, and blocking operations.

## Build and Run Commands

**Build the project:**
```bash
cargo build --release --target-dir=/tmp/codecrafters-build-redis-rust
```

**Run the server:**
```bash
./your_program.sh
# Or directly:
/tmp/codecrafters-build-redis-rust/release/codecrafters-redis
```

**Run as replica:**
```bash
./your_program.sh --port 6380 --replicaof "127.0.0.1 6379"
```

**Run with RDB file:**
```bash
./your_program.sh --dir /tmp/redis-data --dbfilename dump.rdb
```

**Submit to CodeCrafters:**
```bash
git commit -am "your message"
git push origin master
```

## Architecture

### Core Components

**Main Server (`src/main.rs`)**
- Entry point is the `main()` function which sets up the TCP listener on port 6379 (configurable)
- Uses Tokio for async networking with full features enabled
- Each client connection spawns a separate async task via the `handler()` function
- The server can operate in two roles: Master or Slave (configured via `--replicaof`)

**Message Protocol (`src/message.rs`)**
- Implements RESP (REdis Serialization Protocol) via `MessageFramer`
- `MessageFramer` implements Tokio's `Encoder` and `Decoder` traits for frame-based communication
- Message types: Array, SimpleString, BulkString, Integer, SimpleError, RDB
- All messages can calculate their own byte length via `message.length()`

**Data Storage (`src/rdb/mod.rs`)**
- `RDB` struct contains the entire database state with multiple database indices
- `Database` struct holds a BTreeMap of key-value pairs for a single database
- `Value` struct wraps all value types and handles expiry timestamps
- `ValueType` enum supports: StringValue, StreamValue, ListValue, SortedSet
- Expiry is stored as Unix timestamp in milliseconds

**Sorted Sets (`src/rdb/zset/`)**
- Custom skip list implementation in `skip_list.rs` for O(log n) operations
- `ZSet` provides Redis-like sorted set operations: add, rank, range, score, rem

### Key Design Patterns

**Global State Management**
- Uses `LazyLock` and `OnceLock` for lazy-initialized static globals
- `REGISTER_MAP`: Tracks blocking BLPOP clients by peer address
- `SUBSCRIBE_MAP`: Maps channel names to subscribed client writers
- `SUBSCRIBE_CLIENT_MAP`: Reverse map from client address to subscribed channels
- `get_rdb()`: Returns static `RwLock<RDB>` for concurrent database access

**Connection Handling**
- Connections are split into separate reader/writer using `socket.split()`
- Writers are wrapped in `Arc<Mutex<SplitSink>>` for shared ownership
- Readers are wrapped in `Arc<Mutex<SplitStream>>` for exclusive access
- This allows concurrent reads from one task while another writes

**Message Broadcasting**
- Uses `tokio::sync::broadcast` channel for pub/sub and replication
- Master propagates write commands (SET, XADD) to all replicas via broadcast
- LPUSH/RPUSH commands wake blocking BLPOP clients via broadcast
- Each replica subscribes to the broadcast channel after PSYNC handshake

**Transaction Support**
- MULTI command starts queuing messages in a `VecDeque<Message>`
- Commands between MULTI and EXEC are queued and return "QUEUED"
- EXEC executes all queued commands atomically and returns results as an array
- DISCARD clears the queue without executing

**Replication Protocol**
- Slave connects to master and performs handshake: PING → REPLCONF → REPLCONF → PSYNC
- Master responds with FULLRESYNC and sends RDB snapshot
- After sync, master propagates all write commands to replicas
- Replicas track byte offset for each received command
- WAIT command uses REPLCONF GETACK to check replica synchronization

**Blocking Operations**
- BLPOP registers client in `REGISTER_MAP` with timestamp and original message
- When LPUSH/RPUSH occurs, it finds oldest waiting BLPOP client and wakes it
- Uses broadcast channel with timeout for finite blocking durations
- Blocking with 0 timeout waits indefinitely until list has data

**Pub/Sub Mode**
- After SUBSCRIBE, client enters special mode with restricted command set
- Only SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT allowed
- PUBLISH sends messages to all subscribed clients via stored message writers
- Client disconnection automatically cleans up subscriptions

**Stream Operations**
- Streams store entries as `Vec<BTreeMap<String, String>>` with "id" key
- XADD auto-generates IDs based on timestamp and sequence number
- XRANGE uses binary search to find start/end indices efficiently
- XREAD supports blocking mode similar to BLPOP using broadcast channel
- Special "$" ID in XREAD means "latest" for blocking reads

## Implementation Details

**Command Parsing**
- Commands are parsed from Array messages into `Vec<&str>` via `parse_message()`
- First element is command name, rest are parameters
- Use `get_param!` macro for safe parameter extraction with error messages

**Database Selection**
- Each handler maintains `selected_db_index` in `Arc<RwLock<usize>>`
- Use `with_db()` helper for safe database access with proper locking

**Expiry Handling**
- All value access checks `is_expired()` before returning data
- Expired values are treated as if key doesn't exist
- Expired values are removed from map when detected

**Slave Offset Tracking**
- Slaves maintain byte offset of all processed commands in `State.offset`
- Master tracks expected offset in `State.expected_offset`
- WAIT command uses this to verify replica synchronization state

**Error Handling**
- Most commands return `anyhow::Result<Option<Message>>`
- `Ok(Some(message))` sends response to client
- `Ok(None)` sends no response (for slave-only commands)
- `Err(e)` propagates error up

## Important Notes

- The `handler()` function has an `is_slave` parameter to differentiate replica behavior
- Slave connections don't send responses for write commands (SET, XADD, etc.)
- Message length includes full RESP encoding (e.g., "$3\r\nfoo\r\n" = 9 bytes)
- Some commands update replication state: `acked_replica_count`, `expected_offset`
- PING in subscribe mode returns array format, not simple string
- List operations (LPUSH/RPUSH) reverse order for correct insertion
