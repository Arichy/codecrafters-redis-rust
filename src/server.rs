use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, Notify, RwLock};

use crate::commands::auth::User;
use crate::message::{Message, MessageWriter};
use crate::rdb::RDB;
use futures_util::SinkExt;

/// Core server state that manages all shared resources
#[derive(Clone)]
pub struct Server {
    /// The main database
    pub rdb: Arc<RwLock<RDB>>,
    /// Blocking operations manager
    pub blocking: Arc<BlockingManager>,
    /// Pub/Sub manager
    pub pubsub: Arc<PubSubManager>,
    /// Replication state
    pub replication: Arc<RwLock<ReplicationState>>,
    /// Replica connection manager
    pub replicas: Arc<ReplicaManager>,
    /// Users
    pub users: Arc<DashMap<String, Arc<User>>>,
    /// Watchers
    pub watchers: Arc<WatchingManager>,
}

impl Server {
    pub fn new(rdb: RDB, replication_state: ReplicationState) -> Self {
        let default_user = User {
            name: "default".to_string(),
            passwords: vec![],
        };

        let mut users = DashMap::new();
        users.insert("default".to_string(), Arc::new(default_user));

        Self {
            rdb: Arc::new(RwLock::new(rdb)),
            blocking: Arc::new(BlockingManager::new()),
            pubsub: Arc::new(PubSubManager::new()),
            replication: Arc::new(RwLock::new(replication_state)),
            replicas: Arc::new(ReplicaManager::new()),
            users: Arc::new(users),
            watchers: Arc::new(WatchingManager::new()),
        }
    }

    pub fn notify_watchers(&self, key: &str) {}
}

/// Manages blocking operations like BLPOP, XREAD block
pub struct BlockingManager {
    /// Map of key -> list of waiting clients (FIFO order)
    /// Each client has a Notify to wake them up
    list_waiters: Mutex<HashMap<String, Vec<Arc<Notify>>>>,
    /// Map of stream key -> list of waiting clients for XREAD
    stream_waiters: Mutex<HashMap<String, Vec<Arc<Notify>>>>,
}

impl BlockingManager {
    pub fn new() -> Self {
        Self {
            list_waiters: Mutex::new(HashMap::new()),
            stream_waiters: Mutex::new(HashMap::new()),
        }
    }

    /// Register a client waiting for a list key (BLPOP)
    pub async fn register_list_waiter(&self, key: String) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let mut waiters = self.list_waiters.lock().await;
        waiters
            .entry(key)
            .or_insert_with(Vec::new)
            .push(notify.clone());
        notify
    }

    /// Notify ONE client waiting for a list key (FIFO - first registered gets notified)
    pub async fn notify_list_key(&self, key: &str) {
        let mut waiters = self.list_waiters.lock().await;
        if let Some(list) = waiters.get_mut(key) {
            if let Some(notify) = list.first() {
                notify.notify_one();
                list.remove(0);
            }
            if list.is_empty() {
                waiters.remove(key);
            }
        }
    }

    /// Register a client waiting for a stream key (XREAD)
    pub async fn register_stream_waiter(&self, key: String) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let mut waiters = self.stream_waiters.lock().await;
        waiters
            .entry(key)
            .or_insert_with(Vec::new)
            .push(notify.clone());
        notify
    }

    /// Register a client-provided Notify on a stream key (for multi-stream XREAD)
    pub async fn register_stream_waiter_with_notify(&self, key: String, notify: Arc<Notify>) {
        let mut waiters = self.stream_waiters.lock().await;
        waiters.entry(key).or_insert_with(Vec::new).push(notify);
    }

    /// Notify ALL clients waiting for a stream key
    pub async fn notify_stream_key(&self, key: &str) {
        let mut waiters = self.stream_waiters.lock().await;
        if let Some(list) = waiters.remove(key) {
            for notify in list {
                notify.notify_one();
            }
        }
    }

    /// Remove a waiter when client disconnects or cancels
    pub async fn remove_list_waiter(&self, key: &str, notify: &Arc<Notify>) {
        let mut waiters = self.list_waiters.lock().await;
        if let Some(list) = waiters.get_mut(key) {
            list.retain(|n| !Arc::ptr_eq(n, notify));
            if list.is_empty() {
                waiters.remove(key);
            }
        }
    }

    pub async fn remove_stream_waiter(&self, key: &str, notify: &Arc<Notify>) {
        let mut waiters = self.stream_waiters.lock().await;
        if let Some(list) = waiters.get_mut(key) {
            list.retain(|n| !Arc::ptr_eq(n, notify));
            if list.is_empty() {
                waiters.remove(key);
            }
        }
    }
}

/// Manages Pub/Sub subscriptions
pub struct PubSubManager {
    /// channel_name -> (client_id, MessageWriter)
    subscriptions: Mutex<HashMap<String, HashMap<String, MessageWriter>>>,
    /// client_id -> set of subscribed channels
    client_channels: Mutex<HashMap<String, Vec<String>>>,
}

impl PubSubManager {
    pub fn new() -> Self {
        Self {
            subscriptions: Mutex::new(HashMap::new()),
            client_channels: Mutex::new(HashMap::new()),
        }
    }

    pub async fn subscribe(&self, client_id: String, channel: String, writer: MessageWriter) {
        let mut subs = self.subscriptions.lock().await;
        subs.entry(channel.clone())
            .or_insert_with(HashMap::new)
            .insert(client_id.clone(), writer);

        let mut clients = self.client_channels.lock().await;
        clients
            .entry(client_id)
            .or_insert_with(Vec::new)
            .push(channel);
    }

    pub async fn unsubscribe(&self, client_id: &str, channel: &str) -> usize {
        let mut subs = self.subscriptions.lock().await;
        if let Some(channel_subs) = subs.get_mut(channel) {
            channel_subs.remove(client_id);
            if channel_subs.is_empty() {
                subs.remove(channel);
            }
        }

        let mut clients = self.client_channels.lock().await;
        if let Some(channels) = clients.get_mut(client_id) {
            channels.retain(|c| c != channel);
            let remaining = channels.len();
            if channels.is_empty() {
                clients.remove(client_id);
            }
            return remaining;
        }
        0
    }

    pub async fn get_subscribers(&self, channel: &str) -> Vec<MessageWriter> {
        let subs = self.subscriptions.lock().await;
        subs.get(channel)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn get_client_channels(&self, client_id: &str) -> Vec<String> {
        let clients = self.client_channels.lock().await;
        clients.get(client_id).cloned().unwrap_or_default()
    }

    pub async fn is_subscribed(&self, client_id: &str) -> bool {
        let clients = self.client_channels.lock().await;
        clients.contains_key(client_id)
    }

    pub async fn disconnect_client(&self, client_id: &str) {
        let channels = {
            let mut clients = self.client_channels.lock().await;
            clients.remove(client_id)
        };

        if let Some(channels) = channels {
            let mut subs = self.subscriptions.lock().await;
            for channel in channels {
                if let Some(channel_subs) = subs.get_mut(&channel) {
                    channel_subs.remove(client_id);
                    if channel_subs.is_empty() {
                        subs.remove(&channel);
                    }
                }
            }
        }
    }
}

/// Replication state for master-slave replication
#[derive(Debug, Clone)]
pub struct ReplicationState {
    pub role: Role,
    pub master_replid: String,
    pub master_repl_offset: isize,
    // Master-specific state
    pub total_replica_count: usize,
    pub acked_replica_count: usize,
    pub expected_offset: usize,
    pub wait_notify: Arc<Notify>,
    // Slave-specific state
    pub slave_offset: usize,
}

#[derive(Debug, Clone)]
pub enum Role {
    Master,
    Slave(std::net::SocketAddr),
}

impl ReplicationState {
    pub fn new_master() -> Self {
        Self {
            role: Role::Master,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            total_replica_count: 0,
            acked_replica_count: 0,
            expected_offset: 0,
            wait_notify: Arc::new(Notify::new()),
            slave_offset: 0,
        }
    }

    pub fn new_slave(master_addr: std::net::SocketAddr) -> Self {
        Self {
            role: Role::Slave(master_addr),
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            total_replica_count: 0,
            acked_replica_count: 0,
            expected_offset: 0,
            wait_notify: Arc::new(Notify::new()),
            slave_offset: 0,
        }
    }

    pub fn is_master(&self) -> bool {
        matches!(self.role, Role::Master)
    }

    pub fn is_slave(&self) -> bool {
        matches!(self.role, Role::Slave(_))
    }
}

/// Manages replica connections for command propagation
pub struct ReplicaManager {
    /// peer_addr -> MessageWriter for each connected replica
    writers: Mutex<HashMap<String, MessageWriter>>,
}

impl ReplicaManager {
    pub fn new() -> Self {
        Self {
            writers: Mutex::new(HashMap::new()),
        }
    }

    pub async fn register(&self, peer_addr: String, writer: MessageWriter) {
        self.writers.lock().await.insert(peer_addr, writer);
    }

    pub async fn unregister(&self, peer_addr: &str) {
        self.writers.lock().await.remove(peer_addr);
    }

    /// Send a message to all replicas, returning the count
    pub async fn broadcast(&self, message: &Message) -> usize {
        let writers = self.writers.lock().await;
        let mut count = 0;
        for writer in writers.values() {
            let mut w = writer.lock().await;
            if w.send(message.clone()).await.is_ok() {
                count += 1;
            }
        }
        count
    }
}

pub struct WatchingManager {
    /// key -> (client_id -> is_dirty)
    /// Each key can have multiple clients watching it
    watchers: DashMap<String, HashMap<String, Arc<AtomicBool>>>,
    /// client_id -> set of keys (reverse mapping for UNWATCH)
    client_keys: DashMap<String, HashSet<String>>,
}

impl WatchingManager {
    pub fn new() -> Self {
        Self {
            watchers: DashMap::new(),
            client_keys: DashMap::new(),
        }
    }

    /// Register a client watching a key
    pub fn register(&self, key: String, client_id: String, is_dirty: Arc<AtomicBool>) {
        // Add to watchers (key -> client)
        self.watchers
            .entry(key.clone())
            .or_insert_with(HashMap::new)
            .insert(client_id.clone(), is_dirty);

        // Add to client_keys (client -> key)
        self.client_keys
            .entry(client_id)
            .or_insert_with(HashSet::new)
            .insert(key);
    }

    /// Unregister a specific key watch for a client
    pub fn unregister(&self, key: &str, client_id: &str) {
        // Remove from watchers
        if let Some(mut entry) = self.watchers.get_mut(key) {
            entry.remove(client_id);
            if entry.is_empty() {
                self.watchers.remove(key);
            }
        }

        // Remove from client_keys
        if let Some(mut entry) = self.client_keys.get_mut(client_id) {
            entry.remove(key);
            if entry.is_empty() {
                self.client_keys.remove(client_id);
            }
        }
    }

    /// Unwatch all keys for a client (UNWATCH command)
    pub fn unwatch_all(&self, client_id: &str) {
        // Get all keys this client is watching
        if let Some((_, keys)) = self.client_keys.remove(client_id) {
            // Remove each key from the watchers map
            for key in keys {
                if let Some(mut entry) = self.watchers.get_mut(&key) {
                    entry.remove(client_id);
                    if entry.is_empty() {
                        self.watchers.remove(&key);
                    }
                }
            }
        }
    }

    /// Notify all clients watching a key that it has changed
    pub fn notify(&self, key: &str) {
        if let Some((_, watchers)) = self.watchers.remove(key) {
            for (_, is_dirty) in watchers {
                // Use Release to ensure database writes are visible to observers
                is_dirty.store(true, std::sync::atomic::Ordering::Release);
            }
        }
    }
}
