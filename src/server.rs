use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};

use crate::message::MessageWriter;
use crate::rdb::RDB;

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
}

impl Server {
    pub fn new(rdb: RDB, replication_state: ReplicationState) -> Self {
        Self {
            rdb: Arc::new(RwLock::new(rdb)),
            blocking: Arc::new(BlockingManager::new()),
            pubsub: Arc::new(PubSubManager::new()),
            replication: Arc::new(RwLock::new(replication_state)),
        }
    }
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
        waiters.entry(key).or_insert_with(Vec::new).push(notify.clone());
        notify
    }

    /// Notify ONE client waiting for a list key (FIFO - first registered gets notified)
    pub async fn notify_list_key(&self, key: &str) {
        let mut waiters = self.list_waiters.lock().await;
        if let Some(list) = waiters.get_mut(key) {
            if let Some(notify) = list.first() {
                notify.notify_one();
                list.remove(0); // Remove the notified client
            }
            // Clean up empty entries
            if list.is_empty() {
                waiters.remove(key);
            }
        }
    }

    /// Register a client waiting for a stream key (XREAD)
    pub async fn register_stream_waiter(&self, key: String) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let mut waiters = self.stream_waiters.lock().await;
        waiters.entry(key).or_insert_with(Vec::new).push(notify.clone());
        notify
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
