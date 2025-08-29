use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Notify, Mutex};
use tokio::time::{timeout, Duration, Instant};

use crate::connection::MessageWriter;

pub struct BlockingClient {
    pub client_addr: String,
    pub key: String,
    pub writer: MessageWriter,
    pub timeout: Option<Duration>,
    pub start_time: Instant,
    pub notify: Arc<Notify>,
}

pub struct BlockingCommandManager {
    // key -> list of waiting clients (ordered by arrival time)
    waiting_clients: Arc<RwLock<HashMap<String, Vec<BlockingClient>>>>,
    // client_addr -> (key, notify)
    client_keys: Arc<RwLock<HashMap<String, (String, Arc<Notify>)>>>,
}

impl BlockingCommandManager {
    pub fn new() -> Self {
        Self {
            waiting_clients: Arc::new(RwLock::new(HashMap::new())),
            client_keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn add_blocking_client(
        &self,
        client_addr: &str,
        key: &str,
        writer: MessageWriter,
        timeout_secs: f64,
    ) -> Arc<Notify> {
        let mut waiting_clients = self.waiting_clients.write().await;
        let mut client_keys = self.client_keys.write().await;
        
        // Create a notify for this client
        let notify = Arc::new(Notify::new());
        
        let blocking_client = BlockingClient {
            client_addr: client_addr.to_string(),
            key: key.to_string(),
            writer,
            timeout: if timeout_secs == 0.0 {
                None
            } else {
                Some(Duration::from_secs_f64(timeout_secs))
            },
            start_time: Instant::now(),
            notify: Arc::clone(&notify),
        };
        
        // Add to waiting list for this key
        waiting_clients
            .entry(key.to_string())
            .or_insert_with(Vec::new)
            .push(blocking_client);
        
        // Track which key this client is waiting for
        client_keys.insert(client_addr.to_string(), (key.to_string(), Arc::clone(&notify)));
        
        notify
    }
    
    pub async fn remove_client(&self, client_addr: &str) {
        let mut waiting_clients = self.waiting_clients.write().await;
        let mut client_keys = self.client_keys.write().await;
        
        if let Some((key, _)) = client_keys.remove(client_addr) {
            // Remove from waiting list
            if let Some(clients) = waiting_clients.get_mut(&key) {
                clients.retain(|c| c.client_addr != client_addr);
                
                if clients.is_empty() {
                    waiting_clients.remove(&key);
                }
            }
        }
    }
    
    pub async fn pop_waiting_client(&self, key: &str) -> Option<BlockingClient> {
        let mut waiting_clients = self.waiting_clients.write().await;
        let mut client_keys = self.client_keys.write().await;
        
        if let Some(clients) = waiting_clients.get_mut(key) {
            if !clients.is_empty() {
                // Take the first client (FIFO order)
                let client = clients.remove(0);
                
                // Remove from client_keys tracking
                client_keys.remove(&client.client_addr);
                
                // Clean up if no more clients
                if clients.is_empty() {
                    waiting_clients.remove(key);
                }
                
                return Some(client);
            }
        }
        
        None
    }
    
    pub async fn has_waiting_clients(&self, key: &str) -> bool {
        let waiting_clients = self.waiting_clients.read().await;
        waiting_clients.get(key).map(|v| !v.is_empty()).unwrap_or(false)
    }
    
    pub async fn wait_for_key(&self, notify: Arc<Notify>, timeout_duration: Option<Duration>) -> bool {
        if let Some(duration) = timeout_duration {
            timeout(duration, notify.notified()).await.is_ok()
        } else {
            notify.notified().await;
            true
        }
    }
}