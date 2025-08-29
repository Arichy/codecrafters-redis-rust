use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use futures_util::SinkExt;

use crate::connection::MessageWriter;
use crate::message::{Message, Array, BulkString, Integer};

pub struct PubSubManager {
    // channel_name -> (client_addr -> MessageWriter)
    channels: Arc<RwLock<HashMap<String, HashMap<String, MessageWriter>>>>,
    // client_addr -> set of subscribed channels
    client_channels: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl PubSubManager {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            client_channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn subscribe(
        &self,
        client_addr: &str,
        channel: &str,
        writer: MessageWriter,
    ) -> usize {
        let mut channels = self.channels.write().await;
        let mut client_channels = self.client_channels.write().await;
        
        // Add to channel subscribers
        channels
            .entry(channel.to_string())
            .or_insert_with(HashMap::new)
            .insert(client_addr.to_string(), writer);
        
        // Track client's subscriptions
        let client_subs = client_channels
            .entry(client_addr.to_string())
            .or_insert_with(HashSet::new);
        client_subs.insert(channel.to_string());
        
        client_subs.len()
    }
    
    pub async fn unsubscribe(&self, client_addr: &str, channel: &str) -> usize {
        let mut channels = self.channels.write().await;
        let mut client_channels = self.client_channels.write().await;
        
        // Remove from channel subscribers
        if let Some(channel_subs) = channels.get_mut(channel) {
            channel_subs.remove(client_addr);
            if channel_subs.is_empty() {
                channels.remove(channel);
            }
        }
        
        // Update client's subscriptions
        if let Some(client_subs) = client_channels.get_mut(client_addr) {
            client_subs.remove(channel);
            if client_subs.is_empty() {
                client_channels.remove(client_addr);
                return 0;
            }
            return client_subs.len();
        }
        
        0
    }
    
    pub async fn unsubscribe_all(&self, client_addr: &str) {
        let mut channels = self.channels.write().await;
        let mut client_channels = self.client_channels.write().await;
        
        // Get all channels this client is subscribed to
        if let Some(client_subs) = client_channels.remove(client_addr) {
            // Remove client from all channels
            for channel in client_subs {
                if let Some(channel_subs) = channels.get_mut(&channel) {
                    channel_subs.remove(client_addr);
                    if channel_subs.is_empty() {
                        channels.remove(&channel);
                    }
                }
            }
        }
    }
    
    pub async fn publish(&self, channel: &str, message: &str) -> Result<usize, anyhow::Error> {
        let channels = self.channels.read().await;
        
        if let Some(subscribers) = channels.get(channel) {
            let mut count = 0;
            
            for (_, writer) in subscribers.iter() {
                let mut writer = writer.lock().await;
                let send_result = writer.send(Message::Array(Array {
                    items: vec![
                        Message::BulkString(BulkString {
                            length: 7,
                            string: "message".to_string(),
                        }),
                        Message::BulkString(BulkString {
                            length: channel.len() as isize,
                            string: channel.to_string(),
                        }),
                        Message::BulkString(BulkString {
                            length: message.len() as isize,
                            string: message.to_string(),
                        }),
                    ],
                })).await;
                
                if send_result.is_ok() {
                    count += 1;
                }
            }
            
            Ok(count)
        } else {
            Ok(0)
        }
    }
    
    pub async fn is_subscribed(&self, client_addr: &str) -> bool {
        let client_channels = self.client_channels.read().await;
        client_channels.contains_key(client_addr)
    }
    
    pub async fn get_subscription_count(&self, client_addr: &str) -> usize {
        let client_channels = self.client_channels.read().await;
        client_channels.get(client_addr).map(|s| s.len()).unwrap_or(0)
    }
}