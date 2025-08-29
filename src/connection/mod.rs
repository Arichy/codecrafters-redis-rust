use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use futures_util::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::time::{timeout, Instant};
use tokio_util::codec::Framed;

use crate::commands::{CommandDispatcher, CommandMessage};
use crate::message::{Message, MessageFramer, Array, BulkString, SimpleString, SimpleError};
use crate::store::Store;
use crate::server::Config;
use crate::replication::{ReplicationInfo, ReplicationState};
use crate::pubsub::PubSubManager;
use crate::blocking::BlockingCommandManager;

pub type MessageReader = Arc<Mutex<SplitStream<Framed<TcpStream, MessageFramer>>>>;
pub type MessageWriter = Arc<Mutex<SplitSink<Framed<TcpStream, MessageFramer>, Message>>>;

pub struct Connection {
    socket: Framed<TcpStream, MessageFramer>,
    peer_addr: SocketAddr,
    config: Arc<RwLock<Config>>,
    store: Arc<Store>,
    replication_info: Arc<ReplicationInfo>,
    replication_state: Arc<Mutex<ReplicationState>>,
    pubsub_manager: Arc<PubSubManager>,
    blocking_manager: Arc<BlockingCommandManager>,
    command_tx: broadcast::Sender<CommandMessage>,
    selected_db_index: Arc<RwLock<usize>>,
    transaction_queue: VecDeque<Message>,
    is_in_transaction: bool,
}

impl Connection {
    pub fn new(
        socket: Framed<TcpStream, MessageFramer>,
        peer_addr: SocketAddr,
        config: Arc<RwLock<Config>>,
        store: Arc<Store>,
        replication_info: Arc<ReplicationInfo>,
        replication_state: Arc<Mutex<ReplicationState>>,
        pubsub_manager: Arc<PubSubManager>,
        blocking_manager: Arc<BlockingCommandManager>,
        command_tx: broadcast::Sender<CommandMessage>,
    ) -> Self {
        Self {
            socket,
            peer_addr,
            config,
            store,
            replication_info,
            replication_state,
            pubsub_manager,
            blocking_manager,
            command_tx,
            selected_db_index: Arc::new(RwLock::new(0)),
            transaction_queue: VecDeque::new(),
            is_in_transaction: false,
        }
    }
    
    pub async fn run(mut self, is_slave: bool) -> Result<()> {
        // Perform handshake if slave
        if is_slave {
            self.perform_slave_handshake().await?;
        }
        
        // Extract fields we need before splitting the socket
        let peer_addr_str = self.peer_addr.to_string();
        let pubsub_manager = Arc::clone(&self.pubsub_manager);
        let blocking_manager = Arc::clone(&self.blocking_manager);
        
        let (message_writer, message_reader) = self.socket.split();
        let message_writer = Arc::new(Mutex::new(message_writer));
        let message_reader = Arc::new(Mutex::new(message_reader));
        
        let dispatcher = CommandDispatcher::new(
            Arc::clone(&self.config),
            Arc::clone(&self.store),
            Arc::clone(&self.replication_info),
            Arc::clone(&self.replication_state),
            Arc::clone(&self.pubsub_manager),
            Arc::clone(&self.blocking_manager),
            self.command_tx.clone(),
            Arc::clone(&self.selected_db_index),
        );
        
        loop {
            let message = {
                let mut reader = message_reader.lock().await;
                reader.next().await
            };
            
            if message.is_none() {
                // Client disconnected
                handle_disconnect(&peer_addr_str, &pubsub_manager, &blocking_manager).await;
                break;
            }
            
            let message = message.unwrap()?;
            
            // Special handling for slaves
            if is_slave {
                match &message {
                    Message::RDB(rdb_content) => {
                        // Load the RDB content into the store
                        let rdb_bytes = rdb_content.to_bytes();
                        self.store.load_from_rdb(&rdb_bytes).await?;
                        continue; // Skip normal message processing and offset tracking
                    }
                    _ => {
                        // For non-RDB messages, process normally and track offset
                        let response = handle_message_internal(
                            &mut self.transaction_queue,
                            &mut self.is_in_transaction,
                            &peer_addr_str,
                            message.clone(),
                            &dispatcher,
                            Arc::clone(&message_reader),
                            Arc::clone(&message_writer),
                            is_slave,
                        ).await?;
                        
                        // Update offset AFTER processing
                        let message_length = message.length()?;
                        let mut state = self.replication_state.lock().await;
                        state.offset += message_length;

                        if let Some(response) = response {
                            let mut writer = message_writer.lock().await;
                            writer.send(response).await?;
                        }
                        continue;
                    }
                }
            }
            
            // Normal processing for non-slaves
            let response = handle_message_internal(
                &mut self.transaction_queue,
                &mut self.is_in_transaction,
                &peer_addr_str,
                message,
                &dispatcher,
                Arc::clone(&message_reader),
                Arc::clone(&message_writer),
                is_slave,
            ).await?;
            
            if let Some(response) = response {
                let mut writer = message_writer.lock().await;
                writer.send(response).await?;
            }
        }
        
        Ok(())
    }
    
    pub async fn run_as_slave(self) -> Result<()> {
        // Run as slave (handshake will be performed inside run)
        self.run(true).await
    }
    

    
    async fn perform_slave_handshake(&mut self) -> Result<()> {
        // Send PING
        self.socket.send(Message::Array(Array {
            items: vec![Message::BulkString(BulkString {
                length: 4,
                string: "PING".to_string(),
            })],
        })).await?;
        
        // Receive PONG
        let _ = self.socket.next().await.unwrap()?;
        
        // Send REPLCONF listening-port
        let port = self.config.read().await.port.to_string();
        self.socket.send(Message::Array(Array {
            items: vec![
                Message::BulkString(BulkString {
                    length: 8,
                    string: "REPLCONF".to_string(),
                }),
                Message::BulkString(BulkString {
                    length: 14,
                    string: "listening-port".to_string(),
                }),
                Message::BulkString(BulkString {
                    length: port.len() as isize,
                    string: port,
                }),
            ],
        })).await?;
        
        // Receive OK
        let _ = self.socket.next().await.unwrap()?;
        
        // Send REPLCONF capa psync2
        self.socket.send(Message::Array(Array {
            items: vec![
                Message::BulkString(BulkString {
                    length: 8,
                    string: "REPLCONF".to_string(),
                }),
                Message::BulkString(BulkString {
                    length: 4,
                    string: "capa".to_string(),
                }),
                Message::BulkString(BulkString {
                    length: 6,
                    string: "psync2".to_string(),
                }),
            ],
        })).await?;
        
        // Receive OK
        let _ = self.socket.next().await.unwrap()?;
        
        // Send PSYNC
        self.socket.send(Message::Array(Array {
            items: vec![
                Message::BulkString(BulkString {
                    length: 5,
                    string: "PSYNC".to_string(),
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
        })).await?;
        
        // Receive FULLRESYNC response
        let _ = self.socket.next().await.unwrap()?;
        
        // Note: The RDB file will be sent after FULLRESYNC and will be 
        // handled in the main message loop
        
        Ok(())
    }
}

async fn handle_disconnect(
    peer_addr: &str,
    pubsub_manager: &Arc<PubSubManager>,
    blocking_manager: &Arc<BlockingCommandManager>,
) {
    // Clean up pub/sub subscriptions
    pubsub_manager.unsubscribe_all(peer_addr).await;
    
    // Clean up any blocking commands
    blocking_manager.remove_client(peer_addr).await;
}

async fn handle_message_internal(
    transaction_queue: &mut VecDeque<Message>,
    is_in_transaction: &mut bool,
    peer_addr: &str,
    message: Message,
    dispatcher: &CommandDispatcher,
    message_reader: MessageReader,
    message_writer: MessageWriter,
    is_slave: bool,
) -> Result<Option<Message>> {
        let commands = parse_message(&message);
        
        if commands.is_empty() {
            return Ok(None);
        }
        
        let cmd = commands[0].to_lowercase();
        
        // Handle transaction commands specially
        match cmd.as_str() {
            "multi" => {
                *is_in_transaction = true;
                transaction_queue.clear();
                return Ok(Some(Message::SimpleString(SimpleString {
                    string: "OK".to_string(),
                })));
            }
            "exec" => {
                if !*is_in_transaction {
                    return Ok(Some(Message::SimpleError(SimpleError {
                        string: "ERR EXEC without MULTI".to_string(),
                    })));
                }
                
                let mut responses = Vec::new();
                for queued_message in transaction_queue.iter() {
                    if let Some(response) = dispatcher.dispatch(
                        queued_message.clone(),
                        message_reader.clone(),
                        message_writer.clone(),
                        peer_addr.to_string(),
                        is_slave,
                    ).await? {
                        responses.push(response);
                    }
                }
                
                *is_in_transaction = false;
                transaction_queue.clear();
                
                return Ok(Some(Message::Array(Array {
                    items: responses,
                })));
            }
            "discard" => {
                if !*is_in_transaction {
                    return Ok(Some(Message::SimpleError(SimpleError {
                        string: "ERR DISCARD without MULTI".to_string(),
                    })));
                }
                
                *is_in_transaction = false;
                transaction_queue.clear();
                
                return Ok(Some(Message::SimpleString(SimpleString {
                    string: "OK".to_string(),
                })));
            }
            _ => {}
        }
        
        // If in transaction, queue command
        if *is_in_transaction {
            transaction_queue.push_back(message);
            return Ok(Some(Message::SimpleString(SimpleString {
                string: "QUEUED".to_string(),
            })));
        }
        
        // Dispatch regular command
        dispatcher.dispatch(
            message,
            message_reader,
            message_writer,
            peer_addr.to_string(),
            is_slave,
        ).await
    }

fn parse_message(message: &Message) -> Vec<&str> {
    match message {
        Message::Array(Array { items }) => items
            .iter()
            .filter_map(|item| match item {
                Message::BulkString(BulkString { string, .. }) => Some(string.as_str()),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}