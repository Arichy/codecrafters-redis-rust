use anyhow::Result;
use tokio::time::{timeout, Duration, Instant};
use crate::message::{Message, Array, BulkString, Integer, SimpleError};
use crate::commands::{CommandContext, CommandMessage};
use crate::get_param;

pub async fn handle(params: &[&str], ctx: &CommandContext) -> Result<Option<Message>> {
    if params.len() < 2 {
        return Ok(Some(Message::SimpleError(SimpleError {
            string: "WAIT requires 2 arguments".to_string(),
        })));
    }
    
    let numreplicas: usize = params[0].parse()
        .map_err(|_| anyhow::anyhow!("Invalid numreplicas"))?;
    let timeout_ms: u64 = params[1].parse()
        .map_err(|_| anyhow::anyhow!("Invalid timeout"))?;
    
    // Check if we have any replicas connected
    let (total_replica_count, current_offset) = {
        let state = ctx.replication_state.lock().await;
        (state.total_replica_count, state.offset)
    };
    
    if total_replica_count == 0 {
        // No replicas connected, return 0 immediately
        return Ok(Some(Message::Integer(Integer {
            value: 0,
        })));
    }
    
    // If no commands have been sent (offset is 0), all replicas are already synchronized
    if current_offset == 0 {
        return Ok(Some(Message::Integer(Integer { value: total_replica_count as i64 })));
    }
    
    // Send REPLCONF GETACK to all replicas
    let get_ack_msg = Message::Array(Array {
        items: vec![
            Message::BulkString(BulkString {
                length: 8,
                string: "REPLCONF".to_string(),
            }),
            Message::BulkString(BulkString {
                length: 6,
                string: "GETACK".to_string(),
            }),
            Message::BulkString(BulkString {
                length: 1,
                string: "*".to_string(),
            }),
        ],
    });
    
    // Reset acks and set expected offset to current master offset
    let expected_offset = {
        let mut state = ctx.replication_state.lock().await;
        state.reset_acks();
        let current_offset = state.offset;
        state.expected_offset = current_offset;
        current_offset
    };
    
    // Send GETACK to all replicas
    ctx.command_tx.send(CommandMessage {
        peer_addr: ctx.peer_addr.clone(),
        message: get_ack_msg,
    })?;
    
    // Wait for acknowledgments
    let start_time = Instant::now();
    let timeout_duration = Duration::from_millis(timeout_ms);
    
    loop {
        let acked_count = {
            let state = ctx.replication_state.lock().await;
            state.acked_replica_count
        };
        
        if acked_count >= numreplicas {
            return Ok(Some(Message::Integer(Integer {
                value: numreplicas as i64,
            })));
        }
        
        let remaining_time = timeout_duration
            .checked_sub(start_time.elapsed())
            .unwrap_or(Duration::from_millis(0));
        
        if remaining_time.is_zero() {
            let state = ctx.replication_state.lock().await;
            let count = std::cmp::min(numreplicas, state.acked_replica_count);
            return Ok(Some(Message::Integer(Integer {
                value: count as i64,
            })));
        }
        
        let wait_notify = {
            let state = ctx.replication_state.lock().await;
            state.wait_notify.clone()
        };
        
        if timeout(remaining_time, wait_notify.notified()).await.is_err() {
            let state = ctx.replication_state.lock().await;
            let count = std::cmp::min(numreplicas, state.acked_replica_count);
            return Ok(Some(Message::Integer(Integer {
                value: count as i64,
            })));
        }
    }
}