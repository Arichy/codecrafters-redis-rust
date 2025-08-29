use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub enum Role {
    Master,
    Slave(SocketAddr),
}

#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    pub role: Role,
    pub master_replid: String,
    pub master_repl_offset: isize,
}

#[derive(Debug, Clone)]
pub struct ReplicationState {
    pub offset: usize,
    pub total_replica_count: usize,
    pub acked_replica_count: usize,
    pub expected_offset: usize,
    pub wait_notify: Arc<Notify>,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            offset: 0,
            total_replica_count: 0,
            acked_replica_count: 0,
            expected_offset: 0,
            wait_notify: Arc::new(Notify::new()),
        }
    }
    
    pub fn add_replica(&mut self) {
        self.total_replica_count += 1;
        self.acked_replica_count += 1;
    }
    
    pub fn remove_replica(&mut self) {
        if self.total_replica_count > 0 {
            self.total_replica_count -= 1;
        }
    }
    
    pub fn reset_acks(&mut self) {
        self.acked_replica_count = 0;
    }
    
    pub fn increment_ack(&mut self) {
        self.acked_replica_count += 1;
        self.wait_notify.notify_waiters();
    }
}