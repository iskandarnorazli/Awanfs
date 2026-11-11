//! awan-storage/server/src/cluster.rs
//! Intra-cluster Gossip Heartbeat & Node Membership Discovery Protocol
//! Detects peer failures and signals node degradation after 3 missed intervals.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Healthy,
    Degraded,
    Dead,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: u32,
    pub addr: SocketAddr,
    pub last_seen: Instant,
    pub status: NodeStatus,
    pub missed_heartbeats: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatMessage {
    pub sender_id: u32,
    pub sequence: u64,
    pub term: u64,
    pub timestamp_ms: u64,
}

pub struct ClusterMembership {
    pub local_id: u32,
    pub heartbeat_interval: Duration,
    pub nodes: HashMap<u32, NodeInfo>,
    current_sequence: u64,
    current_term: u64,
}

impl ClusterMembership {
    pub fn new(local_id: u32, heartbeat_interval: Duration) -> Self {
        Self {
            local_id,
            heartbeat_interval,
            nodes: HashMap::new(),
            current_sequence: 0,
            current_term: 1,
        }
    }

    pub fn register_peer(&mut self, id: u32, addr: SocketAddr, now: Instant) {
        if id == self.local_id {
            return;
        }
        self.nodes.insert(
            id,
            NodeInfo {
                id,
                addr,
                last_seen: now,
                status: NodeStatus::Healthy,
                missed_heartbeats: 0,
            },
        );
    }

    pub fn generate_heartbeat(&mut self, now_ms: u64) -> HeartbeatMessage {
        self.current_sequence += 1;
        HeartbeatMessage {
            sender_id: self.local_id,
            sequence: self.current_sequence,
            term: self.current_term,
            timestamp_ms: now_ms,
        }
    }

    pub fn receive_heartbeat(&mut self, msg: &HeartbeatMessage, now: Instant) -> bool {
        if let Some(node) = self.nodes.get_mut(&msg.sender_id) {
            node.last_seen = now;
            node.missed_heartbeats = 0;
            if msg.term > self.current_term {
                self.current_term = msg.term;
            }
            true
        } else {
            false
        }
    }

    /// Evaluates peer heartbeats and transitions unresponsive nodes:
    /// >= 3 missed intervals -> Degraded
    /// >= 6 missed intervals -> Dead
    pub fn tick(&mut self, now: Instant) -> Vec<(u32, NodeStatus)> {
        let mut transitions = Vec::new();
        let interval = self.heartbeat_interval;

        for node in self.nodes.values_mut() {
            let elapsed = now.duration_since(node.last_seen);
            let missed = (elapsed.as_millis() / interval.as_millis().max(1)) as u32;
            node.missed_heartbeats = missed;

            let prev_status = node.status;
            let new_status = if missed >= 6 {
                NodeStatus::Dead
            } else if missed >= 3 {
                NodeStatus::Degraded
            } else {
                NodeStatus::Healthy
            };

            if new_status != prev_status {
                node.status = new_status;
                transitions.push((node.id, new_status));
            }
        }

        transitions
    }

    pub fn healthy_count(&self) -> usize {
        1 + self.nodes.values().filter(|n| n.status == NodeStatus::Healthy).count()
    }

    pub fn is_quorum_available(&self, required_quorum: usize) -> bool {
        self.healthy_count() >= required_quorum
    }
}
