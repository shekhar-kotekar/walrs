use std::collections::HashMap;

use bincode::{Decode, Encode};
use commons::models::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Encode, Decode)]
pub struct NodeInfo {
    pub registed_topics: Vec<String>,
    pub address: String,
}

impl NodeInfo {
    pub fn new(address: String) -> Self {
        Self {
            registed_topics: Vec::new(),
            address,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterInfo {
    // key: node address
    pub nodes: HashMap<String, NodeInfo>,
}

impl ClusterInfo {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }
    pub fn add_node(&mut self, node_info: NodeInfo) {
        self.nodes.insert(node_info.address.clone(), node_info);
    }

    pub fn set_peers(&mut self, nodes: Vec<String>) {
        for node in nodes {
            self.add_node(NodeInfo::new(node));
        }
    }
}

#[derive(Debug)]
pub enum PartitionWriterResponse {
    MessagesPersisted { count: u8 },
}

#[derive(Debug)]
pub enum PartitionReaderResponse {
    MessagesRead { messages: Vec<Message> },
    InternalError { message: String },
}

#[derive(Debug)]
pub enum PartitionCommand {
    WriteMessages {
        messages: Vec<Message>,
        tx: oneshot::Sender<PartitionWriterResponse>,
    },
    FetchMessages {
        topic_name: String,
        tx: oneshot::Sender<PartitionReaderResponse>,
    },
}

#[derive(Debug, Encode, Decode)]
pub enum PartitionRole {
    Leader { followers: HashMap<String, usize> },
    Follower { leader_address: String },
}

#[derive(Debug, Encode, Decode)]
pub enum CommandToPeer {
    CreatePartitionWriter {
        topic_name: String,
        partition_number: u8,
        role: PartitionRole,
    },
    Heartbeat {
        peer_listener_address: String,
        broker_status: NodeInfo,
    },
}

#[derive(Debug, Encode, Decode)]
pub enum PeerResponse {
    PartitionWriterCreated,
    HeartbeatReceived,
    Error { message: String },
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct NodeConfig {
    pub ip: String,
    pub port: u16,
    pub peers: Vec<String>,
    pub heartbeat_interval_ms: u16,
    pub mpsc_queue_size: usize,
    pub base_path_for_data: String,
}

pub struct NodeConfigBuilder {
    ip: Option<String>,
    port: u16,
    heartbeat_interval_ms: u16,
    mpsc_max_queue_size: usize,
    base_path_for_data: String,
    peers: Vec<String>,
}

impl NodeConfigBuilder {
    pub fn from_yaml_file(path: &str) -> Result<Self, String> {
        tracing::info!("Reading config from file: {}", path);
        let file = std::fs::File::open(path)
            .map_err(|e| format!("Failed to open node config YAML file: {}", e))?;
        let config: NodeConfig = serde_yml::from_reader(file)
            .map_err(|e| format!("Failed to read broker config from YAML file: {}", e))?;
        Ok(Self {
            ip: None,
            port: config.port,
            heartbeat_interval_ms: config.heartbeat_interval_ms,
            mpsc_max_queue_size: config.mpsc_queue_size,
            base_path_for_data: config.base_path_for_data,
            peers: config.peers,
        })
    }

    pub fn ip(mut self, ip: String) -> Self {
        self.ip = Some(ip);
        self
    }

    pub fn build(self) -> NodeConfig {
        NodeConfig {
            ip: self.ip.unwrap_or("0.0.0.0".to_string()),
            port: self.port,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            mpsc_queue_size: self.mpsc_max_queue_size,
            base_path_for_data: self.base_path_for_data,
            peers: self.peers,
        }
    }
}
