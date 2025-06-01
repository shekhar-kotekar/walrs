use std::collections::HashMap;

use common::models::{ClusterResponse, Message, Topic};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandToPeer {
    CreatePartitionWriter {
        topic_name: String,
        partition_number: u8,
        role: PartitionWriterRole,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PeerResponse {
    PartitionWriterCreated,
    Error { message: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PartitionWriterRole {
    Leader,
    Follower,
}

pub fn to_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serialize(value).expect("Failed to serialize value")
}

pub fn from_bytes<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> T {
    bincode::deserialize(bytes).expect("Failed to deserialize value")
}

#[derive(Debug)]
pub enum PartitionWriterResponse {
    MessagesPersisted { count: u8 },
    // InternalError { message: String },
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
    ReadMessages {
        topic_name: String,
        tx: oneshot::Sender<PartitionReaderResponse>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    broker_status: BrokerInfo,
    timestamp: u64,
}

impl Heartbeat {
    pub fn new(broker_status: BrokerInfo) -> Self {
        Heartbeat {
            broker_status,
            timestamp: Self::current_timestamp(),
        }
    }

    fn current_timestamp() -> u64 {
        // Get the current timestamp in milliseconds
        let now = std::time::SystemTime::now();
        now.duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub address: String,
    pub partition_leaders: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub brokers: HashMap<String, BrokerInfo>,
    pub topics_in_cluster: Vec<String>,
}

pub enum CommandToBroker {
    CreateNewTopic {
        topic: Topic,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
    CreatePartitionWriter {
        topic_name: String,
        partition_number: u8,
        broker_tx: oneshot::Sender<BrokerResponse>,
        role: PartitionWriterRole,
    },
    GetPartitionWriter {
        topic_name: String,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
    GetPartitionReader {
        topic_name: String,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
    Heartbeat {
        sender_address: String,
        message: Heartbeat,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
    RegisterPeer {
        peer_info: BrokerInfo,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
    GetStatus {
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
}

#[derive(Debug)]
pub enum BrokerResponse {
    TopicCreated { partition_leaders: HashMap<u8, String> },
    PartitionWriterCreated,
    TopicAlreadyExists,
    PartitionNotFound,
    PartitionManagerFound { tx: mpsc::Sender<PartitionCommand> },
    TopicNotFound,
    HeartbeatReceived,
    BrokerError { message: String },
    PeerRegistered,
    Status { info: BrokerInfo },
}

impl BrokerResponse {
    pub fn to_cluster_response(&self) -> ClusterResponse {
        match self {
            BrokerResponse::TopicCreated { partition_leaders } => ClusterResponse::TopicCreated {
                partition_leaders: partition_leaders.clone(),
            },
            BrokerResponse::TopicAlreadyExists => ClusterResponse::TopicAlreadyExists,
            _ => ClusterResponse::InternalError {
                message: "Unknown broker response".to_string(),
            },
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct BrokerConfig {
    pub ip: String,
    pub port: u16,
    pub peer_listen_port: u16,
    pub peers: Vec<String>,
    pub heartbeat_interval_ms: u16,
    pub mpsc_queue_size: usize,
    pub base_path_for_data: String,
}

pub struct BrokerConfigBuilder {
    ip: Option<String>,
    port: u16,
    peer_listen_port: u16,
    heartbeat_interval: u16,
    mpsc_max_queue_size: usize,
    base_path_for_data: String,
    peers: Vec<String>,
}

impl BrokerConfigBuilder {
    pub fn from_yaml_file(path: &str) -> Result<Self, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("Failed to open broker config YAML file: {}", e))?;
        let config: BrokerConfig =
            serde_yml::from_reader(file).map_err(|e| format!("Failed to read broker config from YAML file: {}", e))?;
        Ok(Self {
            ip: None,
            port: config.port,
            peer_listen_port: config.peer_listen_port,
            heartbeat_interval: config.heartbeat_interval_ms,
            mpsc_max_queue_size: config.mpsc_queue_size,
            base_path_for_data: config.base_path_for_data,
            peers: config.peers,
        })
    }

    pub fn ip(mut self, ip: String) -> Self {
        self.ip = Some(ip);
        self
    }

    // pub fn port(mut self, port: u16) -> Self {
    //     self.port = port;
    //     self
    // }

    // pub fn heartbeat_interval(mut self, interval: u8) -> Self {
    //     self.heartbeat_interval = interval;
    //     self
    // }

    pub fn mpsc_max_queue_size(mut self, size: usize) -> Self {
        self.mpsc_max_queue_size = size;
        self
    }

    pub fn base_path_for_data(mut self, path: String) -> Self {
        self.base_path_for_data = path;
        self
    }

    pub fn build(self) -> BrokerConfig {
        BrokerConfig {
            ip: self.ip.unwrap_or("0.0.0.0".to_string()),
            port: self.port,
            peer_listen_port: self.peer_listen_port,
            heartbeat_interval_ms: self.heartbeat_interval,
            mpsc_queue_size: self.mpsc_max_queue_size,
            base_path_for_data: self.base_path_for_data,
            peers: self.peers,
        }
    }
}
