use std::collections::HashMap;
use std::fmt::Display;

use common::models::{ClusterResponse, Message, Topic};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PartitionRole {
    Leader { followers: HashMap<String, usize> },
    Follower { leader_address: String },
}

impl Display for PartitionRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionRole::Leader { followers: _ } => write!(f, "leader"),
            PartitionRole::Follower { leader_address: _ } => write!(f, "follower"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandToPeer {
    CreatePartitionWriter {
        topic_name: String,
        partition_number: u8,
        role: PartitionRole,
    },
    Heartbeat {
        peer_listener_address: String,
        broker_status: BrokerInfo,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PeerResponse {
    PartitionWriterCreated,
    HeartbeatReceived,
    Error { message: String },
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
    ReadMessages {
        topic_name: String,
        tx: oneshot::Sender<PartitionReaderResponse>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub registed_topics: Vec<String>,
}

impl BrokerInfo {
    pub fn new() -> Self {
        Self {
            registed_topics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterInfo {
    // key: broker address, value: BrokerInfo
    pub brokers: HashMap<String, BrokerInfo>,
}

impl ClusterInfo {
    pub fn new() -> Self {
        Self {
            brokers: HashMap::new(),
        }
    }
    pub fn with_peers(mut self, peers: Vec<String>) -> Self {
        self.brokers = peers
            .into_iter()
            .map(|peer| {
                let broker_info = BrokerInfo {
                    registed_topics: Vec::new(),
                };
                (peer, broker_info)
            })
            .collect();
        self
    }
    pub fn add_broker(&mut self, broker_address: String, broker_info: BrokerInfo) {
        self.brokers.insert(broker_address, broker_info);
    }
}

#[derive(Debug)]
pub enum CommandToBroker {
    CreateNewTopic {
        topic: Topic,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
    CreatePartitionWriter {
        topic_name: String,
        partition_number: u8,
        role: PartitionRole,
        broker_tx: oneshot::Sender<BrokerResponse>,
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
        sender_status: BrokerInfo,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
    RegisterPeer {
        peer_info: BrokerInfo,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
    GetTopicMetadata {
        topic_name: String,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
    GetPartitionLeaders {
        topics: Vec<String>,
        broker_tx: oneshot::Sender<BrokerResponse>,
    },
}

#[derive(Debug)]
pub enum BrokerResponse {
    TopicCreated {
        topic_metadata: common::models::TopicMetadata,
    },
    PartitionWriterCreated,
    TopicAlreadyExists,
    PartitionManagerFound {
        tx: mpsc::Sender<PartitionCommand>,
    },
    TopicNotFound,
    HeartbeatReceived,
    BrokerError {
        message: String,
    },
    PeerRegistered,
    TopicMetadata {
        metadata: common::models::TopicMetadata,
    },
    PartitionLeaders {
        partition_leaders: HashMap<String, Vec<String>>,
    },
}

impl BrokerResponse {
    pub fn to_cluster_response(&self) -> ClusterResponse {
        match self {
            BrokerResponse::TopicCreated { topic_metadata } => ClusterResponse::TopicCreated {
                topic_metadata: topic_metadata.clone(),
            },
            BrokerResponse::TopicAlreadyExists => ClusterResponse::TopicAlreadyExists,
            BrokerResponse::BrokerError { message } => ClusterResponse::InternalError {
                message: message.clone(),
            },
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
    pub peer_listener_port: u16,
    pub peers: Vec<String>,
    pub heartbeat_interval_ms: u16,
    pub mpsc_queue_size: usize,
    pub base_path_for_data: String,
}

pub struct BrokerConfigBuilder {
    ip: Option<String>,
    port: u16,
    peer_listener_port: u16,
    heartbeat_interval_ms: u16,
    mpsc_max_queue_size: usize,
    base_path_for_data: String,
    peers: Vec<String>,
}

impl BrokerConfigBuilder {
    pub fn from_yaml_file(path: &str) -> Result<Self, String> {
        tracing::info!("Reading config from file: {}", path);
        let file = std::fs::File::open(path)
            .map_err(|e| format!("Failed to open broker config YAML file: {}", e))?;
        let config: BrokerConfig = serde_yml::from_reader(file)
            .map_err(|e| format!("Failed to read broker config from YAML file: {}", e))?;
        Ok(Self {
            ip: None,
            port: config.port,
            peer_listener_port: config.peer_listener_port,
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

    pub fn build(self) -> BrokerConfig {
        BrokerConfig {
            ip: self.ip.unwrap_or("0.0.0.0".to_string()),
            port: self.port,
            peer_listener_port: self.peer_listener_port,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            mpsc_queue_size: self.mpsc_max_queue_size,
            base_path_for_data: self.base_path_for_data,
            peers: self.peers,
        }
    }
}
