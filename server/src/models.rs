use common::models::{ClusterResponse, Message, Topic};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum PartitionWriterResponse {
    MessagesPersisted { count: u8 },
    InternalError { message: String },
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
    lead_partition_count: u8,
    timestamp: u64,
}

impl Heartbeat {
    pub fn new(lead_partition_count: u8) -> Self {
        Heartbeat {
            lead_partition_count,
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
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize heartbeat")
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Failed to deserialize heartbeat")
    }
}

pub enum MainToBrokerCommand {
    CreateNewTopic {
        topic: Topic,
        broker_tx: oneshot::Sender<BrokerToMainResponse>,
    },
    GetPartitionWriter {
        topic_name: String,
        broker_tx: oneshot::Sender<BrokerToMainResponse>,
    },
    GetPartitionReader {
        topic_name: String,
        broker_tx: oneshot::Sender<BrokerToMainResponse>,
    },
    Heartbeat {
        sender_address: String,
        message: Heartbeat,
        broker_tx: oneshot::Sender<BrokerToMainResponse>,
    },
}

pub enum BrokerToMainResponse {
    TopicCreated { leader_address: String },
    TopicAlreadyExists,
    PartitionNotFound,
    PartitionManagerFound { tx: mpsc::Sender<PartitionCommand> },
    TopicNotFound,
    HeartbeatReceived,
}

impl BrokerToMainResponse {
    pub fn to_cluster_response(&self) -> ClusterResponse {
        match self {
            BrokerToMainResponse::TopicCreated { leader_address } => ClusterResponse::TopicCreated {
                leader_address: leader_address.clone(),
            },
            BrokerToMainResponse::TopicAlreadyExists => ClusterResponse::TopicAlreadyExists,
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
    pub heartbeat_interval: u8,
    pub mpsc_max_queue_size: usize,
    pub base_path_for_data: String,
}

pub struct BrokerConfigBuilder {
    ip: Option<String>,
    port: u16,
    heartbeat_interval: u8,
    mpsc_max_queue_size: usize,
    base_path_for_data: String,
}

impl BrokerConfigBuilder {
    pub fn from_yaml_file(path: &str) -> Result<Self, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("Failed to open broker config YAML file: {}", e))?;
        let config: BrokerConfig =
            serde_yml::from_reader(file).map_err(|e| format!("Failed to read broker config from YAML file: {}", e))?;
        Ok(Self {
            ip: None,
            port: config.port,
            heartbeat_interval: config.heartbeat_interval,
            mpsc_max_queue_size: config.mpsc_max_queue_size,
            base_path_for_data: config.base_path_for_data,
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
            heartbeat_interval: self.heartbeat_interval,
            mpsc_max_queue_size: self.mpsc_max_queue_size,
            base_path_for_data: self.base_path_for_data,
        }
    }
}
