use std::collections::HashMap;

use commons::models::{BrokerInfo, Message};
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum PartitionWriterResponse {
    MessagesPersisted { count: u8 },
    Error { message: String },
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

#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub self_info: BrokerInfo,
    // key: Broker address
    pub peers: HashMap<String, BrokerInfo>,
}

impl ClusterInfo {
    pub fn new() -> Self {
        Self {
            self_info: BrokerInfo::new("".to_string()), // Placeholder, will be set later
            peers: HashMap::new(),
        }
    }
    pub fn add_peer(&mut self, broker_info: BrokerInfo) {
        self.peers.insert(broker_info.address.clone(), broker_info);
    }
}
