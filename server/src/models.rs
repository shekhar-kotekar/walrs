use std::collections::HashMap;

use bincode::{Decode, Encode};
use commons::models::Message;
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
    // pub fn with_peers(mut self, peers: Vec<String>) -> Self {
    //     self.nodes = peers
    //         .into_iter()
    //         .map(|peer| {
    //             let node_info = NodeInfo {
    //                 registed_topics: Vec::new(),
    //                 address: peer.clone(),
    //             };
    //             (peer, node_info)
    //         })
    //         .collect();
    //     self
    // }

    pub fn add_node(&mut self, node_info: NodeInfo) {
        self.nodes.insert(node_info.address.clone(), node_info);
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

// pub enum TopicManagerResponse {
//     TopicCreated {
//         topic: Topic,
//         // partition_zero_tx: mpsc::Sender<PartitionCommand>,
//     },
//     TopicCreationFailed {
//         topic_name: String,
//         error: String,
//     },
// }
