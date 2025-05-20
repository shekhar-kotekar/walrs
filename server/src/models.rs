use common::models::{ClusterResponse, Message};
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

pub enum BrokerCommand {
    CreateNewTopic {
        topic_name: String,
        num_partitions: u8,
        retention_period_hours: u16,
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
}

pub enum BrokerResponse {
    TopicCreated { leader_address: String },
    TopicAlreadyExists,
    PartitionNotFound,
    PartitionManagerFound { tx: mpsc::Sender<PartitionCommand> },
}

impl BrokerResponse {
    pub fn to_cluster_response(&self) -> ClusterResponse {
        match self {
            BrokerResponse::TopicCreated { leader_address } => ClusterResponse::TopicCreated {
                leader_address: leader_address.clone(),
            },
            BrokerResponse::TopicAlreadyExists => ClusterResponse::TopicAlreadyExists,
            _ => ClusterResponse::InternalError {
                message: "Unknown broker response".to_string(),
            },
        }
    }
}
