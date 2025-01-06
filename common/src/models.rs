use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MessageBatch {
    pub topic_name: String,
    pub messages: Vec<Message>,
    pub ack_level: AcknowledgementLevel,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum AcknowledgementLevel {
    Leader = 0,
    Majority = 1,
    All,
}

impl From<u8> for AcknowledgementLevel {
    fn from(value: u8) -> Self {
        match value {
            0 => AcknowledgementLevel::Leader,
            1 => AcknowledgementLevel::Majority,
            _ => AcknowledgementLevel::All,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClientCommand {
    CreateTopic {
        topic_name: String,
        num_partitions: u8,
        retention_period_hours: u64,
    },
    RequestToProduce {
        topic_name: String,
    },
    RequestToStop {
        topic_name: String,
    },
}

#[derive(Serialize, Deserialize)]
pub enum BrokerResponse {
    TopicCreated {
        leader_address: String,
    },
    TopicAlreadyExists,
    TopicNotFound,
    ProducerAcknowledged {
        topic_name: String,
        partition_port: u16,
    },
    ProducerNotAcknowledged {
        topic_name: String,
    },
    ProducerStopAcknowledged {
        topic_name: String,
    },
    InternalError {
        message: String,
    },
}
