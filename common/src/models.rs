use serde::{Deserialize, Serialize};

use crate::TWO_MB;

#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    pub data: [u8; TWO_MB],
    pub topic_name: String,
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
    TopicCreated { leader_address: String },
    TopicAlreadyExists,
    TopicNotFound,
    ProducerAcknowledged { topic_name: String },
    ProducerNotAcknowledged { topic_name: String },
    ProducerStopAcknowledged { topic_name: String },
}
