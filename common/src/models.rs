use std::{io::Read, net::TcpStream};

use serde::{Deserialize, Serialize};

use crate::consumer::ConsumerResponse;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum AckLevel {
    None = 0,
    Leader = 1,
    All = 2,
}

impl From<u8> for AckLevel {
    fn from(value: u8) -> Self {
        match value {
            0 => AckLevel::None,
            1 => AckLevel::Leader,
            _ => AckLevel::All,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClientCommand {
    CreateTopic {
        topic_name: String,
        num_partitions: u8,
        retention_period_hours: u16,
    },
    RequestToConnect {
        client_type: ClientType,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClientType {
    Producer,
    Consumer { topic_name: String },
    Admin,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ClusterResponse {
    TopicCreated { leader_address: String },
    TopicAlreadyExists,
    TopicNotFound,
    InternalError { message: String },
    ConnectionAccepted,
    ConnectionRejected { reason: String },
    MessagesPersisted { count: u8 },
    ConsumerResponse(ConsumerResponse),
}

impl ClusterResponse {
    pub fn deserialize(stream: &mut TcpStream) -> Option<ClusterResponse> {
        let mut buffer = [0u8; 2048];
        let bytes_read = stream.read(&mut buffer).ok()?;
        let broker_response: ClusterResponse = bincode::deserialize(&buffer[..bytes_read]).ok()?;
        Some(broker_response)
    }
}
