use crate::consumer::ConsumerResponse;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Read, net::TcpStream};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClientCommand {
    CreateTopic { topic_details: Topic },
    RequestToConnect { client_type: ClientType },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClientType {
    Producer,
    Consumer { topic_name: String },
    Admin,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AckLevel {
    // Acknowledgment sent immediately after the leader has written the messages to its Write Ahead Log (WAL)
    Leader,
    // Acknowledgment sent when majority of followers have written the messages to their WALs
    // Majority is defined as (N/2) + 1, where N is the number of followers
    Majority,
    // Acknowledgment sent when all followers have written the messages to their WALs
    All,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Topic {
    pub id: Option<u64>,
    pub name: String,
    pub num_partitions: u8,
    pub replication_factor: u8,
    pub retention_period_minutes: u16,
    pub ack_level: AckLevel,
}

impl Topic {
    pub fn new(
        name: String,
        num_partitions: Option<u8>,
        replication_factor: Option<u8>,
        retention_period_minutes: Option<u16>,
        ack_level: Option<AckLevel>,
    ) -> Self {
        Self {
            id: None,
            name,
            num_partitions: num_partitions.unwrap_or(3),
            replication_factor: replication_factor.unwrap_or(3),
            retention_period_minutes: retention_period_minutes.unwrap_or(48),
            ack_level: ack_level.unwrap_or(AckLevel::Leader),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ClusterResponse {
    TopicCreated { partition_leaders: HashMap<u8, String> },
    TopicAlreadyExists,
    TopicNotFound,
    InternalError { message: String },
    ConnectionAccepted,
    ConnectionRejected { reason: String },
    MessagesPersisted { count: u8 },
    ConsumerResponse(ConsumerResponse),
    Success { message: String },
}

impl ClusterResponse {
    pub fn deserialize(stream: &mut TcpStream) -> Option<ClusterResponse> {
        let mut buffer = [0u8; 2048];
        let bytes_read = stream.read(&mut buffer).ok()?;
        let broker_response: ClusterResponse = bincode::deserialize(&buffer[..bytes_read]).ok()?;
        Some(broker_response)
    }
}
