use crate::consumer::ConsumerResponse;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Read, net::TcpStream};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ProducerCommand {
    WriteMessages {
        topic_name: String,
        messages: Vec<Message>,
    },
    GetPartitionLeaders {
        topics: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub key: Option<String>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClientCommand {
    CreateTopic { topic_details: Topic },
    RequestToConnect { client_type: ClientType },
    GetTopicMetadata { topic_name: String },
    GetPartitionLeaders { topics: Vec<String> },
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionWriterRole {
    Leader { follower_addresses: Vec<String> },
    Follower,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub replication_factor: u8,
    pub retention_period_minutes: u16,
    pub ack_level: AckLevel,
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub number: u8,
    pub leader_address: String,
    pub follower_addresses: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Topic {
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
    ) -> Result<Self, String> {
        let new_topic = Self {
            name,
            num_partitions: num_partitions.unwrap_or(3),
            replication_factor: replication_factor.unwrap_or(3),
            retention_period_minutes: retention_period_minutes.unwrap_or(60),
            ack_level: ack_level.unwrap_or(AckLevel::Leader),
        };
        new_topic.check_constraints().unwrap();
        Ok(new_topic)
    }

    fn check_constraints(&self) -> Result<(), String> {
        if self.num_partitions < 1 {
            return Err("Number of partitions must be at least 1".to_string());
        }
        if self.replication_factor < 1 || self.replication_factor > self.num_partitions {
            return Err(
                "Replication factor must be between 1 and the number of partitions".to_string(),
            );
        }
        if self.retention_period_minutes == 0 {
            return Err("Retention period must be greater than 0".to_string());
        }
        if self.name.is_empty() {
            return Err("Topic name cannot be empty".to_string());
        }
        if self.name.len() > 25 {
            return Err("Topic name cannot exceed 25 characters".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClusterResponse {
    TopicCreated {
        topic_metadata: TopicMetadata,
    },
    TopicAlreadyExists,
    TopicNotFound,
    InternalError {
        message: String,
    },
    ConnectionAccepted,
    ConnectionRejected {
        reason: String,
    },
    MessagesPersisted {
        count: u8,
    },
    ConsumerResponse(ConsumerResponse),
    Success {
        message: String,
    },
    TopicMetadata {
        metadata: TopicMetadata,
    },
    PartitionLeaders {
        leaders: HashMap<String, Vec<String>>,
    },
}

impl ClusterResponse {
    pub fn deserialize(stream: &mut TcpStream) -> Option<ClusterResponse> {
        let mut buffer = [0u8; 2048];
        let bytes_read = stream.read(&mut buffer).ok()?;
        let broker_response: ClusterResponse = bincode::deserialize(&buffer[..bytes_read]).ok()?;
        Some(broker_response)
    }
}
