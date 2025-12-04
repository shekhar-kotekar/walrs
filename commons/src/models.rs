use std::{collections::HashMap, str::FromStr, time::SystemTime};

use bincode::{Decode, Encode};
use serde::Serialize;

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct PartitionInfo {
    pub number: u8,
    pub leader_address: String,
    pub followers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum AckLevel {
    // TODO: Enable none later
    // None,
    // Acknowledgment sent immediately after the leader has written the messages to its Write Ahead Log (WAL)
    Leader,
    // Acknowledgment sent when majority of followers have written the messages to their WALs
    // Majority is defined as (N/2) + 1, where N is the number of followers
    Majority,
    // Acknowledgment sent when all followers have written the messages to their WALs
    All,
}

impl FromStr for AckLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            // "none" | "0" => Ok(AckLevel::None),
            "leader" | "1" => Ok(AckLevel::Leader),
            "all" | "-1" => Ok(AckLevel::All),
            _ => Err(format!(
                "Invalid ack level: '{}'. Valid options: none, leader, all",
                s
            )),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub enum TopicStatus {
    ReadyToServe,
    CreationInProgress,
    NotReady,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct Topic {
    pub name: String,
    pub replication_factor: u8,
    pub retention_period_minutes: u16,
    pub ack_level: AckLevel,
    pub partitions: Vec<PartitionInfo>,
    pub status: TopicStatus,
}

impl Topic {
    pub const DEFAULT_NUM_PARTITIONS: u8 = 3;
    const DEFAULT_REPLICATION_FACTOR: u8 = 3;
    const DEFAULT_RETENTION_PERIOD_MINUTES: u16 = 60;

    pub fn new(
        name: String,
        num_partitions: Option<u8>,
        replication_factor: Option<u8>,
        retention_period_minutes: Option<u16>,
        ack_level: Option<AckLevel>,
    ) -> Result<Self, std::io::Error> {
        let num_partitions = num_partitions.unwrap_or(Self::DEFAULT_NUM_PARTITIONS);
        let replication_factor = replication_factor.unwrap_or(Self::DEFAULT_REPLICATION_FACTOR);
        let retention_period_minutes =
            retention_period_minutes.unwrap_or(Self::DEFAULT_RETENTION_PERIOD_MINUTES);
        let ack_level = ack_level.unwrap_or(AckLevel::Leader);

        Ok(Topic {
            name,
            replication_factor,
            retention_period_minutes,
            ack_level,
            partitions: Vec::with_capacity(num_partitions as usize),
            status: TopicStatus::NotReady,
        })
    }
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum AdminCommand {
    CreateTopic {
        name: String,
        num_partitions: Option<u8>,
        replication_factor: Option<u8>,
        retention_period_minutes: Option<u16>,
        ack_level: Option<AckLevel>,
    },
    GetTopicInfo {
        topic_names: Vec<String>,
    },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum AdminResponse {
    TopicInfo { topics: Vec<Topic> },
    Error(String),
    RequestAccepted,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum WalrsCommand {
    Admin(AdminCommand),
    Peer(PeerCommand),
    Producer(ProducerCommand),
    Consumer(ConsumerCommand),
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum ConsumerCommand {
    FetchMessages {
        topic: String,
        partition_number: u8,
        offset: Option<u64>,
    },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum ProducerCommand {
    WriteMessages {
        topic: String,
        partition_number: u8,
        messages: Vec<Message>,
    },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum WalrsResponse {
    Admin(AdminResponse),
    Peer(PeerResponse),
    Producer(ProducerResponse),
    Consumer(ConsumerResponse),
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum ProducerResponse {
    Error { message: String },
    MessagesPersisted { count: u8 },
    RequestAccepted,
    Redirect { leader_address: String },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum ConsumerResponse {
    MessagesFetched { messages: Vec<Message> },
    Error(String),
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum PartitionRole {
    // key: follower address, value: partition number
    Leader { followers: Vec<String> },
    Follower { leader_address: String },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub struct BrokerInfo {
    pub registered_topics: HashMap<String, (Topic, TopicStatus)>,
    pub address: String,
}

impl BrokerInfo {
    pub fn new(address: String) -> Self {
        Self {
            registered_topics: HashMap::new(),
            address,
        }
    }
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum PeerCommand {
    CreatePartition {
        topic_name: String,
        partition_number: u8,
        role: PartitionRole,
    },
    Heartbeat {
        broker_info: BrokerInfo,
    },
    SyncMessages {
        topic_name: String,
        partition_number: u8,
        messages: Vec<Message>,
    },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum PeerResponse {
    PartitionCreated {
        topic_name: String,
        partition_number: u8,
    },
    Error {
        message: String,
    },
    MessagesSynced {
        topic_name: String,
        partition_number: u8,
        count: u8,
    },
    HeartbeatAcknowledged,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Serialize)]
pub struct Message {
    //TODO: Make these fields private so that
    // they can only be accessed through the constructor and we can validate each field
    pub payload: Vec<u8>,
    pub key: Option<String>,
    pub headers: HashMap<String, Vec<u8>>,
}

impl Message {
    pub fn new(payload: Vec<u8>) -> Self {
        let mut headers = HashMap::new();
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Invalid timestamp. Time went backwards.")
            .as_millis();
        headers.insert("created_at".into(), vec![timestamp as u8]);
        Self {
            payload,
            key: None,
            headers,
        }
    }
    pub fn with_key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }
    pub fn with_header(mut self, key: String, value: Vec<u8>) -> Self {
        self.headers.insert(key, value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let message = Message::new(vec![1, 2, 3]);
        assert_eq!(message.payload, vec![1, 2, 3]);
        assert_eq!(message.key, None);
        assert_eq!(message.headers.len(), 1);
        assert_eq!(message.headers.contains_key("created_at"), true);
    }

    #[test]
    fn test_message_with_key() {
        let message = Message::new(vec![1, 2, 3]).with_key("test_key".into());
        assert_eq!(message.key, Some("test_key".into()));
        assert_eq!(message.headers.len(), 1);
        assert_eq!(message.headers.contains_key("created_at"), true);
    }

    #[test]
    fn test_message_with_header() {
        let message = Message::new(vec![1, 2, 3]).with_header("test_header".into(), vec![4, 5, 6]);
        assert_eq!(message.headers.len(), 2);
        assert_eq!(message.headers.get("test_header"), Some(&vec![4, 5, 6]));
        assert_eq!(message.headers.contains_key("created_at"), true);
    }

    #[test]
    fn test_message_with_key_and_headers() {
        let message = Message::new(vec![1, 2, 3])
            .with_key("test_key".into())
            .with_header("header_1".into(), vec![4, 5, 6])
            .with_header("header_2".into(), vec![7, 8, 9]);

        assert_eq!(message.key, Some("test_key".into()));
        assert_eq!(message.headers.len(), 3);
        assert_eq!(message.headers.get("header_1"), Some(&vec![4, 5, 6]));
        assert_eq!(message.headers.get("header_2"), Some(&vec![7, 8, 9]));
        assert_eq!(message.headers.contains_key("created_at"), true);
    }
}
