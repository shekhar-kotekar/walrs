use std::collections::HashMap;

use bincode::{Decode, Encode};

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub struct Message {
    pub payload: Vec<u8>,
    pub key: Option<String>,
    pub headers: HashMap<String, String>,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum AdminCommand {
    CreateTopic { topic: Topic },
    GetTopicInfo { topic_name: String },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum ProducerCommand {
    WriteMessages {
        topic: String,
        messages: Vec<Message>,
    },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum ConsumerCommand {
    FetchMessages { topic: String, offset: Option<u64> },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum WalrsCommand {
    Admin(AdminCommand),
    Producer(ProducerCommand),
    Consumer(ConsumerCommand),
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum WalrsResponse {
    Admin(AdminResponse),
    Producer(ProducerResponse),
    Consumer(ConsumerResponse),
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum ConsumerResponse {
    MessagesFetched { messages: Vec<Message> },
    Error(String),
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum AdminResponse {
    TopicInfo { topic: Topic },
    Error(String),
    RequestAccepted,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum ProducerResponse {
    MessagesSent,
    Error(String),
    RequestAccepted,
    MessagesPersisted { count: u8 },
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum AckLevel {
    // Acknowledgment sent immediately after the leader has written the messages to its Write Ahead Log (WAL)
    Leader,
    // Acknowledgment sent when majority of followers have written the messages to their WALs
    // Majority is defined as (N/2) + 1, where N is the number of followers
    Majority,
    // Acknowledgment sent when all followers have written the messages to their WALs
    All,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub enum TopicStatus {
    ReadyToServe,
    CreationInProgress,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct Topic {
    pub name: String,
    pub num_partitions: u8,
    pub replication_factor: u8,
    pub retention_period_minutes: u16,
    pub ack_level: AckLevel,
    pub partitions: Vec<PartitionInfo>,
    pub status: TopicStatus,
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
            partitions: Vec::new(),
            status: TopicStatus::CreationInProgress,
        };
        new_topic.check_constraints().unwrap();
        Ok(new_topic)
    }

    pub fn add_partition(&mut self, partition: PartitionInfo) {
        self.partitions.push(partition);
    }

    pub fn add_partitions(&mut self, partitions: Vec<PartitionInfo>) {
        self.partitions.extend(partitions);
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

// #[derive(Debug, Clone, Encode, Decode)]
// pub struct TopicMetadata {
//     pub name: String,
//     pub replication_factor: u8,
//     pub retention_period_minutes: u16,
//     pub ack_level: AckLevel,
//     pub partitions: Vec<PartitionInfo>,
// }

#[derive(Debug, Clone, Encode, Decode, PartialEq)]
pub struct PartitionInfo {
    pub number: u8,
    pub leader_address: String,
    pub follower_addresses: Vec<String>,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_message_serialization() {
        let message = Message {
            payload: vec![1, 2, 3],
            key: Some("key".into()),
            headers: HashMap::new(),
        };
        let serialized_message =
            bincode::encode_to_vec(&message, bincode::config::standard()).unwrap();

        let (deserialized_message, decoded_length): (Message, usize) =
            bincode::decode_from_slice(&serialized_message, bincode::config::standard()).unwrap();

        assert_eq!(message, deserialized_message);
        assert_eq!(serialized_message.len(), decoded_length);
        assert_eq!(decoded_length, serialized_message.len());
    }

    #[test]
    fn test_command_serialization() {
        let topic_to_create = Topic::new(
            "test_topic".into(),
            None,
            None,
            Some(60000),
            Some(AckLevel::Leader),
        );

        let command = WalrsCommand::Admin(AdminCommand::CreateTopic {
            topic: topic_to_create.unwrap(),
        });

        let serialized_command =
            bincode::encode_to_vec(&command, bincode::config::standard()).unwrap();

        let (deserialized_command, decoded_length): (WalrsCommand, usize) =
            bincode::decode_from_slice(&serialized_command, bincode::config::standard()).unwrap();

        assert_eq!(command, deserialized_command);
        assert_eq!(serialized_command.len(), decoded_length);
        assert_eq!(decoded_length, serialized_command.len());

        // if let WalrsCommand::Admin(AdminCommand::CreateTopic {
        //     name,
        //     num_partitions,
        //     replication_factor,
        //     retention_period_minutes: retention_period_ms,
        // }) = deserialized_command
        // {
        //     assert_eq!(name, "test_topic");
        //     assert_eq!(num_partitions, 3);
        //     assert_eq!(replication_factor, 2);
        //     assert_eq!(retention_period_ms, Some(60000));
        // } else {
        //     panic!("Deserialized command is not of type AdminCommand::CreateTopic");
        // }
    }
}
