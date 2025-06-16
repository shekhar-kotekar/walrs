use std::{
    collections::{HashMap, hash_map::Entry},
    hash::{DefaultHasher, Hash, Hasher},
};

use crate::{
    models::{AdminCommand, AdminResponse, Message, ProducerCommand, ProducerResponse, Topic},
    send_and_receive_admin_command, send_and_receive_producer_command,
};

pub struct Producer {
    brokers: Vec<String>,
    // key: topic name, value: vector of messages to be sent to that topic
    buffer: HashMap<String, Vec<Message>>,
}

impl Producer {
    pub fn new(brokers: Vec<String>) -> Self {
        Producer {
            brokers,
            buffer: HashMap::new(),
        }
    }

    pub fn send(&mut self, topic: String, message: &Message) {
        //TODO: We are copying messages many times. We should optimize this.
        self.buffer.entry(topic).or_default().push(message.clone());
    }

    pub fn send_batch(&mut self, topic: String, messages: Vec<Message>) {
        self.buffer.entry(topic).or_default().extend(messages);
    }

    pub async fn flush(&mut self) -> Result<ProducerResponse, std::io::Error> {
        if self.buffer.is_empty() {
            tracing::warn!("Producer buffer is empty, nothing to flush.");
            Ok(ProducerResponse::Error {
                message: String::from("Buffer is empty"),
            })
        } else {
            let topic_info: HashMap<String, Topic> = self.get_topic_info().await?;
            let mapped_messages: HashMap<String, (String, Vec<Message>)> =
                self.map_messages_to_nodes(&topic_info);

            let mut total_message_persisted = 0;
            for (node_address, (topic_name, messages)) in mapped_messages {
                let result = self
                    .send_messages_to_node(&node_address, &topic_name, messages)
                    .await;
                match result {
                    ProducerResponse::MessagesPersisted { count } => {
                        total_message_persisted += count;
                    }
                    other => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "Failed to send messages to node {}: {:?}",
                                node_address, other
                            ),
                        ));
                    }
                }
            }
            self.buffer.clear();
            Ok(ProducerResponse::MessagesPersisted {
                count: total_message_persisted,
            })
        }
    }

    async fn send_messages_to_node(
        &self,
        node_address: &str,
        topic_name: &str,
        messages: Vec<Message>,
    ) -> ProducerResponse {
        let producer_command = ProducerCommand::WriteMessages {
            topic: topic_name.to_string(),
            messages: messages.clone(),
        };
        tracing::debug!(
            "Sending messages to node: {}, topic: {}, messages: {:?}",
            node_address,
            topic_name,
            messages
        );
        send_and_receive_producer_command(producer_command, node_address).await
    }

    fn map_messages_to_nodes(
        &self,
        topic_info: &HashMap<String, Topic>,
    ) -> HashMap<String, (String, Vec<Message>)> {
        // key is broker address, value is a key pair of (topic_name, vector of messages to be sent to that broker)
        let mut messages_mapped_to_nodes: HashMap<String, (String, Vec<Message>)> = HashMap::new();
        let mut hasher = DefaultHasher::new();
        for (topic, messages) in &self.buffer {
            // key: partition number, value: leader address
            let partition_leaders: HashMap<u8, String> = topic_info
                .get(topic)
                .unwrap()
                .partitions
                .iter()
                .map(|partition| (partition.number, partition.leader_address.clone()))
                .collect();

            tracing::debug!(
                "topic: {}, partition leaders: {:?}",
                topic,
                partition_leaders
            );

            let partition_count = topic_info.get(topic).unwrap().partitions.len() as u8;
            tracing::debug!("topic: {}, partition count: {}", topic, partition_count);

            for message in messages {
                // hash key of each message to determine the broker. If key is not found then randomly select a broker
                let partition_number = self.get_partition_number_for_message(
                    &message.key,
                    partition_count,
                    &mut hasher,
                );
                tracing::debug!(
                    "Message key: {:?}, partition number: {}",
                    message.key,
                    partition_number
                );
                // if mapping has no entry for this broker, create a new one and add tuple (topic_name, messages)
                let node_address = partition_leaders.get(&partition_number).unwrap();
                match messages_mapped_to_nodes.entry(node_address.clone()) {
                    Entry::Vacant(entry) => {
                        //TODO: We are copying messages many times. We should optimize this.
                        entry.insert((topic.clone(), vec![message.clone()]));
                    }
                    Entry::Occupied(mut occupied_entry) => {
                        //TODO: We are copying messages many times. We should optimize this.
                        occupied_entry.get_mut().1.push(message.clone());
                    }
                }
            }
        }
        messages_mapped_to_nodes
    }

    fn get_partition_number_for_message(
        &self,
        message_key: &Option<String>,
        topic_partition_count: u8,
        hasher: &mut DefaultHasher,
    ) -> u8 {
        match message_key {
            Some(key) => {
                key.hash(hasher);
                let hash = hasher.finish() as usize % topic_partition_count as usize;
                hash as u8
            }
            None => {
                // Randomly select a partition if key is not present
                // For time being we will use the first partition leader
                0
            }
        }
    }

    async fn get_topic_info(&self) -> Result<HashMap<String, Topic>, std::io::Error> {
        let get_topic_info_command = AdminCommand::GetTopicInfo {
            topic_names: self.buffer.keys().cloned().collect(),
        };
        match send_and_receive_admin_command(get_topic_info_command, &self.brokers[0]).await {
            AdminResponse::TopicInfo { topics } => {
                Ok(topics.into_iter().map(|t| (t.name.clone(), t)).collect())
            }
            AdminResponse::Error(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, err)),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected response type: {:?}", other),
            )),
        }
    }
}

#[cfg(test)]
mod should {
    use tracing_test::traced_test;

    use crate::models::PartitionInfo;

    use super::*;

    #[tokio::test]
    #[traced_test]
    async fn map_messages_to_nodes() {
        // for each message in the buffer, determine the broker to which it should be sent.
        // use message key to determine the broker. If key is not found then randomly select a broker

        let mut producer: Producer = Producer::new(vec![
            "127.0.0.1:5075".into(),
            "127.0.0.1:5076".into(),
            "127.0.0.1:5077".into(),
        ]);
        producer.send_batch(
            "first_topic".into(),
            vec![
                Message {
                    key: Some("key1".to_string()),
                    payload: "this is first message".as_bytes().to_vec(),
                    headers: HashMap::new(),
                },
                Message {
                    key: Some("key2".to_string()),
                    payload: "this is second message".as_bytes().to_vec(),
                    headers: HashMap::new(),
                },
            ],
        );
        producer.send_batch(
            "second_topic".into(),
            vec![Message {
                key: None,
                payload: "this is third message".as_bytes().to_vec(),
                headers: HashMap::new(),
            }],
        );

        let mut first_topic_info =
            Topic::new("first_topic".into(), None, None, None, None).unwrap();
        first_topic_info.partitions = vec![
            PartitionInfo {
                number: 0,
                leader_address: "127.0.0.1:5075".into(),
                follower_addresses: vec!["127.0.0.1:5076".into(), "127.0.0.1:5077".into()],
            },
            PartitionInfo {
                number: 1,
                leader_address: "127.0.0.1:5076".into(),
                follower_addresses: vec!["127.0.0.1:5075".into(), "127.0.0.1:5077".into()],
            },
            PartitionInfo {
                number: 2,
                leader_address: "127.0.0.1:5077".into(),
                follower_addresses: vec!["127.0.0.1:5076".into(), "127.0.0.1:5075".into()],
            },
        ];
        let mut second_topic_info =
            Topic::new("second_topic".into(), None, None, None, None).unwrap();
        second_topic_info.partitions = vec![
            PartitionInfo {
                number: 2,
                leader_address: "127.0.0.1:5075".into(),
                follower_addresses: vec!["127.0.0.1:5076".into(), "127.0.0.1:5077".into()],
            },
            PartitionInfo {
                number: 1,
                leader_address: "127.0.0.1:5076".into(),
                follower_addresses: vec!["127.0.0.1:5075".into(), "127.0.0.1:5077".into()],
            },
            PartitionInfo {
                number: 0,
                leader_address: "127.0.0.1:5077".into(),
                follower_addresses: vec!["127.0.0.1:5076".into(), "127.0.0.1:5075".into()],
            },
        ];
        let topic_info: HashMap<String, Topic> = vec![
            (first_topic_info.name.clone(), first_topic_info),
            (second_topic_info.name.clone(), second_topic_info),
        ]
        .into_iter()
        .collect();
        let mapped_messages: HashMap<String, (String, Vec<Message>)> =
            producer.map_messages_to_nodes(&topic_info);
        assert_eq!(mapped_messages.len(), 3);
        for (node_address, (topic_name, messages)) in mapped_messages {
            assert!(
                ["127.0.0.1:5075", "127.0.0.1:5076", "127.0.0.1:5077"]
                    .contains(&node_address.as_str())
            );
            assert!(["first_topic", "second_topic"].contains(&topic_name.as_str()));
            assert!(!messages.is_empty());
        }
    }
}
