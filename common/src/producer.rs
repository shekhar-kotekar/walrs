use std::{collections::HashMap, io::Write, net::TcpStream};

use bytes::BytesMut;
use tokio_util::codec::Encoder;

use crate::{
    authenticator,
    message_batch::{MessageBatch, MessageBatchCodec},
    models::{ClientCommand, ClientType, ClusterResponse, Message},
};

pub struct Producer {
    brokers: Vec<String>,
    buffer: HashMap<String, Vec<Message>>,
    message_batch_codec: MessageBatchCodec,
}

impl Producer {
    pub fn new(brokers: Vec<String>) -> Self {
        let codec = MessageBatchCodec;
        Producer {
            brokers,
            buffer: HashMap::new(),
            message_batch_codec: codec,
        }
    }

    pub fn send(&mut self, topic: String, message: &Message) {
        self.buffer.entry(topic).or_default().push(message.clone());
    }

    pub fn flush(&mut self) -> ClusterResponse {
        tracing::debug!("Flushing producer buffer...");
        let mut stream = TcpStream::connect(&self.brokers[0]).unwrap();
        match authenticator::authenticate(&mut stream, ClientType::Producer) {
            Some(ClusterResponse::ConnectionAccepted) => {
                tracing::info!("Producer authenticated.");
                let partition_leaders: HashMap<String, Vec<String>> =
                    match self.get_partition_leaders_for_topics(&mut stream) {
                        Some(leaders) => leaders,
                        None => {
                            return ClusterResponse::InternalError {
                                message: "Failed to get partition leaders.".to_string(),
                            }
                        }
                    };

                let messages_grouped_by_brokers = self.map_messages_to_brokers(&partition_leaders);

                messages_grouped_by_brokers.iter().for_each(|(broker, messages)| {
                    let mut stream = TcpStream::connect(broker).unwrap();
                    tracing::debug!("Connected to broker: {}", broker);
                    for message_batch in messages {
                        let mut buf = BytesMut::new();
                        self.message_batch_codec
                            .encode(message_batch.clone(), &mut buf)
                            .expect("Encoding failed for messages being sent.");
                        stream.write_all(&buf).unwrap_or_else(|_| {
                            tracing::error!("Failed to write message batch to broker: {}", broker)
                        });
                    }
                    stream.flush().unwrap();
                    tracing::debug!("Messages sent to broker: {}", broker);
                });
                ClusterResponse::MessagesPersisted {
                    count: self.buffer.values().map(|v| v.len()).sum::<usize>() as u8,
                }
                // self.send_messages(&mut stream)
            }
            _ => ClusterResponse::ConnectionRejected {
                reason: "Failed to authenticate producer.".to_string(),
            },
        }
    }

    fn map_messages_to_brokers(
        &mut self,
        partition_leaders: &HashMap<String, Vec<String>>,
    ) -> HashMap<String, Vec<MessageBatch>> {
        let mut messages_grouped_by_brokers: HashMap<String, Vec<MessageBatch>> = HashMap::new();
        let drained_buffer: Vec<(String, Vec<Message>)> = self.buffer.drain().collect();
        for (topic, messages) in drained_buffer {
            let partition_leaders_for_topic = partition_leaders.get(&topic).cloned().unwrap_or_default();
            let brokers_for_topic = self.group_messages_by_brokers(partition_leaders_for_topic, messages);

            for (broker, messages) in brokers_for_topic {
                let message_batch = MessageBatch {
                    topic_name: topic.clone(),
                    messages,
                };
                messages_grouped_by_brokers
                    .entry(broker)
                    .or_default()
                    .push(message_batch);
            }
        }
        messages_grouped_by_brokers
    }

    fn get_partition_leaders_for_topics(
        &mut self,
        stream: &mut TcpStream,
    ) -> Option<HashMap<String, Vec<String>>> {
        let command_to_get_leaders = ClientCommand::GetPartitionLeaders {
            topics: self.buffer.keys().cloned().collect(),
        };
        stream
            .write_all(&bincode::serialize(&command_to_get_leaders).unwrap())
            .expect("Failed to send partition leaders request.");

        match ClusterResponse::deserialize(stream).unwrap() {
            ClusterResponse::PartitionLeaders { leaders } => Some(leaders),
            ClusterResponse::InternalError { message } => {
                tracing::error!("Failed to get partition leaders: {}", message);
                None
            }
            _ => {
                tracing::error!("Unexpected response while getting partition leaders.");
                None
            }
        }
    }

    fn group_messages_by_brokers(
        &mut self,
        brokers: Vec<String>,
        messages: Vec<Message>,
    ) -> HashMap<String, Vec<Message>> {
        let mut grouped_messages: HashMap<String, Vec<Message>> = HashMap::new();
        for message in messages {
            // If message has a key then take its hash and use it to determine the broker
            // If message key is None, choose a random broker
            // each message should be sent to a single broker
            let broker = if let Some(key) = &message.key {
                let hash = crate::hash_code(key) % brokers.len() as u64;
                brokers[hash as usize].clone()
            } else {
                brokers[0].clone()
            };
            grouped_messages.entry(broker).or_default().push(message);
        }
        grouped_messages
    }

    // fn send_messages(&mut self, stream: &mut TcpStream) -> ClusterResponse {
    //     self.buffer.drain().for_each(|(topic, messages)| {
    //         let get_topic_medata_command = ClientCommand::GetTopicMetadata {
    //             topic_name: topic.clone(),
    //         };
    //         stream
    //             .write_all(&bincode::serialize(&get_topic_medata_command).unwrap())
    //             .expect("Failed to send topic metadata request.");

    //         match ClusterResponse::deserialize(stream).unwrap() {
    //             ClusterResponse::TopicMetadata { metadata } => {
    //                 tracing::info!("Topic metadata found for topic: {}", metadata.name);

    //                 let message_batch_to_send = MessageBatch {
    //                     topic_name: topic,
    //                     messages,
    //                 };
    //                 let mut buf = BytesMut::new();
    //                 self.message_batch_codec
    //                     .encode(message_batch_to_send, &mut buf)
    //                     .expect("Encoding failed for messages being sent.");
    //                 stream.write_all(&buf).unwrap();
    //             }
    //             ClusterResponse::InternalError { message } => {
    //                 tracing::error!("Failed to get metadata for topic: {} : {}", topic, message);
    //                 return;
    //             }
    //             _ => {
    //                 tracing::error!("Unexpected response while getting metadata for topic: {}", topic);
    //                 return;
    //             }
    //         }
    //         stream.flush().unwrap();
    //         tracing::debug!("Producer buffer flushed.");
    //     });
    //     ClusterResponse::deserialize(stream).unwrap()
    // }
}

#[cfg(test)]
mod should {
    use super::*;
    use crate::models::Message;
    use tracing_test::traced_test;

    #[test]
    // #[ignore = "reason"]
    #[traced_test]
    fn test_producer_should_sort_messages_by_broker_before_sending() {
        let message_1 = Message {
            key: Some("key1".to_string()),
            payload: b"message_1".to_vec(),
        };
        let message_2 = Message {
            key: Some("key2".to_string()),
            payload: b"message_2".to_vec(),
        };
        let message_3 = Message {
            key: None,
            payload: b"message_3".to_vec(),
        };
        let mut producer = Producer::new(vec!["broker_1:9092".to_string(), "broker_2:9092".to_string()]);
        producer.send("topic1".to_string(), &message_1);
        producer.send("topic1".to_string(), &message_2);
        producer.send("topic1".to_string(), &message_3);

        let grouped_messages = producer.group_messages_by_brokers(
            producer.brokers.clone(),
            producer.buffer.get("topic1").unwrap().clone(),
        );
        tracing::debug!("Grouped messages: {:?}", grouped_messages);
        assert_eq!(grouped_messages.len(), 2);
        assert!(
            grouped_messages.contains_key("broker_1:9092") || grouped_messages.contains_key("broker_2:9092")
        );
    }

    #[test]
    #[traced_test]
    fn test_producer_should_map_messages_to_brokers_before_sending() {
        let message_1 = Message {
            key: Some("key1".to_string()),
            payload: b"message_1".to_vec(),
        };
        let message_2 = Message {
            key: Some("key2".to_string()),
            payload: b"message_2".to_vec(),
        };
        let message_3 = Message {
            key: None,
            payload: b"message_3".to_vec(),
        };
        let message_4 = Message {
            key: Some("key2".to_string()),
            payload: b"message_4".to_vec(),
        };
        let mut producer = Producer::new(vec![
            "broker_1:9092".to_string(),
            "broker_2:9092".to_string(),
            "broker_3:9092".to_string(),
        ]);

        let partition_leaders = HashMap::from([
            (
                "topic1".to_string(),
                vec!["broker_1:9092".to_string(), "broker_3:9092".to_string()],
            ),
            ("topic2".to_string(), vec!["broker_2:9092".to_string()]),
        ]);
        producer.buffer.insert(
            "topic1".to_string(),
            vec![
                message_1.clone(),
                message_2.clone(),
                message_3.clone(),
                Message {
                    key: Some("key1".to_string()),
                    payload: b"message_6".to_vec(),
                },
                Message {
                    key: None,
                    payload: b"message_7".to_vec(),
                },
            ],
        );
        producer.buffer.insert(
            "topic2".to_string(),
            vec![
                message_4.clone(),
                Message {
                    key: Some("key1".to_string()),
                    payload: b"message_5".to_vec(),
                },
            ],
        );

        let messages_grouped_by_brokers = producer.map_messages_to_brokers(&partition_leaders);
        tracing::debug!("Messages grouped by brokers:");
        messages_grouped_by_brokers.iter().for_each(|(broker, messages)| {
            tracing::debug!("Broker: {}, Messages: {:?}", broker, messages);
        });
        assert_eq!(messages_grouped_by_brokers.len(), 3);
    }
}
