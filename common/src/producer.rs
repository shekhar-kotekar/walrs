use std::{collections::HashMap, io::Write, net::TcpStream};

use bytes::BytesMut;
use tokio_util::codec::Encoder;

use crate::{
    authenticator,
    message_batch::{MessageBatch, MessageBatchCodec},
    models::{ClientType, ClusterResponse, Message},
};

pub struct Producer {
    brokers: Vec<String>,
    buffer: HashMap<String, Vec<Message>>,
}

impl Producer {
    pub fn new(brokers: Vec<String>) -> Self {
        Producer {
            brokers: brokers,
            buffer: HashMap::new(),
        }
    }

    pub fn send(&mut self, topic: String, message: Message) {
        self.buffer.entry(topic).or_default().push(message);
    }

    pub fn flush(&mut self) -> ClusterResponse {
        let mut stream = TcpStream::connect(&self.brokers[0]).unwrap();
        match authenticator::authenticate(&mut stream, ClientType::Producer) {
            Some(ClusterResponse::ConnectionAccepted) => {
                tracing::info!(
                    "Authentication successful. Producer sending message to {}",
                    &self.brokers[0]
                );
                let mut codec = MessageBatchCodec;
                self.buffer.drain().for_each(|(topic, messages)| {
                    let message_batch_to_send = MessageBatch {
                        topic_name: topic,
                        messages: messages,
                    };
                    let mut buf = BytesMut::new();
                    codec
                        .encode(message_batch_to_send, &mut buf)
                        .expect("Encoding failed for messages being sent.");
                    stream.write_all(&buf).unwrap();
                });

                stream.flush().unwrap();

                ClusterResponse::deserialize(&mut stream).unwrap()
            }
            _ => {
                tracing::error!("Failed to authenticate producer request");
                ClusterResponse::ConnectionRejected {
                    reason: "Failed to authenticate producer request".to_string(),
                }
            }
        }
    }
}
