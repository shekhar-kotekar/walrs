use std::{
    io::{Read, Write},
    net::TcpStream,
};

use serde::{Deserialize, Serialize};

use crate::{
    authenticator,
    message_batch::MessageBatch,
    models::{ClientType, ClusterResponse, Message},
};

pub struct Consumer {
    topic: String,
    stream: TcpStream,
    previous_message_offset: u128,
}

impl Consumer {
    pub fn new(topic: String, brokers: Vec<String>) -> Self {
        Consumer {
            topic,
            stream: TcpStream::connect(&brokers[0]).unwrap(),
            previous_message_offset: 0,
        }
    }

    pub async fn next_message(&mut self) -> Option<MessageBatch> {
        match authenticator::authenticate(
            &mut self.stream,
            ClientType::Consumer {
                topic_name: self.topic.clone(),
            },
        ) {
            Some(ClusterResponse::ConnectionAccepted) => {
                tracing::debug!("Consumer authenticated successfully.");
                self.fetch_next().await
            }
            other => {
                tracing::error!("Failed to authenticate consumer: {:?}", other);
                None
            }
        }
    }

    async fn fetch_next(&mut self) -> Option<MessageBatch> {
        let fetch_next_message = ConsumerCommand::FetchNext {
            last_consumed_offset: self.previous_message_offset,
        };
        let command_bytes = bincode::serialize(&fetch_next_message);
        self.stream.write_all(&command_bytes.unwrap()).unwrap();
        self.stream.flush().unwrap();
        tracing::debug!("Request sent to fetch next message batch.");

        let response = ClusterResponse::deserialize(&mut self.stream);
        match response {
            Some(ClusterResponse::ConsumerResponse(consumer_response)) => match consumer_response {
                ConsumerResponse::MessagesFetched { messages } => {
                    let message_count = messages.len() as u128;
                    tracing::debug!("Fetched {} messages.", message_count);
                    self.previous_message_offset += message_count;
                    Some(MessageBatch {
                        topic_name: self.topic.clone(),
                        messages,
                    })
                }
                ConsumerResponse::InternalError { message } => {
                    tracing::error!("Failed to fetch messages: {}", message);
                    None
                }
            },
            None => {
                tracing::error!("Failed to receive response from partition reader");
                return None;
            }
            _ => {
                tracing::error!("Received unexpected response from cluster: {:?}", response);
                None
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConsumerCommand {
    FetchNext { last_consumed_offset: u128 },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConsumerResponse {
    MessagesFetched { messages: Vec<Message> },
    InternalError { message: String },
}

impl ConsumerResponse {
    pub fn deserialize(stream: &mut TcpStream) -> Option<ConsumerResponse> {
        let mut buffer = [0u8; 64];
        let bytes_read = stream.read(&mut buffer).ok()?;
        let consumer_response: ConsumerResponse = bincode::deserialize(&buffer[..bytes_read]).ok()?;
        Some(consumer_response)
    }
}
