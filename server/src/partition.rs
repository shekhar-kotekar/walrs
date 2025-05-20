use common::models::Message;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::models::{PartitionCommand, PartitionReaderResponse, PartitionWriterResponse};

pub struct PartitionWriter {
    topic: String,
}

impl PartitionWriter {
    pub fn new(topic: String) -> Self {
        PartitionWriter { topic }
    }

    pub async fn start(
        &mut self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!("Starting partition manager for topic {}", self.topic);
        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::WriteMessages { messages, tx } => {
                            let message_count = messages.len() as u8;
                            for message in messages {
                                tracing::debug!("Writing message to partition {}: {:?}", self.topic, message);
                            }
                            let _ = tx.send(PartitionWriterResponse::MessagesPersisted { count: message_count });
                        }
                        _ => {
                            tracing::error!("Unknown partition command received: {:?}", command);
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token called. Partition writer for topic {} shutting down...", self.topic);
                    break;
                }
            }
        }
        tracing::info!("Partition writer for topic {} stopped.", self.topic);
    }
}

pub struct PartitionReader {
    pub topic: String,
}

impl PartitionReader {
    pub async fn start(
        &mut self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!("Starting partition reader for topic {}", self.topic);
        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::ReadMessages { topic_name, tx } => {
                            if topic_name == self.topic {
                                tracing::debug!("Reading messages from partition {}...", self.topic);
                                let messages = vec![Message {
                                    payload: "first_message".as_bytes().to_vec(),
                                }]; // Simulate reading messages
                                let _ = tx.send(PartitionReaderResponse::MessagesRead { messages });
                            } else {
                                let _ = tx.send(PartitionReaderResponse::InternalError {
                                    message: format!("Received read request for different topic: {}", topic_name),
                                });
                            }
                        }
                        _ => {
                            tracing::warn!("Invalid partition command received: {:?}", command);
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token called. Partition reader for topic {} shutting down...", self.topic);
                    break;
                }
            }
        }
        tracing::info!("Partition reader for topic {} stopped.", self.topic);
    }
}
