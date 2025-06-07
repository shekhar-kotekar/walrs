use common::models::Message;
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::models::{PartitionCommand, PartitionReaderResponse};
use tokio::io::AsyncBufReadExt;

pub struct PartitionReader {
    topic_name: String,
    message_batch_size: u16,
    partition_name: String,
    partition_path: String,
}

impl PartitionReader {
    pub fn new(topic: String, partition_number: u8, base_path: String, message_batch_size: u16) -> Self {
        let partition_name = format!("{}-{}", topic, partition_number);
        let partition_path = format!("{}/{}/p{}", base_path, topic, partition_number);

        PartitionReader {
            topic_name: topic,
            message_batch_size,
            partition_name,
            partition_path,
        }
    }

    async fn read_messages_from_file(
        &mut self,
        reader: &mut tokio::io::BufReader<tokio::fs::File>,
    ) -> Vec<Message> {
        let mut messages = Vec::new();
        let mut buf = String::new();

        let mut lines_read = 0;
        while lines_read < self.message_batch_size
            && reader.read_line(&mut buf).await.unwrap_or_else(|_| {
                panic!(
                    "Failed to read line from partition for partition: {}",
                    self.partition_name
                )
            }) > 0
        {
            let payload = buf.trim().as_bytes().to_vec();
            messages.push(Message { key: None, payload });
            buf.clear();
            lines_read += 1;
        }
        messages
    }

    pub async fn start(
        &mut self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!("Starting partition reader for partition {}", self.partition_name);

        let log_file_name = format!("{}/data.log", self.partition_path);
        tracing::info!("Partition data will be read from {}", log_file_name);
        let file = OpenOptions::new()
            .read(true)
            .open(&log_file_name)
            .await
            .unwrap_or_else(|open_error| panic!("Failed to open partition file for reading: {}", open_error));

        let mut reader = tokio::io::BufReader::new(file);

        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::ReadMessages { topic_name, tx } => {
                            if topic_name == self.topic_name {
                                tracing::debug!("Reading messages from partition {}...", self.partition_name);

                                let messages = self.read_messages_from_file(&mut reader).await;
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
                    tracing::info!("Cancellation token called. Partition reader shutting down: {}", self.partition_name);
                    let _ = reader.shutdown().await;
                    tracing::info!("Partition reader file closed: {}", self.partition_name);
                    break;
                }
            }
        }
        tracing::info!("Partition reader stopped: {}", self.partition_name);
    }
}
