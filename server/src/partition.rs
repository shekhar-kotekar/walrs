use common::models::Message;
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::models::{PartitionCommand, PartitionReaderResponse, PartitionWriterResponse, PartitionWriterRole};
use tokio::io::AsyncBufReadExt;

pub struct PartitionWriter {
    partition_name: String,
    partition_path: String,
}

impl PartitionWriter {
    pub fn new(topic: &String, partition_number: u8, role: &PartitionWriterRole, base_path: &String) -> Self {
        let partition_name = format!("{}-p{}-{:?}", topic, partition_number, role);
        let partition_path = format!("{}/{}/p{}-{:?}", base_path, topic, partition_number, role);
        std::fs::create_dir_all(&partition_path)
            .expect(format!("Failed to create partition directory: {}", partition_name).as_str());

        PartitionWriter {
            partition_name,
            partition_path,
        }
    }

    pub async fn start(
        &mut self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!("Starting partition writer for partition {}", self.partition_name);

        let partition_file_path = format!("{}/data.log", self.partition_path);
        tracing::info!("Partition data will be stored in {}", partition_file_path);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(partition_file_path)
            .await
            .expect(format!("Failed to open partition file for partition: {}", self.partition_name).as_str());

        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::WriteMessages { messages, tx } => {
                            let message_count = messages.len() as u8;
                            for message in messages {
                                file.write_all(&message.payload).await.expect("Failed to write message to partition");
                                file.write_all(b"\n").await.expect("Failed to write newline to partition");
                            }
                            tracing::debug!("Wrote {} messages to partition {}", message_count, self.partition_name);
                            let _ = tx.send(PartitionWriterResponse::MessagesPersisted { count: message_count });
                        }
                        _ => {
                            tracing::error!("Unknown partition command received: {:?}", command);
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token called. Partition writer for partition {} shutting down...", self.partition_name);
                    file.flush().await.expect("Failed to flush partition file");
                    let _ = file.shutdown().await;
                    tracing::info!("Partition file flushed & closed for partition: {}", self.partition_name);
                    break;
                }
            }
        }
        tracing::info!("Partition writer for partition {} stopped.", self.partition_name);
    }
}

pub struct PartitionReader {
    topic_name: String,
    message_batch_size: u8,
    partition_name: String,
    partition_path: String,
}

impl PartitionReader {
    pub fn new(topic: String, partition_number: u8, base_path: String, message_batch_size: u8) -> Self {
        let partition_name = format!("{}-{}", topic, partition_number);
        let partition_path = format!("{}/{}/{}", base_path, topic, partition_number);

        PartitionReader {
            topic_name: topic,
            message_batch_size,
            partition_name,
            partition_path,
        }
    }

    async fn read_messages_from_file(&mut self, reader: &mut tokio::io::BufReader<tokio::fs::File>) -> Vec<Message> {
        let mut messages = Vec::new();
        let mut buf = String::new();

        let mut lines_read = 0;
        while lines_read < self.message_batch_size
            && reader.read_line(&mut buf).await.expect(
                format!(
                    "Failed to read line from partition for partition: {}",
                    self.partition_name
                )
                .as_str(),
            ) > 0
        {
            let payload = buf.trim().as_bytes().to_vec();
            messages.push(Message { payload });
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
            .expect(format!("Failed to open partition file for reading: {}", self.partition_name).as_str());
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
