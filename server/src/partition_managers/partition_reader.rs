use std::{collections::HashMap, io::SeekFrom};

use commons::models::Message;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{PartitionCommand, PartitionReaderResponse},
    partition_managers::models::{INDEX_ENTRY_SIZE, RecordIndex},
};

pub struct PartitionReader {
    topic_name: String,
    message_batch_size: u16,
    partition_name: String,
    partition_path: String,
}

impl PartitionReader {
    pub fn new(
        topic: String,
        partition_number: u8,
        base_path: String,
        message_batch_size: u16,
    ) -> Self {
        let partition_name = format!("{}-{}", topic, partition_number);
        let partition_path = format!("{}/{}/p{}", base_path, topic, partition_number);

        PartitionReader {
            topic_name: topic,
            message_batch_size,
            partition_name,
            partition_path,
        }
    }

    // async fn read_messages_from_file(
    //     &mut self,
    //     reader: &mut tokio::io::BufReader<tokio::fs::File>,
    // ) -> Vec<Message> {
    //     let mut messages = Vec::new();
    //     let mut buf = String::new();

    //     let mut lines_read = 0;
    //     while lines_read < self.message_batch_size
    //         && reader.read_line(&mut buf).await.unwrap_or_else(|error| {
    //             panic!(
    //                 "Failed to read line for partition: {}. Error: {}",
    //                 self.partition_name, error
    //             )
    //         }) > 0
    //     {
    //         let payload = buf.trim().as_bytes().to_vec();
    //         messages.push(Message {
    //             key: None,
    //             payload,
    //             headers: HashMap::new(),
    //         });
    //         buf.clear();
    //         lines_read += 1;
    //     }
    //     messages
    // }

    pub async fn start(
        &mut self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!(
            "Starting partition reader for partition {}",
            self.partition_name
        );

        let data_file_path = format!("{}/data.log", self.partition_path);
        tracing::info!("Partition data will be read from {}", data_file_path);
        let data_file = OpenOptions::new()
            .read(true)
            .open(&data_file_path)
            .await
            .unwrap_or_else(|open_error| {
                panic!("Failed to open partition file for reading: {}", open_error)
            });

        let index_file_path = format!("{}/index.log", self.partition_path);
        tracing::debug!("Partition index will be stored in {}", index_file_path);
        let mut index_file = OpenOptions::new()
            .read(true)
            .open(&index_file_path)
            .await
            .unwrap_or_else(|open_error| {
                panic!(
                    "Failed to open partition index file for reading: {}",
                    open_error
                )
            });

        let mut reader = tokio::io::BufReader::new(data_file);

        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::FetchMessages { topic_name, tx } => {
                            let response: PartitionReaderResponse = if topic_name == self.topic_name {
                                tracing::debug!("Reading messages from partition {}...", self.partition_name);
                                // let messages = self.read_messages_from_file(&mut reader).await;
                                match self.read_messages(reader.get_mut(), &mut index_file).await {
                                    Ok(messages) => {
                                        PartitionReaderResponse::MessagesRead { messages }
                                    }
                                    Err(e) => {
                                        PartitionReaderResponse::InternalError {
                                            message: format!("Failed to read messages: {}", e),
                                        }
                                    }
                                }
                            } else {
                                PartitionReaderResponse::InternalError {
                                    message: format!("Received read request for different topic: {}", topic_name),
                                }
                            };
                            tx.send(response).unwrap_or_else(|send_error| {
                                tracing::error!(
                                    "Failed to send response for partition {}: {:?}",
                                    self.partition_name,
                                    send_error
                                );
                            });
                        }
                        other => {
                            tracing::error!("Invalid command received: {:?}", other);
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

    async fn read_messages(
        &self,
        data_file: &mut File,
        index_file: &mut File,
    ) -> Result<Vec<Message>, std::io::Error> {
        let mut messages: Vec<Message> = Vec::new();
        for _ in 0..self.message_batch_size {
            let message = self.read_message_using_index(data_file, index_file).await?;
            messages.push(message);
        }
        Ok(messages)
    }

    async fn read_message_using_index(
        &self,
        data_file: &mut File,
        index_file: &mut File,
    ) -> Result<Message, std::io::Error> {
        //TODO: Identify EOF and return an error or empty message
        //TODO: For time being we will always start reading from the beginning of the file.
        // let message_offset = INDEX_ENTRY_SIZE as u64 * 0; // Start from the beginning
        let message_offset = 0;
        index_file.seek(SeekFrom::Start(message_offset)).await?;

        let mut index_bytes = vec![0u8; INDEX_ENTRY_SIZE];
        index_file.read_exact(&mut index_bytes).await?;
        let record_entry: RecordIndex =
            bincode::decode_from_slice(&index_bytes, bincode::config::standard())
                .unwrap()
                .0;

        data_file.seek(SeekFrom::Start(record_entry.offset)).await?;

        let mut timestamp_bytes = [0u8; 8];
        data_file.read_exact(&mut timestamp_bytes).await?;
        let timestamp = i64::from_be_bytes(timestamp_bytes);

        let mut length_bytes = [0u8; 4];
        data_file.read_exact(&mut length_bytes).await?;
        let length = u32::from_be_bytes(length_bytes);

        let mut payload_bytes = vec![0u8; length as usize];
        data_file.read_exact(&mut payload_bytes).await?;

        let message = Message {
            key: None,
            payload: payload_bytes,
            headers: HashMap::new(),
        };
        tracing::info!(
            "Read message with timestamp: {}, length: {}, payload: {:?}",
            timestamp,
            length,
            message.payload
        );

        Ok(message)
    }
}
