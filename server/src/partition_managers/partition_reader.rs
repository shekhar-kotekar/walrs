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
                                            message: format!("Error while reading messages: {}", e),
                                        }
                                    }
                                }
                            } else {
                                PartitionReaderResponse::InternalError {
                                    message: format!("Received read request for different topic: {}", topic_name),
                                }
                            };
                            tx.send(response).unwrap_or_else(|err| {
                                tracing::error!(
                                    "Failed to send response:{}: {:?}",
                                    self.partition_name,
                                    err
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
        for message_number in 0..self.message_batch_size {
            match self
                .read_message_using_index(data_file, index_file, message_number)
                .await
            {
                Ok(message) => {
                    messages.push(message);
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        tracing::warn!("EOF while reading messages: {}", self.partition_name);
                        break;
                    } else {
                        return Err(e); // Propagate other errors
                    }
                }
            }
        }
        Ok(messages)
    }

    async fn read_message_using_index(
        &self,
        data_file: &mut File,
        index_file: &mut File,
        message_number: u16,
    ) -> Result<Message, std::io::Error> {
        //TODO: Identify EOF and return an error or empty message
        //TODO: For time being we will always start reading from the beginning of the file.
        let message_offset = INDEX_ENTRY_SIZE as u64 * message_number as u64; // Start from the beginning
        index_file.seek(SeekFrom::Start(message_offset)).await?;
        tracing::debug!(
            "Reading from index file. offset: {}, record length: {}",
            message_offset,
            INDEX_ENTRY_SIZE
        );

        let mut index_bytes = vec![0u8; INDEX_ENTRY_SIZE];
        index_file.read_exact(&mut index_bytes).await?;
        let record_entry: RecordIndex =
            bincode::decode_from_slice(&index_bytes, bincode::config::standard())
                .unwrap()
                .0;
        tracing::debug!("Read record index entry: {:?}", record_entry);

        data_file.seek(SeekFrom::Start(record_entry.offset)).await?;

        let mut record_buffer = vec![0u8; record_entry.length as usize];
        data_file.read_exact(&mut record_buffer).await?;
        tracing::debug!(
            "Read record from data file at offset {} with length {}",
            record_entry.offset,
            record_entry.length
        );
        let timestamp = i64::from_be_bytes(record_buffer[0..8].try_into().unwrap());
        let payload_length = u32::from_be_bytes(record_buffer[8..12].try_into().unwrap());
        let payload_bytes = record_buffer[12..].to_vec();
        tracing::debug!(
            "Read timestamp: {}, payload length: {}, payload bytes: {:?}",
            timestamp,
            payload_length,
            payload_bytes
        );
        let message = Message {
            key: None,
            payload: payload_bytes,
            headers: HashMap::new(),
        };
        tracing::info!(
            "message constructed from bytes. timestamp: {}, length: {}, payload: {:?}",
            timestamp,
            payload_length,
            message.payload
        );

        Ok(message)
    }
}
