use commons::models::Message;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::sync::mpsc;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::models::PartitionCommand;
use crate::models::PartitionWriterResponse;
use crate::partition_managers::models::{INDEX_ENTRY_SIZE, RecordIndex};

pub struct PartitionWriter {
    partition_name: String,
    partition_path: String,
    flush_interval: tokio::time::Interval,
}

impl PartitionWriter {
    pub fn new(topic: &String, partition_number: u8, base_path: &String) -> Self {
        let partition_name = format!("{}-p{}", topic, partition_number);
        let partition_path = format!("{}/{}/p{}", base_path, topic, partition_number);
        std::fs::create_dir_all(&partition_path)
            .unwrap_or_else(|_| panic!("Failed to create partition directory: {}", partition_path));

        PartitionWriter {
            partition_name,
            partition_path,
            //TODO: Read flush interval from config
            flush_interval: tokio::time::interval(std::time::Duration::from_millis(300)),
        }
    }

    pub async fn start(
        &mut self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!(
            "Starting partition writer for partition {}",
            self.partition_name
        );

        let data_file_path = format!("{}/data.log", self.partition_path);
        tracing::debug!("Partition data will be stored in {}", data_file_path);

        let index_file_path = format!("{}/index.log", self.partition_path);
        tracing::debug!("Partition index will be stored in {}", index_file_path);

        let mut data_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(data_file_path)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to open data file for: {}, error: {}",
                    self.partition_name, e
                )
            });

        let mut index_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_file_path)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to open index file for: {}, error: {}",
                    self.partition_name, e
                )
            });

        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::WriteMessages { messages, tx } => {
                            let message_count = messages.len() as u8;
                            for message in messages {
                                match self.write_message(message, &mut data_file, &mut index_file).await {
                                    Ok(()) => {
                                    }
                                    Err(e) => {
                                        //TODO: How to handle errors in writing messages?
                                        //Should we retry or log and continue?
                                        tracing::error!("Failed to write message to partition {}: {}", self.partition_name, e);
                                    }
                                }
                            }
                            data_file.flush().await.expect("Failed to flush data file");
                            index_file.flush().await.expect("Failed to flush index file");
                            tracing::debug!("Data and index files flushed after writing {} messages", message_count);
                            tx.send(PartitionWriterResponse::MessagesPersisted { count: message_count }).unwrap_or_else(|send_error| {
                                tracing::error!(
                                    "Failed to send response for partition {}: {:?}",
                                    self.partition_name,
                                    send_error
                                );
                            });
                        }
                        _ => {
                            tracing::error!("Unknown partition command received: {:?}", command);
                        }
                    }
                }
                _ = self.flush_interval.tick() => {
                    data_file.sync_all().await.expect("Failed to sync partition file");
                    index_file.sync_all().await.expect("Failed to sync index file");
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token called. Partition writer {} shutting down", self.partition_name);
                    data_file.sync_all().await.expect("Failed to sync partition file");
                    index_file.sync_all().await.expect("Failed to sync index file");

                    let _ = index_file.shutdown().await;
                    let _ = data_file.shutdown().await;
                    tracing::info!("Partition file flushed & closed: {}", self.partition_name);
                    break;
                }
            }
        }
        tracing::info!("Partition writer stopped: {}", self.partition_name);
    }

    async fn write_message(
        &self,
        message: Message,
        data_file: &mut File,
        index_file: &mut File,
    ) -> Result<(), std::io::Error> {
        let message_offset = data_file.stream_position().await?;

        let timestamp = chrono::Utc::now().timestamp_millis();

        let payload_len: [u8; 4] = (message.payload.len() as u32).to_be_bytes();
        let timestamp_bytes = timestamp.to_be_bytes();
        let timestamp_bytes_len = timestamp_bytes.len();
        let payload_len_len = payload_len.len();
        if timestamp_bytes_len != 8 || payload_len_len != 4 {
            panic!("Timestamp must be 8 bytes and payload length must be 4 bytes");
        }
        tracing::debug!("Writing message: {:?}", message);

        //TODO: write message headers and key if they exist
        let mut buffer =
            Vec::with_capacity(timestamp_bytes_len + payload_len_len + message.payload.len());
        buffer.extend_from_slice(&timestamp_bytes); // 8 bytes
        buffer.extend_from_slice(&payload_len); // 4 bytes
        buffer.extend_from_slice(&message.payload);

        data_file.write_all(&buffer).await?;

        tracing::debug!(
            "message offset: {}, message.payload.len(): {}, payload_len: {:?},payload_len_len: {}, buffer len: {}",
            message_offset,
            message.payload.len(),
            payload_len,
            payload_len_len,
            buffer.len() as u32
        );

        let index_entry = RecordIndex {
            // timestamp,
            offset: message_offset,
            length: buffer.len() as u32,
        };
        tracing::debug!(
            "Writing index entry: {:?}, offset: {}",
            index_entry,
            message_offset
        );

        let mut index_entry_buffer = vec![0u8; INDEX_ENTRY_SIZE];
        bincode::encode_into_slice(
            &index_entry,
            &mut index_entry_buffer,
            bincode::config::standard(),
        )
        .unwrap();
        index_file.write_all(&index_entry_buffer).await?;
        Ok(())
    }
}
