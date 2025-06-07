use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::models::{PartitionCommand, PartitionWriterResponse};

pub struct PartitionWriter {
    partition_name: String,
    partition_path: String,
}

impl PartitionWriter {
    pub fn new(topic: &String, partition_number: u8, base_path: &String) -> Self {
        let partition_name = format!("{}-p{}", topic, partition_number);
        let partition_path = format!("{}/{}/p{}", base_path, topic, partition_number);
        std::fs::create_dir_all(&partition_path)
            .unwrap_or_else(|_| panic!("Failed to create partition directory: {}", partition_name));

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
        tracing::info!(
            "Starting partition writer for partition {}",
            self.partition_name
        );

        let partition_file_path = format!("{}/data.log", self.partition_path);
        tracing::info!("Partition data will be stored in {}", partition_file_path);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(partition_file_path)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Failed to open partition file for partition: {}",
                    self.partition_name
                )
            });

        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::WriteMessages { messages, tx } => {
                            let message_count = messages.len() as u8;
                            for message in messages {
                                // write each message payload in timestamp in epoch format # followed by actualy payload
                                let timestamp = chrono::Utc::now().timestamp().to_be_bytes();
                                file.write_all(&timestamp).await.expect("Failed to write timestamp to partition");
                                file.write_all(b"#").await.expect("Failed to write separator to partition");
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
        tracing::info!(
            "Partition writer for partition {} stopped.",
            self.partition_name
        );
    }
}
