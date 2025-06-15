use commons::models::{PartitionRole, PeerCommand, PeerResponse};
use tokio::sync::mpsc;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::models::PartitionCommand;
use crate::models::PartitionWriterResponse;

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

pub async fn create_partition(
    topic_name: &String,
    partition_number: u8,
    self_address: &str,
    partition_role: PartitionRole,
    local_data_dir_path: &String,
    cancellation_token: CancellationToken,
) -> Option<mpsc::Sender<PartitionCommand>> {
    tracing::info!(
        "Creating partition {} for topic: {}, role: {:?}",
        partition_number,
        topic_name,
        partition_role
    );
    let (partition_writer_tx, partition_writer_rx) = mpsc::channel::<PartitionCommand>(10);

    let mut partition_writer =
        PartitionWriter::new(topic_name, partition_number, local_data_dir_path);

    tokio::spawn(async move {
        partition_writer
            .start(partition_writer_rx, cancellation_token)
            .await;
    });

    match partition_role {
        PartitionRole::Leader { followers } => {
            tracing::info!(
                "Leader partition created for topic: {}, partition: {}, partition followers: {:?}",
                topic_name,
                partition_number,
                followers
            );
            let peers = followers.keys().cloned().collect::<Vec<String>>();
            let all_peers_created_followers =
                create_partition_followers(topic_name, partition_number, peers, self_address).await;
            if all_peers_created_followers {
                Some(partition_writer_tx)
            } else {
                tracing::error!(
                    "Failed to request peers to create follower partitions. topic: {}, partition: {}",
                    topic_name,
                    partition_number
                );
                None
            }
        }
        PartitionRole::Follower { leader_address } => {
            tracing::info!(
                "Created follower partition {} for topic: {}, leader address: {}",
                partition_number,
                topic_name,
                leader_address
            );
            Some(partition_writer_tx)
        }
    }
}

async fn create_partition_followers(
    topic_name: &String,
    partition_number: u8,
    peers: Vec<String>,
    self_address: &str,
) -> bool {
    for peer in peers {
        let peer_command = PeerCommand::CreatePartitionWriter {
            topic_name: topic_name.clone(),
            partition_number,
            role: PartitionRole::Follower {
                leader_address: self_address.to_string(),
            },
        };
        match commons::send_and_receive_peer_command(peer_command, &peer).await {
            PeerResponse::PartitionWriterCreated => {
                tracing::info!(
                    "Peer {} created follower partition for topic: {}, partition: {}",
                    peer,
                    topic_name,
                    partition_number
                );
            }
            other_response => {
                tracing::error!(
                    "Peer {} responded with unexpected response: {:?} for topic: {}, partition: {}",
                    peer,
                    other_response,
                    topic_name,
                    partition_number
                );
                return false;
            }
        }
    }
    true
}
