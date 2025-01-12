use common::models::{AckLevel, MessageBatch};
use serde::{Deserialize, Serialize};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{PartitionCommand, PartitionResponse},
    partitions::{
        replica::ReplicaRequest,
        segment_writer::{SegmentWriter, SegmentWriterCommand},
    },
};

use super::segment_writer::SegmentWriterResponse;

const BUFFER_SIZE: usize = 256;
const STORAGE_DEFAULT_PATH: &str = "/tmp";
const SEGMENT_WRITER_CHANNEL_SIZE: usize = 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LeaderResponse {
    FetchResponse {
        messages: MessageBatch,
        has_more: bool,
    },
}

pub struct Leader {
    pub num_partition: u8,
    pub topic_name: String,
    pub address: String,
    in_sync_replicas: Vec<String>,
    high_watermark: u64,
    storage_path: String,
}

impl Leader {
    pub fn new(
        num_partition: u8,
        topic_name: String,
        address: String,
        storage_path: Option<String>,
    ) -> Self {
        let storage_base_path = if storage_path.is_none() {
            tracing::warn!("Storage path is not provided. Using default path.");
            STORAGE_DEFAULT_PATH.to_string()
        } else {
            storage_path.unwrap()
        };
        let storage_path = format!(
            "{}/topic_{}/partition_{}",
            storage_base_path, topic_name, num_partition
        );
        Leader {
            num_partition,
            topic_name,
            address,
            in_sync_replicas: vec![],
            high_watermark: 0,
            storage_path,
        }
    }

    async fn start_segment_writer(
        &self,
        segment_id: u32,
        cancellation_token: CancellationToken,
    ) -> mpsc::Sender<SegmentWriterCommand> {
        let (segment_writer_tx, segment_writer_rx) =
            mpsc::channel::<SegmentWriterCommand>(SEGMENT_WRITER_CHANNEL_SIZE);

        let segment_writer: SegmentWriter =
            SegmentWriter::new(self.storage_path.clone(), segment_id);
        tokio::spawn(async move {
            segment_writer
                .start(segment_writer_rx, cancellation_token)
                .await;
        });
        return segment_writer_tx;
    }

    pub async fn start(
        &self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        let current_segment_id: u32 = 0;
        //TODO: Add code to change the file to which we write data after reaching a certain size or time.
        let segment_writer_tx: mpsc::Sender<SegmentWriterCommand> = self
            .start_segment_writer(current_segment_id, cancellation_token.child_token())
            .await;

        tracing::info!(
            "Partition leader for {} topic, partition {} started.",
            self.topic_name,
            self.num_partition
        );
        let leader_socket = UdpSocket::bind(&self.address).await.unwrap();
        loop {
            let mut buffer = [0u8; BUFFER_SIZE];
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::Write { batch, response_tx } => {
                            let segment_writer_tx_clone = segment_writer_tx.clone();
                            tokio::spawn(async move {
                                let batch_bytes = bincode::serialize(&batch).unwrap();
                                let (segment_writer_response_tx, segment_writer_response_rx) = oneshot::channel::<SegmentWriterResponse>();
                                let segment_writer_command = SegmentWriterCommand::Write {
                                    data: batch_bytes,
                                    response_tx: segment_writer_response_tx,
                                };
                                segment_writer_tx_clone.send(segment_writer_command).await.unwrap();
                                let response_from_segment_writer = segment_writer_response_rx.await.unwrap();
                                let response =  match response_from_segment_writer {
                                    SegmentWriterResponse::WriteSuccess => PartitionResponse::LeaderAcknowledged,
                                    SegmentWriterResponse::WriteFailure => PartitionResponse::Error {
                                        message: "Error writing to segment file.".to_string(),
                                    },
                                };
                                response_tx.send(response).unwrap();
                            });
                        }
                    }
                }
                recv_result = leader_socket.recv_from(&mut buffer) => {
                    match recv_result {
                        Ok((size, peer)) => {
                            tracing::info!("Received {} bytes from {}", size, peer);
                            let replica_request: ReplicaRequest = bincode::deserialize(&buffer[..size]).unwrap();
                            match replica_request {
                                ReplicaRequest::FetchRequest { current_offset } => {
                                    let message_batch = MessageBatch {
                                        topic_name: self.topic_name.clone(),
                                        messages: vec![],
                                        ack_level: AckLevel::Leader,
                                    };
                                    let response = LeaderResponse::FetchResponse {
                                        messages: message_batch,
                                        has_more: false,
                                    };
                                    let response_bytes = bincode::serialize(&response).unwrap();
                                    leader_socket.send_to(&response_bytes, &peer).await.unwrap();
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error receiving data: {:?}", e);
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Partition leader for {} topic cancelled.", self.topic_name);
                    break;
                }
            }
        }
        tracing::info!(
            "Partition leader for {} topic, partition {} stopped.",
            self.topic_name,
            self.num_partition
        );
    }
}

#[cfg(test)]
mod tests {
    use common::models::{AckLevel, Message, MessageBatch};
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;

    use crate::models::{PartitionCommand, PartitionResponse};

    use super::*;

    #[tokio::test]
    #[traced_test]
    async fn partition_leader_should_be_able_to_write_message_batches() {
        let partition_leader =
            Leader::new(0, "topic_1".to_string(), "0.0.0.0:1000".to_string(), None);

        let cancellation_token = CancellationToken::new();
        let (main_tx, main_rx) = mpsc::channel::<PartitionCommand>(1);
        let partition_leader_ct = cancellation_token.child_token();
        let leader_task_handle = tokio::spawn(async move {
            partition_leader.start(main_rx, partition_leader_ct).await;
        });

        let messages = vec![
            Message {
                payload: "message_1_payload".as_bytes().to_vec(),
            },
            Message {
                payload: "message_2_payload".as_bytes().to_vec(),
            },
        ];
        let batch = MessageBatch {
            topic_name: "topic1".to_string(),
            messages,
            ack_level: AckLevel::Leader,
        };

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<PartitionResponse>();
        main_tx
            .send(PartitionCommand::Write {
                batch,
                response_tx: oneshot_tx,
            })
            .await
            .unwrap();

        let response = oneshot_rx.await.unwrap();
        assert_eq!(response, PartitionResponse::LeaderAcknowledged);

        cancellation_token.cancel();
        leader_task_handle.await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    #[traced_test]
    async fn partition_leader_should_acknowledge_from_all_followers_when_ack_level_set_to_all() {
        let partition_leader =
            Leader::new(0, "topic_1".to_string(), "0.0.0.0:1001".to_string(), None);

        let cancellation_token = CancellationToken::new();
        let (main_tx, main_rx) = mpsc::channel::<PartitionCommand>(1);
        let partition_leader_ct = cancellation_token.child_token();
        let leader_task_handle = tokio::spawn(async move {
            partition_leader.start(main_rx, partition_leader_ct).await;
        });

        let messages = vec![
            Message {
                payload: "message_1_payload".as_bytes().to_vec(),
            },
            Message {
                payload: "message_2_payload".as_bytes().to_vec(),
            },
        ];
        let batch = MessageBatch {
            topic_name: "topic1".to_string(),
            messages,
            ack_level: AckLevel::All,
        };

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<PartitionResponse>();
        main_tx
            .send(PartitionCommand::Write {
                batch,
                response_tx: oneshot_tx,
            })
            .await
            .unwrap();

        let response = oneshot_rx.await.unwrap();
        assert_eq!(response, PartitionResponse::AllAcknowledged);

        cancellation_token.cancel();
        leader_task_handle.await.unwrap();
    }
}
