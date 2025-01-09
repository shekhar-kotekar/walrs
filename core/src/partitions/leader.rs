use common::models::MessageBatch;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::models::{PartitionCommand, PartitionResponse};

pub struct PartitionLeader {
    pub num_partition: u8,
    pub topic_name: String,
    in_sync_replicas: Vec<String>,
    high_watermark: u64,
}

impl PartitionLeader {
    pub fn new(num_partition: u8, topic_name: String) -> Self {
        PartitionLeader {
            num_partition,
            topic_name,
            in_sync_replicas: vec![],
            high_watermark: 0,
        }
    }

    pub async fn start(
        &self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!(
            "Partition leader for {} topic, partition {} started.",
            self.topic_name,
            self.num_partition
        );
        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::Write { batch, response_tx } => {
                            let response = self.write_message_batch(batch).await;
                            response_tx.send(response).unwrap();
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

    async fn write_message_batch(&self, batch: MessageBatch) -> PartitionResponse {
        PartitionResponse::LeaderAcknowledged
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
        let partition_leader = PartitionLeader::new(0, "topic_1".to_string());

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
        let partition_leader = PartitionLeader::new(0, "topic_1".to_string());

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
