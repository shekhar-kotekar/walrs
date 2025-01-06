use common::models::MessageBatch;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::models::{PartitionCommand, PartitionResponse};

pub struct PartitionLeader {
    pub num_partition: u8,
    pub topic_name: String,
    pub followers: Vec<String>,
}

impl PartitionLeader {
    pub async fn start(
        &self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::WriteMessageBatch { batch, response_tx } => {
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
    }
    async fn write_message_batch(&self, batch: MessageBatch) -> PartitionResponse {
        PartitionResponse::LeaderAcknowledged
    }
}

#[cfg(test)]
mod tests {
    use common::models::{AcknowledgementLevel, Message, MessageBatch};
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;

    use crate::models::{PartitionCommand, PartitionResponse};

    use super::*;

    #[tokio::test]
    #[traced_test]
    async fn partition_leader_should_be_able_to_write_message_batches() {
        let partition_leader = PartitionLeader {
            num_partition: 0,
            topic_name: "topic1".to_string(),
            followers: vec![],
        };
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
        let message_batch = MessageBatch {
            topic_name: "topic1".to_string(),
            messages,
            ack_level: AcknowledgementLevel::Leader,
        };

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<PartitionResponse>();
        main_tx
            .send(PartitionCommand::WriteMessageBatch {
                batch: message_batch,
                response_tx: oneshot_tx,
            })
            .await
            .unwrap();

        let response = oneshot_rx.await.unwrap();
        assert_eq!(response, PartitionResponse::LeaderAcknowledged);

        cancellation_token.cancel();
        leader_task_handle.await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn partition_leader_should_acknowledge_from_majority_followers_when_ack_level_set_to_majority(
    ) {
        let partition_leader = PartitionLeader {
            num_partition: 0,
            topic_name: "topic1".to_string(),
            followers: vec![],
        };

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
        let message_batch = MessageBatch {
            topic_name: "topic1".to_string(),
            messages,
            ack_level: AcknowledgementLevel::Majority,
        };

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<PartitionResponse>();
        main_tx
            .send(PartitionCommand::WriteMessageBatch {
                batch: message_batch,
                response_tx: oneshot_tx,
            })
            .await
            .unwrap();

        let response = oneshot_rx.await.unwrap();
        assert_eq!(response, PartitionResponse::MajorityAcknowledged);

        cancellation_token.cancel();
        leader_task_handle.await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn partition_leader_should_acknowledge_from_all_followers_when_ack_level_set_to_all() {
        let partition_leader = PartitionLeader {
            num_partition: 0,
            topic_name: "topic1".to_string(),
            followers: vec![],
        };

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
        let message_batch = MessageBatch {
            topic_name: "topic1".to_string(),
            messages,
            ack_level: AcknowledgementLevel::Majority,
        };

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<PartitionResponse>();
        main_tx
            .send(PartitionCommand::WriteMessageBatch {
                batch: message_batch,
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
