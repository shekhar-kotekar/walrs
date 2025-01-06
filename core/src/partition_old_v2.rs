use common::models::MessageBatch;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::models::{PartitionMessageBatch, PartitionRole};

pub struct Partition {
    pub number: u8,
    pub topic_name: String,
    role: PartitionRole,
}

// Kafka replication is pull based, not push based.
// Followers periodically send pull request for messages to the leader.
// Reference: https://cwiki.apache.org/confluence/display/kafka/kafka+replication
impl Partition {
    pub async fn start(
        &self,
        mut rx: mpsc::Receiver<PartitionMessageBatch>,
        cancellation_token: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Partition {} for topic {} is cancelled.", self.number, self.topic_name);
                    break;
                }
                Some(message_batch) = rx.recv() => {
                    match &self.role {
                        PartitionRole::Leader(leader_partition) => {
                            let external_message_batch = MessageBatch {
                                messages: message_batch.messages.clone(),
                                ack_level: message_batch.ack_level,
                            };
                            self.send_message_batch_to_followers(leader_partition.followers.clone(), external_message_batch);
                        }
                        PartitionRole::Follower(_) => {
                            // Send messages to leader
                        }
                    }
                }
            }
        }
        tracing::info!(
            "Partition {} for topic {} is stopped.",
            self.number,
            self.topic_name
        );
    }

    fn send_message_batch_to_followers(&self, followers: Vec<String>, message_batch: MessageBatch) {
        for follower in followers {
            // Send messages to followers
        }
    }
    async fn send_message_batch_to(&self, follower_address: &str, message_batch: MessageBatch) {
        let listener = TcpListener::bind(follower_address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (mut reader, mut writer) = socket.into_split();

        let serialized_message_batch = bincode::serialize(&message_batch).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::models::{FollowerPartition, LeaderPartition, ParitionResponse};

    use super::*;
    use common::models::{AcknowledgementLevel, Message};
    use tokio::sync::oneshot;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn leader_partition_should_acknowledge_messages_immediately_when_ack_level_set_to_none() {
        let leader_partition = LeaderPartition {
            followers: vec!["0.0.0.0:5057".to_string()],
        };
        let lead_partition = Partition {
            number: 0,
            topic_name: "test_topic".to_string(),
            role: PartitionRole::Leader(leader_partition),
        };
        let cancellation_token = CancellationToken::new();
        let lead_partition_ct = cancellation_token.child_token();
        let (leader_tx, leader_rx) = mpsc::channel::<PartitionMessageBatch>(1);
        let lead_partition_handle = tokio::spawn(async move {
            lead_partition.start(leader_rx, lead_partition_ct).await;
        });

        let messages = vec![
            Message {
                payload: "message_1_payload".as_bytes().to_vec(),
                topic_name: "test_topic".to_string(),
            },
            Message {
                payload: "message_2_payload".as_bytes().to_vec(),
                topic_name: "test_topic".to_string(),
            },
        ];

        let (tx, rx) = oneshot::channel::<ParitionResponse>();
        let message_batch = PartitionMessageBatch {
            messages,
            ack_level: AcknowledgementLevel::Leader,
            tx,
        };

        leader_tx.send(message_batch).await.unwrap();
        let response = rx.await.unwrap();

        cancellation_token.cancel();
        lead_partition_handle.await.unwrap();

        assert_eq!(
            response,
            ParitionResponse::Acknowledged { message_count: 2 }
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn leader_partition_should_acknowledge_messages_only_after_all_followers_acknowledge_when_ack_level_set_to_all(
    ) {
        let leader_partition = LeaderPartition {
            followers: vec!["0.0.0.0:5057".to_string()],
        };
        let lead_partition = Partition {
            number: 0,
            topic_name: "test_topic".to_string(),
            role: PartitionRole::Leader(leader_partition),
        };
        let cancellation_token = CancellationToken::new();
        let lead_partition_ct = cancellation_token.child_token();

        let (leader_tx, leader_rx) = mpsc::channel::<PartitionMessageBatch>(1);
        let lead_partition_handle = tokio::spawn(async move {
            lead_partition.start(leader_rx, lead_partition_ct).await;
        });

        let follower_partition = Partition {
            number: 1,
            topic_name: "test_topic".to_string(),
            role: PartitionRole::Follower(FollowerPartition {}),
        };

        let follower_partition_ct = cancellation_token.child_token();
        let (_, follower_rx) = mpsc::channel::<PartitionMessageBatch>(1);
        let follower_partition_handle = tokio::spawn(async move {
            follower_partition
                .start(follower_rx, follower_partition_ct)
                .await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let messages = vec![
            Message {
                payload: "message_1_payload".as_bytes().to_vec(),
                topic_name: "test_topic".to_string(),
            },
            Message {
                payload: "message_2_payload".as_bytes().to_vec(),
                topic_name: "test_topic".to_string(),
            },
        ];

        let (tx, rx) = oneshot::channel::<ParitionResponse>();
        let message_batch = PartitionMessageBatch {
            messages,
            ack_level: AcknowledgementLevel::All,
            tx,
        };

        leader_tx.send(message_batch).await.unwrap();

        let response = rx.await.unwrap();

        assert_eq!(
            response,
            ParitionResponse::Acknowledged { message_count: 2 }
        );

        cancellation_token.cancel();
        lead_partition_handle.await.unwrap();
        follower_partition_handle.await.unwrap();
    }
}
