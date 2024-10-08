use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};

use common::models::{Message, Topic};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::managers::partition_manager::start_partition_writer;
use crate::models::PartitionInfo;

const PARTITION_MANAGER_CHANNEL_SIZE: usize = 1000;

pub struct TopicsManager {
    topics: HashMap<String, Topic>,
    cancellation_token: CancellationToken,
    log_dir_path: String,
    partition_client_tx: HashMap<String, Sender<Message>>,
    partition_manager_task_tracker: TaskTracker,
}

impl TopicsManager {
    pub fn new(log_dir_path: String, cancellation_token: CancellationToken) -> Self {
        TopicsManager {
            topics: HashMap::new(),
            cancellation_token,
            log_dir_path,
            partition_client_tx: HashMap::new(),
            partition_manager_task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start_topics_manager(&mut self, mut parent_rx: Receiver<TopicManagerCommands>) {
        tracing::info!("Topic Manager started");
        loop {
            tokio::select! {
                    Some(command) = parent_rx.recv() => {
                        match command {
                            TopicManagerCommands::CreateTopic { topic, reply_tx } => {
                                self.create_topic(topic, reply_tx).await;
                            }
                            TopicManagerCommands::GetPartitionManagerTx {
                                topic_name,
                                message_key,
                                reply_tx,
                            } => {
                                let partition_index = message_key
                                    .as_ref()
                                    .map(|key| {
                                        let mut hasher = DefaultHasher::new();
                                        hasher.write(key.as_bytes());
                                        let topic = self.topics.get(topic_name.as_str()).unwrap();
                                        (hasher.finish() % topic.num_partitions.unwrap() as u64) as u8
                                    })
                                    .unwrap_or(0);
                                let partition_name = format!("{}-{}", topic_name, partition_index);
                                if self.partition_client_tx.contains_key(&partition_name) {
                                    let client_tx = self.partition_client_tx.get(&partition_name).unwrap();
                                    reply_tx.send(Some(client_tx.to_owned())).unwrap();
                                } else {
                                    reply_tx.send(None).unwrap();
                                }
                            }
                            TopicManagerCommands::GetTopicInfo {
                                topic_name,
                                reply_tx,
                            } => {
                                if self.topics.contains_key(topic_name.as_str()) {
                                    let topic = self.topics.get(&topic_name);
                                    reply_tx.send(topic.cloned()).unwrap();
                                } else {
                                    reply_tx.send(None).unwrap();
                                }
                            }
                        }
                    }
                    _ = self.cancellation_token.cancelled() => {
                        tracing::info!("Cancellation token received for topic manager.");
                        self.partition_manager_task_tracker.close();
                        self.partition_manager_task_tracker.wait().await;
                        break;
                }
            }
        }
    }

    async fn create_topic(&mut self, topic: Topic, reply_tx: oneshot::Sender<Option<Topic>>) {
        let topic_name = topic.name.clone();

        if self.topics.contains_key(topic_name.as_str()) {
            tracing::warn!("{} Topic already exists", topic_name);
            let topic = self.topics.get(topic_name.as_str()).unwrap().to_owned();
            reply_tx.send(Some(topic)).unwrap();
        } else {
            for partition_index in 0..topic.num_partitions.unwrap() {
                let partition_name = format!("{}-{}", topic_name, partition_index);
                let (client_tx, client_rx) =
                    mpsc::channel::<Message>(PARTITION_MANAGER_CHANNEL_SIZE);
                self.partition_client_tx
                    .insert(partition_name.clone(), client_tx);
                let partition =
                    PartitionInfo::new(topic.clone(), partition_index, self.log_dir_path.clone());
                let cancellation_token_for_partition = self.cancellation_token.clone();
                self.partition_manager_task_tracker.spawn(async move {
                    start_partition_writer(partition, client_rx, cancellation_token_for_partition)
                        .await;
                });
            }
            self.topics.insert(topic_name.clone(), topic.clone());
            tracing::info!("{} Topic created", topic_name);
            reply_tx.send(Some(topic)).unwrap();
        }
    }
}

pub enum TopicManagerCommands {
    CreateTopic {
        topic: Topic,
        reply_tx: oneshot::Sender<Option<Topic>>,
    },
    GetTopicInfo {
        topic_name: String,
        reply_tx: oneshot::Sender<Option<Topic>>,
    },
    GetPartitionManagerTx {
        topic_name: String,
        message_key: Option<String>,
        reply_tx: oneshot::Sender<Option<Sender<Message>>>,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use bytes::BytesMut;
    use common::{codecs::decoder::BatchDecoder, models::Message};
    use test_log::test;
    use tokio_util::codec::Decoder;

    #[test(tokio::test)]
    async fn test_topics_manager_should_return_partition_manager() {
        let temp_dir = tempdir::TempDir::new("log_dir_").unwrap();
        let log_dir_path = temp_dir.path().to_str().unwrap().to_string();
        let (parent_tx, parent_rx) = mpsc::channel(5);
        let cancellation_token = CancellationToken::new();

        let mut topics_manager =
            TopicsManager::new(log_dir_path.clone(), cancellation_token.clone());

        let topic_name = "test_topic".to_string();

        let topic = Topic {
            name: topic_name.clone(),
            num_partitions: Some(1),
            replication_factor: Some(1),
            retention_period: Some(1),
            batch_size: Some(2),
        };

        let topic_manager_handle = tokio::spawn(async move {
            topics_manager.start_topics_manager(parent_rx).await;
        });

        let (reply_tx, reply_rx) = oneshot::channel();
        parent_tx
            .send(TopicManagerCommands::CreateTopic {
                topic: topic.clone(),
                reply_tx: reply_tx,
            })
            .await
            .unwrap();

        let topic_received = reply_rx.await.unwrap().unwrap();
        assert_eq!(topic_received, topic);
        assert_eq!(topic.name, topic_name.clone());

        let (reply_tx, reply_rx) = oneshot::channel();
        let get_partition_manager_command = TopicManagerCommands::GetPartitionManagerTx {
            topic_name: topic_name.clone(),
            message_key: None,
            reply_tx: reply_tx,
        };

        parent_tx.send(get_partition_manager_command).await.unwrap();
        let partition_manager_tx = reply_rx.await.unwrap().unwrap();

        let message_1 = Message {
            payload: BytesMut::from("Message 1 without timestamp".as_bytes()).freeze(),
            key: Some("dummy_key".to_string()),
            timestamp: None,
        };

        let message_2 = Message {
            payload: BytesMut::from("Message 2 with timestamp".as_bytes()).freeze(),
            key: None,
            timestamp: Some(1234567890),
        };

        let message_3 = Message {
            payload: BytesMut::from("Message 3 with timestamp".as_bytes()).freeze(),
            key: Some("dummy_key_2".to_string()),
            timestamp: Some(1334567899),
        };
        partition_manager_tx.send(message_1.clone()).await.unwrap();
        partition_manager_tx.send(message_2.clone()).await.unwrap();
        partition_manager_tx.send(message_3.clone()).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        cancellation_token.cancel();

        topic_manager_handle.await.unwrap();

        let segment_file_path = format!("{}/0/{}", log_dir_path, "segment_0.log");
        tracing::info!("in test - Segment file path: {}", segment_file_path);
        let file_contents = fs::read(segment_file_path).unwrap();
        tracing::info!("in test - File contents: {:?}", file_contents);
        let mut batch_decoder = BatchDecoder {};
        let mut src = BytesMut::from(file_contents.as_slice());

        let mut decoded_batches = Vec::new();

        while let Some(decoded_batch) = batch_decoder.decode(&mut src).unwrap() {
            tracing::info!("Decoded batch: {:?}", decoded_batch);
            decoded_batches.push(decoded_batch);
        }
        assert_eq!(decoded_batches.len(), 2);
        assert_eq!(decoded_batches[0].records[0], message_1);
        assert_eq!(decoded_batches[0].records[1], message_2);
        assert_eq!(decoded_batches[1].records[0], message_3);
    }
}
