use std::collections::HashMap;

use commons::models::{NodeInfo, Topic};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    handlers::{
        create_topic::{self, TopicManagerResponse},
        heartbeat,
    },
    models::{ClusterInfo, NodeConfig, PartitionCommand},
    partition_managers::partition_reader::PartitionReader,
};

// this is broker struct in old code.
pub struct NodeManager {
    node_config: NodeConfig,
    cluster_info: ClusterInfo,
    heartbeat_interval: tokio::time::Interval,
    local_partition_writers: HashMap<String, mpsc::Sender<PartitionCommand>>,
    local_partition_readers: HashMap<String, mpsc::Sender<PartitionCommand>>,
    topic_metadata: HashMap<String, Topic>,
}

impl NodeManager {
    pub fn new(node_config: NodeConfig, cluster_info: ClusterInfo) -> Self {
        let data_dir_path = format!(
            "{}/{}",
            node_config.base_path_for_data,
            node_config.ip.replace(".", "_")
        );
        std::fs::create_dir_all(&data_dir_path).unwrap_or_else(|e| {
            panic!(
                "Failed to create base directory: {} because: {}",
                &data_dir_path, e
            );
        });
        let heartbeat_interval = tokio::time::interval(std::time::Duration::from_millis(
            node_config.heartbeat_interval_ms.into(),
        ));

        Self {
            node_config,
            cluster_info,
            heartbeat_interval,
            local_partition_writers: HashMap::new(),
            local_partition_readers: HashMap::new(),
            topic_metadata: HashMap::new(),
        }
    }
}

impl NodeManager {
    pub async fn start(
        &mut self,
        mut rx: mpsc::Receiver<NodeManagerCommand>,
        cancellation_token: CancellationToken,
    ) {
        let (broker_to_topic_creator_tx, mut broker_to_topic_creator_rx) =
            mpsc::channel::<TopicManagerResponse>(10);

        tracing::info!("Node manager started.");
        loop {
            tokio::select! {
                Some(response) = broker_to_topic_creator_rx.recv() => {
                    tracing::debug!("Received topic creator response: {:?}", response);
                    match response {
                        TopicManagerResponse::TopicCreated { topic, partition_zero_tx } => {
                            tracing::info!("Topic created successfully: {:?}", topic);
                            self.topic_metadata.insert(topic.name.clone(), topic.clone());
                            self.local_partition_writers.insert(
                                topic.name,
                                partition_zero_tx,
                            );
                        }
                        TopicManagerResponse::TopicCreationFailed { topic_name, error } => {
                            tracing::error!("Error creating topic {}: {}", topic_name, error);
                        }
                    }
                }
                Some(command) = rx.recv() => {
                    tracing::debug!("Received admin command: {:?}", command);
                    match command {
                        NodeManagerCommand::CreateTopic {topic} => {
                            tracing::info!("Creating topic: {:?}", topic);
                            if self.topic_metadata.contains_key(&topic.name) {
                                tracing::warn!("Topic {} already exists.", topic.name);
                            } else {
                                let topic_creator_cancellation_token = cancellation_token.child_token();
                                let node_address_clone = self.node_config.ip.clone();
                                let local_data_dir_path = self.node_config.base_path_for_data.clone();
                                let cluster_info = self.cluster_info.clone();

                                let broker_to_topic_creator_tx_clone = broker_to_topic_creator_tx.clone();
                                tokio::spawn(async move {
                                    create_topic::create_topic(
                                        topic,
                                        node_address_clone,
                                        local_data_dir_path,
                                        cluster_info,
                                        broker_to_topic_creator_tx_clone,
                                        topic_creator_cancellation_token,
                                    )
                                    .await;
                                });
                            }
                        }
                        NodeManagerCommand::GetTopicInfo { topic_name, tx } => {
                            tracing::info!("Getting info for topic: {}", topic_name);
                            if let Some(topic) = self.topic_metadata.get(&topic_name) {
                                tx.send(NodeManagerResponse::TopicInfo { topic: topic.clone() }).unwrap_or_else(|_| {
                                    tracing::warn!("Failed to send topic info response.");
                                });
                            } else {
                                tx.send(NodeManagerResponse::NotFound).unwrap_or_else(|_| {
                                    tracing::warn!("Failed to send topic not found response.");
                                });
                            }
                        }
                        NodeManagerCommand::GetPartitionWriter { topic_name, tx } => {
                            tracing::info!("Getting partition writer for topic: {}", topic_name);
                            let response = if let Some(writer) = self.local_partition_writers.get(&topic_name) {
                                 NodeManagerResponse::PartitionWriter {
                                    writer: writer.clone(),
                                }
                            } else {
                                tracing::warn!("No partition writer found for topic: {}", topic_name);
                                NodeManagerResponse::NotFound
                            };
                            tx.send(response).unwrap_or_else(|_| {
                                tracing::warn!("Failed to send partition writer response.");
                            });
                        }
                        NodeManagerCommand::GetPartitionReader { topic_name, tx } => {
                            self.handle_get_partition_reader(topic_name, tx, cancellation_token.child_token()).await;
                        }
                        NodeManagerCommand::Heartbeat { peer_info, tx } => {
                            tracing::info!("Received heartbeat from peer: {:?}", peer_info);
                            self.cluster_info.add_node(peer_info);
                            tx.send(NodeManagerResponse::HeartbeatAcknowledged).unwrap_or_else(|_| {
                                tracing::warn!("Failed to send heartbeat acknowledgment.");
                            });
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token cancelled. Node manager shutting down.",);
                    break;
                }
                _ = self.heartbeat_interval.tick() => {
                    tracing::info!("Sending heartbeat to cluster: {:?}", self.cluster_info);
                    let cluster_info = self.cluster_info.clone();
                    let self_address = format!("{}:{}", self.node_config.ip, self.node_config.port);
                    tokio::spawn(async move {heartbeat::send_heartbeat(self_address, cluster_info).await;});
                }
            }
        }
        tracing::info!("Node manager stopped.");
    }

    async fn handle_get_partition_reader(
        &mut self,
        topic_name: String,
        tx: oneshot::Sender<NodeManagerResponse>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!("Getting partition reader for topic: {}", topic_name);
        let response = if let Some(reader) = self.local_partition_readers.get(&topic_name) {
            NodeManagerResponse::PartitionReader {
                reader: reader.clone(),
            }
        } else {
            let partition_number = 0; // Assuming we want the reader for partition 0
            let message_batch_size = 10; // Default batch size, can be adjusted as needed
            let mut partition_reader: PartitionReader = PartitionReader::new(
                topic_name.clone(),
                partition_number,
                self.node_config.base_path_for_data.clone(),
                message_batch_size,
            );

            let (partition_reader_tx, partition_reader_rx) =
                mpsc::channel::<PartitionCommand>(self.node_config.mpsc_queue_size);

            tokio::spawn(async move {
                partition_reader
                    .start(partition_reader_rx, cancellation_token)
                    .await;
            });
            self.local_partition_readers
                .insert(topic_name.clone(), partition_reader_tx.clone());
            NodeManagerResponse::PartitionReader {
                reader: partition_reader_tx,
            }
        };
        tx.send(response).unwrap_or_else(|_| {
            tracing::warn!("Failed to send partition reader response.");
        });
    }
}

#[derive(Debug)]
pub enum NodeManagerCommand {
    CreateTopic {
        topic: Topic,
    },
    GetTopicInfo {
        topic_name: String,
        tx: oneshot::Sender<NodeManagerResponse>,
    },
    GetPartitionWriter {
        topic_name: String,
        tx: oneshot::Sender<NodeManagerResponse>,
    },
    GetPartitionReader {
        topic_name: String,
        tx: oneshot::Sender<NodeManagerResponse>,
    },
    Heartbeat {
        peer_info: NodeInfo,
        tx: oneshot::Sender<NodeManagerResponse>,
    },
}

#[derive(Debug)]
pub enum NodeManagerResponse {
    TopicInfo {
        topic: Topic,
    },
    NotFound,
    HeartbeatAcknowledged,
    PartitionWriter {
        writer: mpsc::Sender<PartitionCommand>,
    },
    PartitionReader {
        reader: mpsc::Sender<PartitionCommand>,
    },
}
