use std::collections::HashMap;

use commons::models::{NodeInfo, PartitionRole, Topic, TopicStatus};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    handlers::{
        create_topic::{self, TopicManagerResponse},
        heartbeat,
    },
    models::{ClusterInfo, NodeConfig, PartitionCommand},
    partition_managers::{partition_reader::PartitionReader, partition_writer::PartitionWriter},
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
        std::fs::create_dir_all(&node_config.base_path_for_data).unwrap_or_else(|e| {
            panic!(
                "Failed to create base directory: {} because: {}",
                &node_config.base_path_for_data, e
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
                            self.topic_metadata.remove(&topic_name);
                        }
                    }
                }
                Some(command) = rx.recv() => {
                    match command {
                        NodeManagerCommand::CreateTopic {mut topic} => {
                            tracing::info!("Creating topic: {:?}", topic);
                            if self.topic_metadata.contains_key(&topic.name) {
                                tracing::warn!("Topic {} already exists.", topic.name);
                            } else {
                                let topic_creator_cancellation_token = cancellation_token.child_token();
                                let node_address_clone = self.node_config.address.clone();
                                let local_data_dir_path = self.node_config.base_path_for_data.clone();
                                let cluster_info = self.cluster_info.clone();

                                let broker_to_topic_creator_tx_clone = broker_to_topic_creator_tx.clone();
                                let topic_clone = topic.clone();
                                tokio::spawn(async move {
                                    create_topic::create_topic(
                                        topic_clone,
                                        node_address_clone,
                                        local_data_dir_path,
                                        cluster_info,
                                        broker_to_topic_creator_tx_clone,
                                        topic_creator_cancellation_token,
                                    )
                                    .await;
                                });
                                topic.status = TopicStatus::CreationInProgress;
                                self.topic_metadata.insert(topic.name.clone(), topic);
                            }
                        }
                        NodeManagerCommand::GetTopicInfo { topic_name, tx } => {
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
                        NodeManagerCommand::CreatePartitionWriter {
                            topic_name,
                            partition_number,
                            role,
                            tx,
                        } => {
                            self.handle_create_partition_writer(
                                topic_name,
                                partition_number,
                                role,
                                tx,
                                cancellation_token.child_token(),
                            ).await;
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
                    let cluster_info = self.cluster_info.clone();
                    let self_address = self.node_config.address.clone();
                    tokio::spawn(async move {heartbeat::send_heartbeat(self_address, cluster_info).await;});
                }
            }
        }
        tracing::info!("Node manager stopped.");
    }

    async fn handle_create_partition_writer(
        &mut self,
        topic_name: String,
        partition_number: u8,
        role: PartitionRole,
        tx: oneshot::Sender<NodeManagerResponse>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!(
            "Creating partition writer for topic: {}, partition: {}, role: {:?}",
            topic_name,
            partition_number,
            role
        );

        let response: NodeManagerResponse =
            if self.local_partition_writers.contains_key(&topic_name) {
                NodeManagerResponse::PartitionWriterAlreadyExists
            } else {
                let (partition_writer_tx, partition_writer_rx) =
                    mpsc::channel::<PartitionCommand>(10);

                let mut partition_writer = PartitionWriter::new(
                    &topic_name,
                    partition_number,
                    &self.node_config.base_path_for_data,
                );
                tokio::spawn(async move {
                    partition_writer
                        .start(partition_writer_rx, cancellation_token)
                        .await;
                });

                self.local_partition_writers
                    .insert(topic_name.clone(), partition_writer_tx.clone());
                NodeManagerResponse::PartitionWriterCreated
            };
        tx.send(response).unwrap_or_else(|_| {
            tracing::error!("Failed to send partition writer response.");
        });
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
    CreatePartitionWriter {
        topic_name: String,
        partition_number: u8,
        role: PartitionRole,
        tx: oneshot::Sender<NodeManagerResponse>,
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
    PartitionWriterCreated,
    PartitionWriterAlreadyExists,
    PartitionWriter {
        writer: mpsc::Sender<PartitionCommand>,
    },
    PartitionReader {
        reader: mpsc::Sender<PartitionCommand>,
    },
}
