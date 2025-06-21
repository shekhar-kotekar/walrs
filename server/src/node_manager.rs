use std::collections::{HashMap, hash_map::Entry};

use commons::models::{NodeInfo, PartitionInfo, PartitionRole, Topic, TopicStatus};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    handlers::{
        create_partitions::create_local_partition,
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

    //key: topic name, value: partition writer
    local_partition_writers: HashMap<String, mpsc::Sender<PartitionCommand>>,

    //key: topic name, value: partition reader
    local_partition_readers: HashMap<String, mpsc::Sender<PartitionCommand>>,

    // key: topic name, value: Topic metadata
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
                    match response {
                        TopicManagerResponse::TopicCreated { topic, partition_zero_tx } => {
                            let key_name = format!("{}-{}", topic.name, 0);
                            self.topic_metadata.insert(key_name.clone(), topic.clone());
                            self.local_partition_writers.insert(
                                key_name,
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
                        NodeManagerCommand::CreateTopic { topic} => {
                            self.handle_create_topic(
                                topic,
                                cancellation_token.child_token(),
                                broker_to_topic_creator_tx.clone(),
                            ).await;
                        }
                        NodeManagerCommand::GetTopicInfo { topic_names, tx } => {
                            let topics = self.get_topic_info(topic_names).await;
                            tx.send(NodeManagerResponse::TopicInfo { topics }).unwrap_or_else(|_| {
                                tracing::warn!("Failed to send topic info response.");
                            });
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
                        NodeManagerCommand::GetPartitionWriter { topic_name, partition_number, tx } => {
                            self.handle_get_partition_writer(topic_name, partition_number, tx).await;
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

    async fn get_topic_info(&self, topic_names: Vec<String>) -> Vec<Topic> {
        self.topic_metadata
            .iter()
            .filter_map(|(_, topic)| {
                if topic_names.contains(&topic.name) {
                    Some(topic.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    async fn handle_create_topic(
        &mut self,
        mut topic: Topic,
        cancellation_token: CancellationToken,
        broker_to_topic_creator_tx: mpsc::Sender<TopicManagerResponse>,
    ) {
        let key_name = format!("{}-{}", topic.name, 0);
        if let Entry::Vacant(_) = self.topic_metadata.entry(key_name.clone()) {
            let topic_creator_cancellation_token = cancellation_token.child_token();
            let node_address_clone = self.node_config.address.clone();
            let local_data_dir_path = self.node_config.base_path_for_data.clone();
            let cluster_info = self.cluster_info.clone();

            let topic_clone = topic.clone();
            tokio::spawn(async move {
                create_topic::create_topic(
                    topic_clone,
                    node_address_clone,
                    local_data_dir_path,
                    cluster_info,
                    broker_to_topic_creator_tx,
                    topic_creator_cancellation_token,
                )
                .await;
            });
            topic.status = TopicStatus::CreationInProgress;
            self.topic_metadata.insert(key_name, topic.clone());
            let mut self_node_info: NodeInfo = self
                .cluster_info
                .nodes
                .iter()
                .find(|(node_address, _)| **node_address == self.node_config.address)
                .map(|(_, node_info)| node_info.clone())
                .unwrap();
            self_node_info.registed_topics.push(topic.name.clone());
            self.cluster_info
                .nodes
                .insert(self.node_config.address.clone(), self_node_info);
        } else {
            tracing::warn!("Topic {} already exists.", key_name);
        }
    }

    async fn handle_get_partition_writer(
        &mut self,
        topic_name: String,
        partition_number: u8,
        tx: oneshot::Sender<NodeManagerResponse>,
    ) {
        let key = format!("{}-{}", topic_name, partition_number);
        tracing::debug!("Getting partition writer for: {}", key);

        let response = if let Some(writer) = self.local_partition_writers.get(&key) {
            let topic_metadata = self.topic_metadata.get(&topic_name).unwrap();
            let partition_info: &PartitionInfo = topic_metadata
                .partitions
                .iter()
                .find(|partition| partition.leader_address == self.node_config.address)
                .unwrap();
            assert_eq!(
                partition_info.number, partition_number,
                "Partition number mismatch for topic: {}",
                topic_name
            );
            NodeManagerResponse::PartitionWriter {
                writer: writer.clone(),
            }
        } else {
            tracing::warn!("No partition writer found for: {}", key);
            NodeManagerResponse::NotFound
        };
        tx.send(response).unwrap_or_else(|_| {
            tracing::warn!("Failed to send partition writer response.");
        });
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
            "Creating partition writer. Topic: {}, partition: {}, role: {:?}",
            topic_name,
            partition_number,
            role
        );
        let key_name = format!("{}-{}", topic_name, partition_number);
        let response: NodeManagerResponse =
            match self.local_partition_writers.entry(key_name.clone()) {
                Entry::Occupied(_) => {
                    tracing::warn!("Partition writer already exists: {}", key_name);
                    NodeManagerResponse::PartitionWriterAlreadyExists
                }
                Entry::Vacant(entry) => {
                    match create_local_partition(
                        &topic_name,
                        partition_number,
                        &self.node_config.address,
                        role.clone(),
                        &self.node_config.base_path_for_data,
                        cancellation_token,
                    )
                    .await
                    {
                        Some(partition_writer_tx) => {
                            entry.insert(partition_writer_tx.clone());
                            NodeManagerResponse::PartitionWriterCreated
                        }
                        None => NodeManagerResponse::Error {
                            message: format!(
                                "partition not created for topic: {}, partition: {}, role: {:?}",
                                topic_name, partition_number, role
                            ),
                        },
                    }
                }
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
        topic_names: Vec<String>,
        tx: oneshot::Sender<NodeManagerResponse>,
    },
    GetPartitionWriter {
        topic_name: String,
        partition_number: u8,
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
        topics: Vec<Topic>,
    },
    NotFound,
    Error {
        message: String,
    },
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
