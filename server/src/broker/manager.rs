use std::collections::HashMap;

use common::models::TopicMetadata;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::heartbeat,
    models::{BrokerInfo, BrokerResponse, ClusterInfo, CommandToBroker, PartitionCommand},
    partition_managers::partition_reader::PartitionReader,
    topic_managers::create_topic::{self, TopicManagerResponse},
    MPSC_MAX_Q_SIZE,
};

pub struct Broker {
    cluster_info: ClusterInfo,
    data_dir_path: String,
    address: String,
    heartbeat_interval: tokio::time::Interval,
    local_partition_writers: HashMap<String, mpsc::Sender<PartitionCommand>>,
    local_partition_readers: HashMap<String, mpsc::Sender<PartitionCommand>>,
    topic_metadata: HashMap<String, TopicMetadata>,
}

impl Broker {
    pub fn new(
        ip: String,
        peer_listener_port: u16,
        peers: Vec<String>,
        data_dir_path: String,
        heartbeat_interval_ms: u16,
    ) -> Self {
        let data_dir_path = format!(
            "{}/{}-{}",
            data_dir_path,
            ip.replace([':', '.'], "-"),
            peer_listener_port
        );
        std::fs::create_dir_all(&data_dir_path).unwrap_or_else(|e| {
            panic!(
                "Failed to create base directory: {} because: {}",
                &data_dir_path, e
            );
        });

        let self_address = format!("{}:{}", ip, peer_listener_port);
        let self_info = BrokerInfo::new();

        let mut cluster_info = ClusterInfo::new().with_peers(peers);
        cluster_info.add_broker(self_address.clone(), self_info);

        let heartbeat_interval = tokio::time::interval(std::time::Duration::from_millis(
            heartbeat_interval_ms as u64,
        ));

        Broker {
            cluster_info,
            data_dir_path,
            address: self_address,
            heartbeat_interval,
            local_partition_writers: HashMap::new(),
            local_partition_readers: HashMap::new(),
            topic_metadata: HashMap::new(),
        }
    }
    pub async fn start(
        &mut self,
        mut command_rx: mpsc::Receiver<CommandToBroker>,
        cancellation_token: CancellationToken,
    ) {
        let (broker_to_topic_creator_tx, mut broker_to_topic_creator_rx) =
            mpsc::channel::<TopicManagerResponse>(MPSC_MAX_Q_SIZE);
        loop {
            tokio::select! {
                Some(command) = broker_to_topic_creator_rx.recv() => {
                    match command {
                        TopicManagerResponse::TopicCreated { topic_metadata, partition_zero_tx } => {
                            tracing::info!("*** Topic is ready to serve: {:?}", &topic_metadata);
                            self.topic_metadata.insert(topic_metadata.name.clone(), topic_metadata.clone());
                            self.local_partition_writers.insert(
                                topic_metadata.name.clone(),
                                partition_zero_tx,
                            );
                        }
                        TopicManagerResponse::TopicCreationFailed { topic_name, error } => {
                            tracing::error!("Failed to create topic: {} due to: {}", topic_name, error);
                        }
                    }
                },
                Some(command) = command_rx.recv() => {
                    match command {
                        CommandToBroker::GetTopicStatus { topic_name, broker_tx } => {
                            tracing::info!("Received request for topic status: {}", topic_name);
                            if let Some(topic_metadata) = self.topic_metadata.get(&topic_name) {
                                broker_tx.send(BrokerResponse::TopicCreated {
                                    topic_metadata: topic_metadata.clone(),
                                }).unwrap_or_else(|e| {
                                    tracing::error!("Failed to send broker response: {:?}", e);
                                });
                            } else {
                                broker_tx.send(BrokerResponse::BrokerError {
                                    message: format!("Topic {} not found", topic_name),
                                }).unwrap_or_else(|e| {
                                    tracing::error!("Failed to send broker response: {:?}", e);
                                });
                            }
                        }
                        CommandToBroker::CreateNewTopic { topic, broker_tx } => {
                            if self.topic_metadata.contains_key(&topic.name) {
                                tracing::warn!("Topic {} already exists. Ignoring create request.", topic.name);
                                broker_tx.send(BrokerResponse::TopicAlreadyExists).unwrap_or_else(|e| {
                                    tracing::error!("Failed to send broker response: {:?}", e);
                                });
                                continue;
                            }
                            let self_address = self.address.clone();
                            let self_data_dir_path = self.data_dir_path.clone();
                            let cancellation_token = cancellation_token.clone();
                            let cluster_info = self.cluster_info.clone();
                            let topic_to_create = topic.clone();
                            tracing::debug!("Received create topic command from peer: {}, topic: {}. Creating separate task for this.",
                                self_address, topic.name);

                            // Immediately return InProgress to the sender to avoid blocking the command processing loop.
                            broker_tx.send(BrokerResponse::RequestInProgress).unwrap_or_else(|e| {
                                tracing::error!("Failed to send broker response: {:?}", e);
                            });

                            let broker_to_topic_creator_tx_clone = broker_to_topic_creator_tx.clone();

                            // Spawn a new task to handle topic creation.
                            tokio::spawn(async move {
                                create_topic::create_topic(
                                    topic_to_create,
                                    self_address,
                                    self_data_dir_path,
                                    cluster_info,
                                    broker_to_topic_creator_tx_clone,
                                    cancellation_token,
                                ).await;
                            });
                        }
                        CommandToBroker::CreatePartitionWriter { topic_name, partition_number, role, broker_tx } => {
                            tracing::info!("Received create partition writer command from peer for topic: {}, partition: {}, role: {:?}", topic_name, partition_number, role);
                            let self_address = self.address.clone();
                            let self_data_dir_path = self.data_dir_path.clone();
                            let cancellation_token = cancellation_token.clone();
                            let topic_name_clone = topic_name.clone();
                            let role_clone = role.clone();
                            let join_handle = tokio::spawn(async move {
                                    create_topic::create_partition(
                                        &topic_name_clone,
                                        partition_number,
                                        &self_address,
                                        role_clone,
                                        &self_data_dir_path,
                                        cancellation_token,
                                    ).await
                                }
                            );
                            match join_handle.await {
                                Ok(Some(partition_writer_tx)) => {
                                    self.local_partition_writers.insert(topic_name, partition_writer_tx);
                                    broker_tx.send(BrokerResponse::PartitionWriterCreated).unwrap_or_else(|e| {
                                        tracing::error!("Failed to send broker response: {:?}", e);
                                    });
                                }
                                Ok(None) => {
                                    broker_tx.send(BrokerResponse::BrokerError {
                                        message: format!("Failed to create partition for topic: {} partition: {}, role: {}",
                                        topic_name, partition_number, role) }).unwrap_or_else(|e| {
                                        tracing::error!("Failed to send broker response: {:?}", e);
                                    });
                                }
                                Err(e) => {
                                    tracing::error!("Failed to create partition writer for topic: {}, partition: {}, role: {:?} due to: {:?}", topic_name, partition_number, role, e);
                                    broker_tx.send(BrokerResponse::BrokerError { message:
                                        format!("Failed to create partition writer for topic: {} partition: {}, role: {:?} due to: {:?}", topic_name, partition_number, role, e) }
                                    ).unwrap_or_else(|e| {
                                        tracing::error!("Failed to send broker response: {:?}", e);
                                    });
                                }
                            }
                        }

                        CommandToBroker::GetPartitionReader { topic_name, broker_tx } =>
                            self.handle_get_partition_reader_command(topic_name, broker_tx,  cancellation_token.clone()).await,

                        CommandToBroker::GetPartitionWriter { topic_name, broker_tx } =>
                            self.handle_get_partition_writer_request(&topic_name, broker_tx).await,

                        CommandToBroker::GetTopicMetadata { topics, broker_tx } =>
                            self.handle_get_topic_metadata_request(topics, broker_tx).await,

                        CommandToBroker::Heartbeat { sender_address, sender_status, broker_tx } =>
                            self.update_cluster_info(&sender_address, sender_status, broker_tx).await,
                    }
                },
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token cancelled. Broker shutting down : {}", self.address);
                    break;
                }
                _ = self.heartbeat_interval.tick() => {
                    let cluster_info = self.cluster_info.clone();
                    let self_address = self.address.clone();
                    tokio::spawn(async move {heartbeat::send_heartbeat(self_address, cluster_info).await;});
                }
            }
        }
        tracing::info!("Broker stopped: {}", self.address);
    }

    async fn handle_get_partition_writer_request(
        &self,
        topic_name: &str,
        tx: oneshot::Sender<BrokerResponse>,
    ) {
        tracing::info!(
            "Received request to GET partition writer for topic: {}",
            topic_name
        );
        if let Some(partition_manager) = self.local_partition_writers.get(topic_name) {
            tx.send(BrokerResponse::PartitionManagerFound {
                tx: partition_manager.clone(),
            })
            .unwrap_or_else(|e| {
                tracing::error!(
                    "Failed to send response for GetPartitionWriter command: {:?}",
                    e
                );
            });
        } else {
            let response = BrokerResponse::BrokerError {
                message: format!("No partition writer found for topic: {}", topic_name),
            };
            tx.send(response).unwrap_or_else(|e| {
                tracing::error!(
                    "Failed to send response for GetPartitionWriter command: {:?}",
                    e
                );
            });
        }
    }

    async fn handle_get_topic_metadata_request(
        &self,
        topics: Vec<String>,
        broker_tx: oneshot::Sender<BrokerResponse>,
    ) {
        tracing::info!(
            "Received request for topic metadata for topics: {:?}",
            topics
        );
        let topic_metadata = self
            .topic_metadata
            .iter()
            .filter(|(name, _)| topics.contains(name))
            .map(|(name, metadata)| (name.clone(), metadata.clone()))
            .collect::<HashMap<String, TopicMetadata>>();

        if topic_metadata.is_empty() {
            tracing::warn!(
                "metadata not found for 1 or more requested topics: {:?}",
                topics
            );
            broker_tx
                .send(BrokerResponse::BrokerError {
                    message: "No metadata found for requested topics".to_string(),
                })
                .unwrap_or_else(|e| {
                    tracing::error!(
                        "Failed to send response for GetTopicMetadata command: {:?}",
                        e
                    );
                });
        } else {
            broker_tx
                .send(BrokerResponse::TopicMetadata { topic_metadata })
                .unwrap_or_else(|e| {
                    tracing::error!(
                        "Failed to send response for GetTopicMetadata command: {:?}",
                        e
                    );
                });
        }
    }

    async fn handle_get_partition_reader_command(
        &mut self,
        topic_name: String,
        broker_tx: oneshot::Sender<BrokerResponse>,
        cancellation_token: CancellationToken,
    ) {
        let partition_reader_name = format!("{}-reader", topic_name);
        let response: BrokerResponse = if let Some(partition_reader_tx) =
            self.local_partition_readers.get(&partition_reader_name)
        {
            tracing::debug!("Found partition reader for topic: {}", topic_name);
            BrokerResponse::PartitionManagerFound {
                tx: partition_reader_tx.clone(),
            }
        } else {
            tracing::warn!(
                "No partition reader found for topic: {}. Creating new.",
                topic_name
            );
            let mut partition_reader =
                PartitionReader::new(topic_name.clone(), 0, self.data_dir_path.clone(), 100);
            let (partition_reader_tx, partition_reader_rx) =
                mpsc::channel::<PartitionCommand>(MPSC_MAX_Q_SIZE);

            let partition_cancellation_token = cancellation_token.child_token();
            tokio::spawn(async move {
                partition_reader
                    .start(partition_reader_rx, partition_cancellation_token)
                    .await;
            });
            let partition_number = 0; // Assuming partition 0 for simplicity
            let partition_reader_name = format!("{}-{}-reader", topic_name, partition_number);
            self.local_partition_readers
                .insert(partition_reader_name, partition_reader_tx.clone());
            BrokerResponse::PartitionManagerFound {
                tx: partition_reader_tx,
            }
        };
        broker_tx.send(response).unwrap_or_else(|e| {
            tracing::error!(
                "Failed to send response for GetPartitionReader command: {:?}",
                e
            );
        });
    }

    async fn update_cluster_info(
        &mut self,
        peer_address: &str,
        peer_status: BrokerInfo,
        broker_tx: oneshot::Sender<BrokerResponse>,
    ) {
        tracing::info!("heartbeat received: {:?}", peer_status);
        self.cluster_info
            .brokers
            .insert(peer_address.to_string(), peer_status.clone());
        let _ = broker_tx.send(BrokerResponse::HeartbeatReceived);
        tracing::debug!("Current cluster info: {:?}", self.cluster_info);
    }
}
