use std::collections::HashMap;

use common::models::TopicMetadata;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::heartbeat,
    models::{BrokerInfo, BrokerResponse, ClusterInfo, CommandToBroker, PartitionCommand},
    topic_managers::create_topic::{self, TopicManagerResponse},
    MPSC_MAX_Q_SIZE,
};

pub struct Broker {
    cluster_info: ClusterInfo,
    data_dir_path: String,
    address: String,
    heartbeat_interval: tokio::time::Interval,
    local_partition_writers: HashMap<String, mpsc::Sender<PartitionCommand>>,
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
                            });
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
                                    broker_tx.send(BrokerResponse::BrokerError { message: format!("Failed to create partition writer for topic: {} partition: {}, role: {:?} due to: {:?}", topic_name, partition_number, role, e) }).unwrap_or_else(|e| {
                                        tracing::error!("Failed to send broker response: {:?}", e);
                                    });
                                }
                            }
                        }
                        CommandToBroker::Heartbeat { sender_address, sender_status, broker_tx } => self.update_cluster_info(&sender_address, sender_status, broker_tx).await,
                        _ => {
                            tracing::error!("Command not implemented (yet): {:?}", command);
                        }
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
