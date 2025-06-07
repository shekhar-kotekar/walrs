use std::collections::HashMap;

use common::models::TopicMetadata;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::{create_topic, heartbeat},
    models::{BrokerInfo, BrokerResponse, ClusterInfo, CommandToBroker, PartitionCommand},
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
        loop {
            tokio::select! {
                Some(command) = command_rx.recv() => {
                    match command {
                        CommandToBroker::CreateNewTopic { topic, broker_tx } => {
                            let self_address = self.address.clone();
                            let self_data_dir_path = self.data_dir_path.clone();
                            let cancellation_token = cancellation_token.clone();
                            let cluster_info = self.cluster_info.clone();
                            let topic_to_create = topic.clone();
                            let (oneshot_tx, oneshot_rx) = oneshot::channel::<Option<(mpsc::Sender<PartitionCommand>, TopicMetadata)>>();
                            tracing::debug!("Received create topic command from peer: {}, topic: {}. Creating separate task for this.",
                                self_address, topic.name);

                            tokio::spawn(async move {
                                    tracing::debug!("from within task: creating topic: {}", topic_to_create.name);
                                    let result = create_topic::create_topic(
                                        topic_to_create,
                                        self_address,
                                        self_data_dir_path,
                                        cluster_info,
                                        cancellation_token,
                                    ).await;
                                    let _= oneshot_tx.send(result);
                            });
                            match oneshot_rx.await {
                                Ok(result) => {
                                let (partition_zero_writer, topic_metadata) = result.unwrap();
                                self.local_partition_writers.insert(topic.name.clone(), partition_zero_writer);
                                let broker_response = BrokerResponse::TopicCreated {
                                    topic_metadata: topic_metadata.clone(),
                                };
                                self.topic_metadata.insert(topic.name.clone(), topic_metadata);
                                broker_tx.send(broker_response).unwrap_or_else(|e| {
                                    tracing::error!("Failed to send broker response: {:?}", e);
                                });
                                },
                                Err(e) => {
                                    tracing::error!("Failed to create topic: {:?} due to: {:?}", topic, e);
                                    broker_tx.send(BrokerResponse::BrokerError {
                                        message: format!("Failed to create topic: {} due to: {}", topic.name, e)
                                    }).unwrap_or_else(|e| {
                                        tracing::error!("Failed to send broker response: {:?}", e);
                                    });
                                    continue;
                                }
                            };

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
