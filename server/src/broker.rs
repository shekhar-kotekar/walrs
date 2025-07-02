use std::{collections::HashMap, time::Duration};

use commons::models::{AckLevel, BrokerInfo, PartitionInfo, PartitionRole, Topic, TopicStatus};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    handlers::heartbeat,
    models::{ClusterInfo, PartitionCommand},
    partition_managers::{
        partition_reader::PartitionReader,
        partition_writer::{self, PartitionWriter},
    },
    topic_creation::{self, TopicManagerResponse},
    MPSC_CHANNEL_SIZE,
};

const DEFAULT_HEARTBEAT_INTERVAL_SECOND: u16 = 30;

pub struct Broker {
    address: String,
    data_dir_path: String,
    heartbeat_interval: tokio::time::Interval,

    //key: topic name, value: partition writer
    local_partition_writers: HashMap<String, mpsc::Sender<PartitionCommand>>,

    //key: topic name, value: partition reader
    local_partition_readers: HashMap<String, mpsc::Sender<PartitionCommand>>,

    cluster_info: ClusterInfo,
}

impl Broker {
    pub fn new(
        data_dir_path: String,
        heartbeat_interval: Option<u16>,
        cluster_info: Option<ClusterInfo>,
    ) -> Self {
        assert_ne!(data_dir_path, "", "Data directory path cannot be empty");
        let heartbeat_interval_seconds =
            heartbeat_interval.unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_SECOND);
        let heartbeat_interval =
            tokio::time::interval(Duration::from_secs(heartbeat_interval_seconds as u64));
        std::fs::create_dir_all(&data_dir_path).unwrap_or_else(|e| {
            panic!(
                "Failed to create base directory: {} because: {}",
                &data_dir_path, e
            );
        });
        let cluster_info = cluster_info.unwrap_or_else(ClusterInfo::new);
        Self {
            address: cluster_info.self_info.address.clone(),
            data_dir_path,
            heartbeat_interval,
            local_partition_writers: HashMap::new(),
            local_partition_readers: HashMap::new(),
            cluster_info,
        }
    }

    pub async fn start(
        &mut self,
        mut broker_tx: mpsc::Receiver<BrokerCommand>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!("Starting broker. data directory: {}", self.data_dir_path);
        let (broker_to_topic_creator_tx, mut broker_to_topic_creator_rx) =
            mpsc::channel::<TopicManagerResponse>(MPSC_CHANNEL_SIZE);
        loop {
            tokio::select! {
                Some(response) = broker_to_topic_creator_rx.recv() => {
                    match response {
                        TopicManagerResponse::TopicCreated { topic, partition_zero_tx } => {
                            let key_name = format!("{}-0", topic.name);
                            self.cluster_info.self_info.registered_topics.insert(
                                topic.name.clone(),
                                (topic.clone(), TopicStatus::ReadyToServe),
                            );
                            // self.registered_topics.insert(key_name.clone(), (topic.clone(), TopicStatus::ReadyToServe));
                            self.local_partition_writers.insert(
                                key_name,
                                partition_zero_tx,
                            );
                            tracing::info!("Topic is ready to serve: {}", topic.name);
                        }
                        TopicManagerResponse::TopicCreationFailed { topic_name, error } => {
                            tracing::error!("Failed to create topic: {}. Error: {}", topic_name, error);
                            self.cluster_info.self_info.registered_topics.remove(&topic_name);
                        }
                    }
                }
                Some(command) = broker_tx.recv() => {
                    match command {
                        BrokerCommand::CreateTopic {
                            name,
                            num_partitions,
                            replication_factor,
                            retention_period_minutes,
                            ack_level,
                            broker_response_tx,
                        } => {
                            let topic_to_create: Topic = Topic::new(
                                name,
                                num_partitions,
                                replication_factor,
                                retention_period_minutes,
                                ack_level,
                            )
                            .unwrap();
                            self.handle_create_topic(
                                topic_to_create,
                                broker_response_tx,
                                broker_to_topic_creator_tx.clone(),
                                cancellation_token.clone(),
                            ).await;
                        }
                        BrokerCommand::GetTopicInfo {
                            topic_names,
                            broker_response_tx,
                        } => {
                            tracing::info!("Getting info for topics: {:?}", topic_names);
                            let topic_info: Vec<Topic> = self.cluster_info.self_info.registered_topics
                                .iter()
                                .filter_map(|(name, (topic, _))| {
                                    tracing::debug!("Checking topic: {}", name);
                                    if topic_names.is_empty() || topic_names.contains(name) {
                                        Some(topic.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            let response = BrokerResponse::TopicInfo { topics: topic_info };
                            broker_response_tx.send(response).unwrap_or_else(|err| {
                                tracing::error!(
                                    "Failed to send BrokerResponse for topic info: {:?}",
                                    err
                                );
                            });
                        }
                        BrokerCommand::CreatePartition {
                            topic_name,
                            partition_number,
                            role,
                            broker_response_tx,
                        } => {
                            self.create_partition(
                                topic_name,
                                partition_number,
                                role,
                                broker_response_tx,
                                cancellation_token.clone(),
                            ).await;
                        }
                        BrokerCommand::GetPartitionWriter {topic_name, partition_number, tx} => {
                            self.handle_get_partition_writer(topic_name, partition_number, tx).await;
                        }
                        BrokerCommand::GetPartitionReader {topic_name, partition_number, tx} => {
                            self.handle_get_partition_reader(topic_name, partition_number, tx, cancellation_token.clone()).await;
                        }
                        BrokerCommand::Heartbeat { broker_info } => {
                            tracing::info!("Received heartbeat from: {}", broker_info.address);
                            self.cluster_info.peers.insert(
                                broker_info.address.clone(),
                                broker_info,
                            );
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token cancelled. Broker is shutting down : {}", self.address);
                    break;
                }
                _ = self.heartbeat_interval.tick() => {
                    heartbeat::send_heartbeat(self.cluster_info.clone())
                        .await
                        .unwrap_or_else(|err| {
                            tracing::error!("Failed to send heartbeat: {}", err);
                        });
                }
            }
        }
        tracing::info!("Broker stopped : {}", self.address);
    }

    async fn handle_get_partition_reader(
        &mut self,
        topic_name: String,
        partition_number: u8,
        tx: oneshot::Sender<BrokerResponse>,
        cancellation_token: CancellationToken,
    ) {
        let key = format!("{}-{}", topic_name, partition_number);
        let response = if let Some(partition_tx) = self.local_partition_readers.get(&key) {
            tracing::debug!(
                "partition reader for topic: {}, partition: {}",
                topic_name,
                partition_number
            );
            BrokerResponse::PartitionReader {
                reader: partition_tx.clone(),
            }
        } else {
            tracing::info!(
                "Partition reader not found for topic: {}, partition: {}. Will create a new one.",
                topic_name,
                partition_number
            );
            let message_batch_size = 10; // Default batch size, can be adjusted as needed
            let mut partition_reader: PartitionReader = PartitionReader::new(
                topic_name.clone(),
                partition_number,
                self.data_dir_path.clone(),
                message_batch_size,
            );
            let (partition_tx, partition_rx) = mpsc::channel::<PartitionCommand>(MPSC_CHANNEL_SIZE);
            tokio::spawn(async move {
                partition_reader
                    .start(partition_rx, cancellation_token)
                    .await;
            });
            self.local_partition_readers
                .insert(key.clone(), partition_tx.clone());
            BrokerResponse::PartitionReader {
                reader: partition_tx,
            }
        };
        tx.send(response).unwrap_or_else(|err| {
            tracing::error!(
                "Failed to send BrokerResponse for partition reader: {:?}",
                err
            );
        });
    }

    async fn handle_get_partition_writer(
        &mut self,
        topic_name: String,
        partition_number: u8,
        tx: oneshot::Sender<BrokerResponse>,
    ) {
        let key = format!("{}-{}", topic_name, partition_number);
        let response = if let Some(partition_tx) = self.local_partition_writers.get(&key) {
            BrokerResponse::PartitionWriter {
                writer: partition_tx.clone(),
            }
        } else {
            BrokerResponse::Error(format!(
                "Partition writer not found for topic: {}, partition: {}",
                topic_name, partition_number
            ))
        };
        tx.send(response).unwrap_or_else(|err| {
            tracing::error!(
                "Failed to send BrokerResponse for partition writer: {:?}",
                err
            );
        });
    }

    async fn handle_create_topic(
        &mut self,
        topic: Topic,
        broker_response_tx: oneshot::Sender<BrokerResponse>,
        broker_to_topic_creator_tx: mpsc::Sender<TopicManagerResponse>,
        cancellation_token: CancellationToken,
    ) {
        if self
            .cluster_info
            .self_info
            .registered_topics
            .contains_key(&topic.name)
        {
            let response: BrokerResponse =
                BrokerResponse::Error(format!("Topic already exists: {}", topic.name));
            broker_response_tx.send(response).unwrap_or_else(|err| {
                tracing::error!(
                    "Failed to send BrokerResponse for topic creation: {:?}",
                    err
                );
            });
            return;
        }

        let data_dir_path = self.data_dir_path.clone();
        let topic_clone = topic.clone();
        let cluster_info = self.cluster_info.clone();
        tokio::spawn(async move {
            let mut topic_details = topic_clone.clone();
            tracing::debug!("New task spawned to create topic: {}", topic_details.name);
            let partition_info = topic_creation::create_partition_info(
                topic_details.partitions.len() as u8,
                &cluster_info,
            );
            if partition_info.is_empty() {
                broker_to_topic_creator_tx
                    .send(TopicManagerResponse::TopicCreationFailed {
                        topic_name: topic_details.name.clone(),
                        error: "No partitions created".to_string(),
                    })
                    .await
                    .unwrap_or_else(|err| {
                        tracing::error!("Failed to send TopicManagerResponse: {}", err);
                    });
                return;
            }
            topic_details.partitions = partition_info;
            match topic_creation::create_topic(
                topic_details.clone(),
                &data_dir_path,
                cancellation_token.clone(),
            )
            .await
            {
                Ok((topic_status, partition_zero_tx)) => match topic_status {
                    TopicStatus::ReadyToServe => {
                        let topic_manager_response = TopicManagerResponse::TopicCreated {
                            topic: topic_details.clone(),
                            partition_zero_tx,
                        };
                        broker_to_topic_creator_tx
                            .send(topic_manager_response)
                            .await
                            .unwrap_or_else(|err| {
                                tracing::error!("Failed to send TopicManagerResponse: {}", err);
                            });
                    }
                    other => tracing::error!("Topic creation status: {:?}", other),
                },
                Err(err) => tracing::error!("Failed to create topic: {}", err),
            }
        });
        let topic_name = topic.name.clone();
        self.cluster_info.self_info.registered_topics.insert(
            topic_name.clone(),
            (topic.clone(), TopicStatus::CreationInProgress),
        );
        let response = BrokerResponse::TopicCreationInProgress(topic);
        broker_response_tx.send(response).unwrap_or_else(|err| {
            tracing::error!(
                "Failed to send BrokerResponse for topic creation: {:?}",
                err
            );
        });
    }

    async fn create_partition(
        &mut self,
        topic_name: String,
        partition_number: u8,
        role: PartitionRole,
        broker_response_tx: oneshot::Sender<BrokerResponse>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!(
            "Creating partition {} for topic {}. Role: {:?}",
            partition_number,
            topic_name,
            role
        );
        let key: String = format!("{}-{}", topic_name, partition_number);
        let response: BrokerResponse = match role {
            PartitionRole::Follower { leader_address: _ } => {
                let mut local_partition: PartitionWriter =
                    PartitionWriter::new(&topic_name, partition_number, &self.data_dir_path);

                let (partition_tx, partition_rx) =
                    mpsc::channel::<PartitionCommand>(MPSC_CHANNEL_SIZE);
                tokio::spawn(async move {
                    local_partition
                        .start(partition_rx, cancellation_token)
                        .await;
                });
                self.local_partition_writers.insert(key, partition_tx);
                BrokerResponse::PartitionCreated {
                    topic_name,
                    partition_number,
                }
            }
            PartitionRole::Leader { followers } => {
                let partition_info: PartitionInfo = PartitionInfo {
                    number: partition_number,
                    leader_address: self.address.clone(),
                    followers: followers.clone(),
                };
                match partition_writer::create_local_leader_partition(
                    topic_name.clone(),
                    partition_info,
                    self.data_dir_path.clone(),
                    cancellation_token,
                )
                .await
                {
                    Ok(partition_tx) => {
                        self.local_partition_writers.insert(key, partition_tx);
                        BrokerResponse::PartitionCreated {
                            topic_name,
                            partition_number,
                        }
                    }
                    Err(e) => BrokerResponse::Error(format!(
                        "Failed to create local leader partition: {}",
                        e
                    )),
                }
            }
        };
        broker_response_tx.send(response).unwrap_or_else(|err| {
            tracing::error!(
                "Failed to send BrokerResponse for partition creation: {:?}",
                err
            );
        });
    }
}

pub enum BrokerCommand {
    CreateTopic {
        name: String,
        num_partitions: Option<u8>,
        replication_factor: Option<u8>,
        retention_period_minutes: Option<u16>,
        ack_level: Option<AckLevel>,
        broker_response_tx: oneshot::Sender<BrokerResponse>,
    },
    GetTopicInfo {
        topic_names: Vec<String>,
        broker_response_tx: oneshot::Sender<BrokerResponse>,
    },
    CreatePartition {
        topic_name: String,
        partition_number: u8,
        role: PartitionRole,
        broker_response_tx: oneshot::Sender<BrokerResponse>,
    },
    GetPartitionWriter {
        topic_name: String,
        partition_number: u8,
        tx: oneshot::Sender<BrokerResponse>,
    },
    GetPartitionReader {
        topic_name: String,
        partition_number: u8,
        tx: oneshot::Sender<BrokerResponse>,
    },
    Heartbeat {
        broker_info: BrokerInfo,
    },
}

#[derive(Clone, Debug)]
pub enum BrokerResponse {
    TopicCreationInProgress(Topic),
    Error(String),
    TopicInfo {
        topics: Vec<Topic>,
    },
    PartitionCreated {
        topic_name: String,
        partition_number: u8,
    },
    PartitionWriter {
        writer: mpsc::Sender<PartitionCommand>,
    },
    PartitionReader {
        reader: mpsc::Sender<PartitionCommand>,
    },
}
