use std::collections::HashMap;

use common::models::Topic;
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{BrokerCommand, BrokerConfig, BrokerResponse, Heartbeat, PartitionCommand},
    partition::{PartitionReader, PartitionWriter},
};

const MAX_MESSAGE_BATCH_SIZE: u8 = 3;

#[derive(Debug, Clone)]
pub struct BrokerInfo {
    address: String,
    partition_leaders: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub brokers: HashMap<String, BrokerInfo>,
    pub topics_in_cluster: Vec<String>,
}

#[derive(Debug)]
pub struct Broker {
    address: String,
    broker_config: BrokerConfig,
    partition_managers: HashMap<String, mpsc::Sender<PartitionCommand>>,
    cancellation_token: CancellationToken,
    heartbeat_interval: tokio::time::Interval,
    cluster_info: ClusterInfo,
    broker_data_dir: String,
}

impl Broker {
    pub fn new(broker_config: BrokerConfig, cancellation_token: CancellationToken, cluster_info: ClusterInfo) -> Self {
        let broker_heartbeat_interval =
            tokio::time::interval(std::time::Duration::from_secs(broker_config.heartbeat_interval as u64));

        let broker_address = format!("{}:{}", broker_config.ip, broker_config.port);
        let data_dir_path = format!(
            "{}/{}",
            broker_config.base_path_for_data.trim(),
            broker_address.replace(":", "_").replace(".", "_").trim()
        );
        std::fs::create_dir_all(&data_dir_path)
            .expect(format!("Failed to create base directory: {}", &data_dir_path).as_str());

        Broker {
            address: broker_address,
            broker_config,
            cancellation_token,
            partition_managers: HashMap::new(),
            heartbeat_interval: broker_heartbeat_interval,
            cluster_info,
            broker_data_dir: data_dir_path,
        }
    }

    pub async fn start(&mut self, mut main_rx: mpsc::Receiver<BrokerCommand>) {
        tracing::info!("{} broker started...", self.address);

        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        BrokerCommand::CreateNewTopic { topic, broker_tx } => {
                            let response = self.handle_create_topic_command(topic).await;
                            let _ = broker_tx.send(response);
                        }
                        BrokerCommand::GetPartitionWriter { topic_name, broker_tx } => {
                            let response = self.handle_get_partition_writer_command(topic_name).await;
                            let _ = broker_tx.send(response);
                        }
                        BrokerCommand::GetPartitionReader { topic_name, broker_tx } => {
                            let response = self.handle_get_partition_reader_command(topic_name).await;
                            let _ = broker_tx.send(response);
                        }
                        BrokerCommand::Heartbeat {sender_address, message, broker_tx } => {
                            tracing::info!("{} received heartbeat from peer: {} :: {:?}", self.address, sender_address, message);
                            let _ = broker_tx.send(BrokerResponse::HeartbeatReceived);
                        }
                    }
                }
                _ = self.heartbeat_interval.tick() => {
                    for key in self.cluster_info.brokers.keys() {
                        self.send_heartbeat(key).await;
                    }
                    tracing::info!("{} broker: heartbeat sent to {} peers.", self.address, self.cluster_info.brokers.len());
                }
                _ = self.cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token cancelled. {} broker shutting down...", self.address);
                    break;
                }
            }
        }
        tracing::info!("{} broker stopped.", self.address);
    }

    async fn send_heartbeat(&self, peer_address: &str) {
        tracing::info!("{} sending heartbeat to {}", self.address, peer_address);
        let serialized_heartbeat = Heartbeat::new(self.partition_managers.len() as u8).to_bytes();

        match tokio::net::TcpStream::connect(peer_address).await {
            Ok(mut stream) => {
                stream
                    .write_all(&serialized_heartbeat)
                    .await
                    .expect("Failed to send heartbeat");
                stream.flush().await.expect("Failed to flush stream");
                stream.shutdown().await.expect("Failed to shutdown stream");
            }
            Err(e) => {
                tracing::error!("Failed to send heartbeat to {}: {}", peer_address, e);
            }
        }
    }

    async fn handle_get_partition_reader_command(&mut self, topic_name: String) -> BrokerResponse {
        let partition_reader_name = format!("{}-reader", topic_name);
        if let Some(partition_reader_tx) = self.partition_managers.get(&partition_reader_name) {
            tracing::info!("Found partition reader for topic: {}", topic_name);
            BrokerResponse::PartitionManagerFound {
                tx: partition_reader_tx.clone(),
            }
        } else {
            tracing::warn!("No partition reader found for topic: {}. Creating new", topic_name);
            let mut partition_reader = PartitionReader::new(
                topic_name.clone(),
                0,
                self.broker_data_dir.clone(),
                MAX_MESSAGE_BATCH_SIZE,
            );
            let (partition_reader_tx, partition_reader_rx) =
                mpsc::channel::<PartitionCommand>(self.broker_config.mpsc_max_queue_size);

            let partition_cancellation_token = self.cancellation_token.child_token();
            tokio::spawn(async move {
                partition_reader
                    .start(partition_reader_rx, partition_cancellation_token)
                    .await;
            });
            let partition_reader_name = format!("{}-reader", topic_name);
            self.partition_managers
                .insert(partition_reader_name, partition_reader_tx.clone());
            BrokerResponse::PartitionManagerFound {
                tx: partition_reader_tx,
            }
        }
    }

    async fn handle_get_partition_writer_command(&mut self, topic_name: String) -> BrokerResponse {
        let partition_writer_name = format!("{}-writer", topic_name);
        if let Some(partition_writer) = self.partition_managers.get(&partition_writer_name) {
            tracing::info!("Found partition writer for topic: {}", topic_name);
            BrokerResponse::PartitionManagerFound {
                tx: partition_writer.clone(),
            }
        } else {
            tracing::info!("No partition writer found for topic: {}", topic_name);
            BrokerResponse::PartitionNotFound
        }
    }

    fn search_topic_in_cluster(&self, topic_name: &String) -> Option<&BrokerInfo> {
        self.cluster_info
            .brokers
            .values()
            .find(|broker| broker.partition_leaders.contains(topic_name))
    }

    async fn can_create_new_topic(&self, topic_name: &String) -> BrokerResponse {
        let partition_writer_name = format!("{}-writer", topic_name);
        match self.partition_managers.get(&partition_writer_name) {
            Some(_) => {
                return BrokerResponse::TopicAlreadyExists {
                    leader_address: self.address.clone(),
                };
            }
            None => {
                tracing::info!("{} topic not found on this node. Checking in the cluster", topic_name);
                match self.search_topic_in_cluster(topic_name) {
                    Some(broker) => BrokerResponse::TopicAlreadyExists {
                        leader_address: broker.address.clone(),
                    },
                    None => BrokerResponse::TopicNotFound,
                }
            }
        }
    }

    fn find_potential_followers(&self) -> Vec<String> {
        // Find all brokers in the cluster except this one AND the one which has less number of partition leaders than the current one
        let self_partition_leaders = self
            .partition_managers
            .iter()
            .filter(|(topic_name, _)| topic_name.ends_with("-writer"))
            .count();

        self.cluster_info
            .brokers
            .values()
            .filter(|broker| broker.address != self.address && broker.partition_leaders.len() > self_partition_leaders)
            .map(|broker| broker.address.clone())
            .collect()
    }

    async fn handle_create_topic_command(&mut self, topic: Topic) -> BrokerResponse {
        let topic_exists = self.can_create_new_topic(&topic.name).await;
        if let BrokerResponse::TopicAlreadyExists { .. } = topic_exists {
            return topic_exists;
        } else {
            tracing::info!("Creating a partition writer for new topic: {}", topic.name);
            let (partition_writer_tx, partition_writer_rx) =
                mpsc::channel::<PartitionCommand>(self.broker_config.mpsc_max_queue_size);

            let partition_cancellation_token = self.cancellation_token.child_token();
            let partition_number = 0;
            let mut partition_writer =
                PartitionWriter::new(topic.name.clone(), partition_number, self.broker_data_dir.clone());
            tokio::spawn(async move {
                partition_writer
                    .start(partition_writer_rx, partition_cancellation_token)
                    .await;
            });
            self.cluster_info.topics_in_cluster.push(topic.name.clone());
            let partition_writer_name = format!("{}-writer", topic.name);
            self.partition_managers
                .insert(partition_writer_name, partition_writer_tx);
            tracing::info!("Partition writer created for topic {}", &topic.name);
            tracing::info!(
                "Total number of registered partition managers in this broker: {}",
                self.partition_managers.len()
            );
            BrokerResponse::TopicCreated {
                leader_address: "127.0.0.1:5056".into(),
            }
        }
    }
}

#[cfg(test)]
mod should {
    use std::collections::HashMap;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::models::BrokerCommand;

    use super::Broker;

    #[tokio::test]
    async fn test_broker_should_start() {
        let cancellation_token = CancellationToken::new();
        let cluster_info = super::ClusterInfo {
            brokers: HashMap::new(),
            topics_in_cluster: vec![],
        };
        let broker_config = super::BrokerConfig {
            ip: "127.0.0.1".to_string(),
            port: 5056,
            heartbeat_interval: 5,
            mpsc_max_queue_size: 100,
            base_path_for_data: "/tmp/walrs/data".to_string(),
        };
        let mut broker = Broker::new(broker_config, cancellation_token.clone(), cluster_info);
        let (_, main_rx) = mpsc::channel::<BrokerCommand>(2);

        tokio::spawn(async move { broker.start(main_rx).await });

        cancellation_token.cancel();
    }
}
