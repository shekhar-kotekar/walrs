use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    models::{BrokerCommand, BrokerResponse, PartitionCommand},
    partition::{PartitionReader, PartitionWriter},
};

const MAX_MESSAGE_BATCH_SIZE: u8 = 3;

#[derive(Debug)]
pub struct Broker {
    address: String,
    partition_managers: HashMap<String, mpsc::Sender<PartitionCommand>>,
    topics_in_cluster: Vec<String>,
    cancellation_token: CancellationToken,
    base_path_for_data: String,
}

impl Broker {
    pub fn new(address: String, cancellation_token: CancellationToken, base_path_for_data: String) -> Self {
        std::fs::create_dir_all(&base_path_for_data)
            .expect(format!("Failed to create base directory: {}", base_path_for_data).as_str());
        Broker {
            address,
            partition_managers: HashMap::new(),
            topics_in_cluster: Vec::new(),
            cancellation_token,
            base_path_for_data,
        }
    }

    pub async fn start(&mut self, mut main_rx: mpsc::Receiver<BrokerCommand>) {
        tracing::info!("{} broker started...", self.address);
        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        BrokerCommand::CreateNewTopic { topic_name, num_partitions, retention_period_hours, broker_tx } => {
                            let response = self.handle_create_topic_command(topic_name, num_partitions, retention_period_hours).await;
                            let _ = broker_tx.send(response);
                        }
                        BrokerCommand::GetPartitionWriter { topic_name, broker_tx } => {
                            let response = self.handle_get_partition_manager_command(topic_name).await;
                            let _ = broker_tx.send(response);
                        }
                        BrokerCommand::GetPartitionReader { topic_name, broker_tx } => {
                            let response = self.handle_get_partition_reader_command(topic_name).await;
                            let _ = broker_tx.send(response);
                        }
                    }

                }
                _ = self.cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token cancelled. {} broker shutting down...", self.address);
                    break;
                }
            }
        }
        tracing::info!("{} broker stopped.", self.address);
    }

    async fn handle_get_partition_reader_command(&mut self, topic_name: String) -> BrokerResponse {
        let partition_reader_name = format!("{}-reader", topic_name);
        if let Some(partition_reader) = self.partition_managers.get(&partition_reader_name) {
            tracing::info!("Found partition reader for topic: {}", topic_name);
            BrokerResponse::PartitionManagerFound {
                tx: partition_reader.clone(),
            }
        } else {
            tracing::warn!("No partition reader found for topic: {}. Creating new", topic_name);
            let mut partition_reader = PartitionReader::new(
                topic_name.clone(),
                0,
                self.base_path_for_data.clone(),
                MAX_MESSAGE_BATCH_SIZE,
            );
            let (partition_reader_tx, partition_reader_rx) = mpsc::channel::<PartitionCommand>(100);

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

    async fn handle_get_partition_manager_command(&mut self, topic_name: String) -> BrokerResponse {
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

    async fn handle_create_topic_command(
        &mut self,
        topic_name: String,
        _num_partitions: u8,
        _retention_period_hours: u16,
    ) -> BrokerResponse {
        if self.topics_in_cluster.contains(&topic_name) && self.partition_managers.contains_key(&topic_name) {
            tracing::info!("Topic already exists: {}", topic_name);
            return BrokerResponse::TopicAlreadyExists;
        } else {
            tracing::info!("Creating a partition writer for new topic: {}", topic_name);
            let (partition_writer_tx, partition_writer_rx) = mpsc::channel::<PartitionCommand>(100);

            let partition_cancellation_token = self.cancellation_token.child_token();
            let topic_name_clone = topic_name.clone();
            let partition_number = 0;
            let partition_data_path = format!("{}/{}/{}", self.base_path_for_data, topic_name_clone, partition_number);
            tokio::spawn(async move {
                let mut partition_writer =
                    PartitionWriter::new(topic_name_clone, partition_number, partition_data_path);
                partition_writer
                    .start(partition_writer_rx, partition_cancellation_token)
                    .await;
            });
            self.topics_in_cluster.push(topic_name.clone());
            let partition_writer_name = format!("{}-writer", topic_name);
            self.partition_managers
                .insert(partition_writer_name, partition_writer_tx);
            tracing::info!("Partition writer created for topic {}", topic_name);
            tracing::info!(
                "Total number of registered partition writers in this broker are: {}",
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
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::models::BrokerCommand;

    use super::Broker;

    #[tokio::test]
    async fn test_broker_should_start() {
        let cancellation_token = CancellationToken::new();
        let mut broker = Broker::new(
            "local:1234".to_string(),
            cancellation_token.clone(),
            "/tmp/walrs/test/".to_string(),
        );
        let (_, main_rx) = mpsc::channel::<BrokerCommand>(2);

        tokio::spawn(async move { broker.start(main_rx).await });

        cancellation_token.cancel();
    }
}
