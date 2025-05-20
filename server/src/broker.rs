use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    models::{BrokerCommand, BrokerResponse, PartitionCommand},
    partition::Partition,
};

#[derive(Debug)]
pub struct Broker {
    address: String,
    partition_managers: HashMap<String, mpsc::Sender<PartitionCommand>>,
    cancellation_token: CancellationToken,
}

impl Broker {
    pub fn new(address: String, cancellation_token: CancellationToken) -> Self {
        Broker {
            address,
            partition_managers: HashMap::new(),
            cancellation_token,
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
                        BrokerCommand::GetPartitionManager { topic_name, broker_tx } => {
                            let response = self.handle_get_partition_manager_command(topic_name).await;
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

    async fn handle_get_partition_manager_command(&mut self, topic_name: String) -> BrokerResponse {
        if let Some(partition_manager) = self.partition_managers.get(&topic_name) {
            tracing::info!("Found partition manager for topic: {}", topic_name);
            BrokerResponse::PartitionManagerFound {
                tx: partition_manager.clone(),
            }
        } else {
            tracing::info!("No partition manager found for topic: {}", topic_name);
            BrokerResponse::PartitionNotFound
        }
    }

    async fn handle_create_topic_command(
        &mut self,
        topic_name: String,
        _num_partitions: u8,
        _retention_period_hours: u16,
    ) -> BrokerResponse {
        if self.partition_managers.contains_key(&topic_name) {
            tracing::info!("Topic already exists: {}", topic_name);
            return BrokerResponse::TopicAlreadyExists;
        } else {
            tracing::info!("Creating a partition manager for new topic: {}", topic_name);
            let (partition_man_tx, partition_man_rx) = mpsc::channel::<PartitionCommand>(100);

            let partition_cancellation_token = self.cancellation_token.child_token();
            let topic_name_clone = topic_name.clone();
            tokio::spawn(async move {
                let mut partition_manager = Partition::new(topic_name_clone);
                partition_manager
                    .start(partition_man_rx, partition_cancellation_token)
                    .await;
            });

            self.partition_managers.insert(topic_name.clone(), partition_man_tx);
            tracing::info!("Partition manager created for topic {}", topic_name);
            tracing::info!(
                "Total number of registered partition managers: {}",
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
        let mut broker = Broker::new("local:1234".to_string(), cancellation_token.clone());
        let (_, main_rx) = mpsc::channel::<BrokerCommand>(2);

        tokio::spawn(async move { broker.start(main_rx).await });

        cancellation_token.cancel();
    }
}
