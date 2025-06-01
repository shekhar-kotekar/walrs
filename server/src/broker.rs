use std::collections::HashMap;

use common::models::Topic;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{
        BrokerInfo, BrokerResponse, ClusterInfo, CommandToBroker, CommandToPeer, PartitionCommand, PartitionWriterRole,
        PeerResponse,
    },
    partition::{PartitionReader, PartitionWriter},
};

const PARTITION_WRITER_MAX_QUEUE_SIZE: usize = 1000;

#[derive(Debug)]
pub struct Broker {
    partition_managers: HashMap<String, mpsc::Sender<PartitionCommand>>,
    cancellation_token: CancellationToken,
    heartbeat_interval: tokio::time::Interval,
    cluster_info: ClusterInfo,
    self_status: BrokerInfo,
    data_dir_path: String,
    peer_listener_address: String,
}

impl Broker {
    pub fn new(
        ip_address: &str,
        broker_port: u16,
        peer_listener_port: u16,
        data_dir_path: &str,
        heartbeat_interval_ms: u16,
        cancellation_token: CancellationToken,
        cluster_info: ClusterInfo,
    ) -> Self {
        let heartbeat_interval = tokio::time::interval(std::time::Duration::from_millis(heartbeat_interval_ms as u64));

        let data_dir_path = format!(
            "{}/{}-{}",
            data_dir_path,
            ip_address.replace(':', "-").replace('.', "-"),
            broker_port
        );
        std::fs::create_dir_all(&data_dir_path).unwrap_or_else(|_| {
            panic!("Failed to create base directory: {}", &data_dir_path);
        });

        Broker {
            cancellation_token,
            partition_managers: HashMap::new(),
            heartbeat_interval,
            cluster_info,
            self_status: BrokerInfo {
                address: ip_address.to_string(),
                partition_leaders: vec![],
            },
            data_dir_path,
            peer_listener_address: format!("{}:{}", ip_address, peer_listener_port),
        }
    }

    pub async fn start(&mut self, mut main_rx: mpsc::Receiver<CommandToBroker>) {
        tracing::info!("{} broker started...", self.self_status.address);
        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        CommandToBroker::RegisterPeer { peer_info, broker_tx } => {
                            let peer_to_register = peer_info.address.clone();
                            self.cluster_info.brokers.insert(peer_info.address.clone(), peer_info);
                            broker_tx.send(BrokerResponse::PeerRegistered).unwrap_or_else(|e| {
                                tracing::error!("Failed to send response for RegisterPeer command: {:?}", e);
                            });
                            tracing::info!("peer registered: {}", &peer_to_register);
                        }
                        CommandToBroker::CreateNewTopic { topic, broker_tx } => {
                            self.create_topic(topic, broker_tx).await;
                        }
                        CommandToBroker::CreatePartitionWriter { topic_name, partition_number, broker_tx, role } => {
                            let partition_writer_tx_sender = self.create_partition_writer(&topic_name, partition_number, &role).await;
                            let partition_writer_name = format!("{}-{}-{:?}", topic_name, partition_number, role);
                            self.partition_managers.insert(partition_writer_name, partition_writer_tx_sender);
                            broker_tx.send(BrokerResponse::PartitionWriterCreated).unwrap_or_else(|e| {
                                tracing::error!("Failed to send response for CreatePartitionWriter command: {:?}", e);
                            });
                        }
                        CommandToBroker::GetPartitionWriter { topic_name, broker_tx } => {
                            // search in partition_managers for partition writer starting with given topic_name
                            let partition_0_writer_name = format!("{}-0-{:?}", topic_name, PartitionWriterRole::Leader);
                            if let Some(partition_manager) = self.partition_managers.get(&partition_0_writer_name) {
                                broker_tx.send(BrokerResponse::PartitionManagerFound { tx: partition_manager.clone() }).unwrap_or_else(|e| {
                                    tracing::error!("Failed to send response for GetPartitionWriter command: {:?}", e);
                                });
                            } else {
                                let response = BrokerResponse::BrokerError {
                                    message: format!("No partition writer found for topic: {}", topic_name),
                                };
                                broker_tx.send(response).unwrap_or_else(|e| {
                                    tracing::error!("Failed to send response for GetPartitionWriter command: {:?}", e);
                                });
                            }
                        }
                        CommandToBroker::GetPartitionReader { topic_name, broker_tx } => {
                            self.handle_get_partition_reader_command(topic_name, broker_tx).await;
                        }
                        CommandToBroker::Heartbeat { sender_address, sender_status, broker_tx } => {
                            self.update_cluster_info(&sender_address, sender_status, broker_tx).await;
                        }
                        CommandToBroker::GetStatus { broker_tx } => {
                            broker_tx.send(BrokerResponse::Status { info: self.self_status.clone() }).unwrap_or_else(|e| {
                                tracing::error!("Failed to send response for GetStatus command: {:?}", e);
                            });
                        }
                    }
                }
                _ = self.heartbeat_interval.tick() => {
                    let peers: Vec<String> = self.cluster_info.brokers.keys().cloned().collect();
                    let broker_current_status = self.self_status.clone();
                    tracing::info!("broker current status: {:?}", broker_current_status);

                    let peer_listener_address_clone = self.peer_listener_address.clone();
                    tokio::spawn(async move{
                        Self::send_heartbeat(broker_current_status, peer_listener_address_clone, peers).await;
                    });
                }
                _ = self.cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token cancelled. {} broker shutting down...", self.self_status.address);
                    break;
                }
            }
        }
        tracing::info!("{} broker stopped.", self.self_status.address);
    }

    async fn update_cluster_info(
        &mut self,
        peer_address: &str,
        peer_status: BrokerInfo,
        broker_tx: oneshot::Sender<BrokerResponse>,
    ) {
        tracing::info!("heartbeat received from:{} :: {:?}", peer_address, peer_status);
        self.cluster_info
            .brokers
            .insert(peer_address.to_string(), peer_status.clone());
        let _ = broker_tx.send(BrokerResponse::HeartbeatReceived);
    }

    async fn handle_get_partition_reader_command(
        &mut self,
        topic_name: String,
        broker_tx: oneshot::Sender<BrokerResponse>,
    ) {
        let partition_reader_name = format!("{}-reader", topic_name);
        let response: BrokerResponse =
            if let Some(partition_reader_tx) = self.partition_managers.get(&partition_reader_name) {
                tracing::info!("Found partition reader for topic: {}", topic_name);
                BrokerResponse::PartitionManagerFound {
                    tx: partition_reader_tx.clone(),
                }
            } else {
                tracing::warn!("No partition reader found for topic: {}. Creating new", topic_name);
                let mut partition_reader = PartitionReader::new(topic_name.clone(), 0, self.data_dir_path.clone(), 100);
                let (partition_reader_tx, partition_reader_rx) =
                    mpsc::channel::<PartitionCommand>(PARTITION_WRITER_MAX_QUEUE_SIZE);

                let partition_cancellation_token = self.cancellation_token.child_token();
                tokio::spawn(async move {
                    partition_reader
                        .start(partition_reader_rx, partition_cancellation_token)
                        .await;
                });
                let partition_number = 0; // Assuming partition 0 for simplicity
                let partition_reader_name = format!("{}-{}-reader", topic_name, partition_number);
                self.partition_managers
                    .insert(partition_reader_name, partition_reader_tx.clone());
                BrokerResponse::PartitionManagerFound {
                    tx: partition_reader_tx,
                }
            };
        broker_tx.send(response).unwrap_or_else(|e| {
            tracing::error!("Failed to send response for GetPartitionReader command: {:?}", e);
        });
    }

    fn topic_already_exists(&self, topic_name: &str) -> bool {
        self.cluster_info.topics_in_cluster.iter().any(|t| t == topic_name)
    }

    async fn create_partition_writer(
        &self,
        topic_name: &String,
        partition_number: u8,
        role: &PartitionWriterRole,
    ) -> mpsc::Sender<PartitionCommand> {
        let mut partition_writer = PartitionWriter::new(topic_name, partition_number, &self.data_dir_path);
        let (partition_command_tx, partition_command_rx) =
            mpsc::channel::<PartitionCommand>(PARTITION_WRITER_MAX_QUEUE_SIZE);

        let partition_writer_cancellation_token = self.cancellation_token.child_token();
        tokio::spawn(async move {
            partition_writer
                .start(partition_command_rx, partition_writer_cancellation_token)
                .await;
        });
        tracing::info!(
            "Partition writer created for topic: {} partition: {} role: {:?}",
            topic_name,
            partition_number,
            role
        );
        partition_command_tx
    }

    async fn create_topic(&mut self, topic: Topic, broker_tx: oneshot::Sender<BrokerResponse>) {
        tracing::info!("Creating topic: {:?}", topic);

        let response = if self.topic_already_exists(&topic.name) {
            BrokerResponse::TopicAlreadyExists
        } else if (self.cluster_info.brokers.len() + 1) < topic.replication_factor as usize {
            BrokerResponse::BrokerError {
                message: format!(
                    "Cluster has {} brokers to create topic: {}. Need minimum {} brokers",
                    self.cluster_info.brokers.len(),
                    topic.name,
                    topic.replication_factor
                ),
            }
        } else {
            // Create partition 0 leader here on this broker
            // Find follower broker for partition 0 as below:
            // from cluster_info find the brokers with least number of partition leaders.
            // sort brokers by number of partition leaders in ascending order

            let mut peers_partition_count: Vec<(u16, &String)> = self
                .cluster_info
                .brokers
                .iter()
                .map(|(peer_address, peer_info)| (peer_info.partition_leaders.len() as u16, peer_address))
                .collect::<Vec<(u16, &String)>>();

            peers_partition_count.sort();

            let partition_0_writer = self
                .create_partition_writer(&topic.name, 0, &PartitionWriterRole::Leader)
                .await;
            let partition_0_writer_name = format!("{}-0-{:?}", topic.name, PartitionWriterRole::Leader);
            self.partition_managers
                .insert(partition_0_writer_name, partition_0_writer);

            tracing::info!(
                "Selected partition 0 followers for topic {}: {:?}",
                topic.name,
                peers_partition_count
            );

            let follower_addresses: Vec<&String> = peers_partition_count[0..topic.replication_factor as usize - 1]
                .iter()
                .map(|(_, addr)| *addr)
                .collect();

            self.create_partition_followers(&topic.name, 0, follower_addresses)
                .await;

            BrokerResponse::TopicCreated {
                partition_leaders: HashMap::from([
                    (0, self.peer_listener_address.clone()),
                    (1, peers_partition_count[0].1.clone()),
                ]),
            }
        };

        broker_tx.send(response).unwrap_or_else(|e| {
            tracing::error!("Failed to send response for CreateNewTopic command: {:?}", e);
        });
    }

    async fn create_partition_followers(
        &self,
        topic_name: &String,
        partition_number: u8,
        follower_addresses: Vec<&String>,
    ) {
        tracing::info!(
            "Creating partition followers for topic: {}, partition: {}",
            topic_name,
            partition_number,
        );
        for follower_address in follower_addresses {
            let _ = self
                .send_create_partition_writer_request_to_peer(
                    topic_name,
                    partition_number,
                    follower_address,
                    PartitionWriterRole::Follower,
                )
                .await;
        }
    }

    async fn send_create_partition_writer_request_to_peer(
        &self,
        topic_name: &String,
        partition_number: u8,
        peer_address: &str,
        role: PartitionWriterRole,
    ) -> bool {
        // send request to peer to create partition writer using TCP connection
        match tokio::net::TcpStream::connect(peer_address).await {
            Ok(mut stream) => {
                tracing::info!(
                    "Connected to peer {} to create partition writer for topic: {}, partition: {}, role: {:?}",
                    peer_address,
                    topic_name,
                    partition_number,
                    role
                );
                let command_to_peer: CommandToPeer = CommandToPeer::CreatePartitionWriter {
                    topic_name: topic_name.clone(),
                    partition_number,
                    role,
                };
                let serialized_command = crate::models::to_bytes(&command_to_peer);
                if let Err(e) = stream.write_all(&serialized_command).await {
                    tracing::error!(
                        "Failed to send CreatePartitionWriter command to peer {}: {}",
                        peer_address,
                        e
                    );
                    return false;
                }
                tracing::info!(
                    "CreatePartitionWriter command sent to peer {} for topic: {} partition: {}",
                    peer_address,
                    topic_name,
                    partition_number
                );
                // wait for response from peer
                let bytes_to_receive = stream.read_u32().await;
                if let Err(e) = bytes_to_receive {
                    tracing::error!("Failed to read response size from peer {}: {}", peer_address, e);
                    return false;
                }
                let mut response_buffer = vec![0u8; bytes_to_receive.unwrap() as usize];
                if let Err(e) = stream.read_exact(&mut response_buffer).await {
                    tracing::error!("Failed to read response from peer {}: {}", peer_address, e);
                    return false;
                }
                let response_from_peer: PeerResponse = crate::models::from_bytes::<PeerResponse>(&response_buffer);
                match response_from_peer {
                    PeerResponse::PartitionWriterCreated => {
                        tracing::info!(
                            "Partition writer created on peer {} for topic: {}, partition: {}",
                            peer_address,
                            topic_name,
                            partition_number,
                        );
                        return true;
                    }
                    _ => {
                        tracing::error!(
                            "Error from peer {} while creating partition writer for topic: {}, partition: {}",
                            peer_address,
                            topic_name,
                            partition_number,
                        );
                        return false;
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to connect to peer {}: {}", peer_address, e);
                return false;
            }
        };
    }

    async fn send_heartbeat(broker_status: BrokerInfo, peer_listener_address: String, peers: Vec<String>) {
        let message_to_peer: CommandToPeer = CommandToPeer::Heartbeat {
            peer_listener_address,
            broker_status,
        };
        let serialized_heartbeat = crate::models::to_bytes(&message_to_peer);
        for peer in &peers {
            match tokio::net::TcpStream::connect(peer).await {
                Ok(mut stream) => {
                    if let Err(e) = stream.write_all(&serialized_heartbeat).await {
                        tracing::error!("Failed to send heartbeat to {}: {}", peer, e);
                    } else if let Err(e) = stream.flush().await {
                        tracing::error!("Failed to flush stream to {}: {}", peer, e);
                    } else if let Err(e) = stream.shutdown().await {
                        tracing::error!("Failed to shutdown stream to {}: {}", peer, e);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to connect to {}: {}", peer, e);
                }
            }
        }
        tracing::info!("Heartbeat sent to {} peers.", peers.len());
    }
}

#[cfg(test)]
mod should {
    use std::collections::HashMap;

    use common::models::{AckLevel, Topic};
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;

    use crate::models::{BrokerInfo, BrokerResponse, CommandToBroker};

    use super::Broker;

    #[tokio::test]
    // #[ignore]
    #[traced_test]
    async fn test_broker_should_be_able_to_return_handle_to_partition_writer() {
        let peer_listener_port = 5052;
        let broker_heartbeat_interval_ms = 50;
        let data_dir_path: &str = "/tmp/walrs/data";
        let cancellation_token = CancellationToken::new();
        let cluster_info = super::ClusterInfo {
            brokers: HashMap::new(),
            topics_in_cluster: vec![],
        };

        let broker_1_cancellation_token = cancellation_token.child_token();
        let mut broker_1 = Broker::new(
            "127.0.0.1:5051",
            5050,
            peer_listener_port,
            data_dir_path,
            broker_heartbeat_interval_ms,
            broker_1_cancellation_token,
            cluster_info.clone(),
        );
        let (main_to_broker_1_tx, main_to_broker_1_rx) = mpsc::channel::<CommandToBroker>(2);

        tokio::spawn(async move { broker_1.start(main_to_broker_1_rx).await });

        let broker_2_cancellation_token = cancellation_token.child_token();
        let mut broker_2 = Broker::new(
            "127.0.0.1",
            5051,
            peer_listener_port,
            data_dir_path,
            broker_heartbeat_interval_ms,
            broker_2_cancellation_token,
            cluster_info,
        );
        let (main_to_broker_2_tx, main_to_broker_2_rx) = mpsc::channel::<CommandToBroker>(2);

        let broker_2_join_handle = tokio::spawn(async move { broker_2.start(main_to_broker_2_rx).await });

        let peer_listener_address = format!("{}:{}", "127.0.0.1", peer_listener_port);
        let peer_listener_cancellation_token = cancellation_token.child_token();
        tokio::spawn(async move {
            crate::peer::start_peer_listener(
                peer_listener_address,
                main_to_broker_2_tx,
                peer_listener_cancellation_token,
            )
            .await
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
        let register_peer_command = CommandToBroker::RegisterPeer {
            peer_info: BrokerInfo {
                address: "127.0.0.1:5052".to_string(),
                partition_leaders: vec![],
            },
            broker_tx: oneshot_tx,
        };

        main_to_broker_1_tx.send(register_peer_command).await.unwrap();
        match oneshot_rx.await {
            Ok(response) => {
                assert!(matches!(response, BrokerResponse::PeerRegistered));
            }
            Err(e) => {
                panic!("Failed to receive response from Broker 1: {:?}", e);
            }
        }

        let (oneshot_tx, _) = oneshot::channel::<BrokerResponse>();
        let topic_to_create = Topic {
            id: None,
            name: "test_topic".to_string(),
            num_partitions: 2,
            replication_factor: 2,
            retention_period_minutes: 1,
            ack_level: AckLevel::Leader,
        };
        let create_topic_command = CommandToBroker::CreateNewTopic {
            topic: topic_to_create,
            broker_tx: oneshot_tx,
        };
        main_to_broker_1_tx.send(create_topic_command).await.unwrap();

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
        let get_partition_writer_command = CommandToBroker::GetPartitionWriter {
            topic_name: "test_topic".to_string(),
            broker_tx: oneshot_tx,
        };
        main_to_broker_1_tx.send(get_partition_writer_command).await.unwrap();

        match oneshot_rx.await {
            Ok(response) => match response {
                BrokerResponse::PartitionManagerFound { tx } => {
                    tracing::info!("Received partition manager handle for topic: test_topic: {:?}", tx);
                }
                _ => panic!("Expected PartitionManagerFound response, got: {:?}", response),
            },
            Err(e) => {
                panic!("Failed to receive response from Broker 1: {:?}", e);
            }
        }

        cancellation_token.cancel();
        broker_2_join_handle.await.unwrap();
    }

    #[tokio::test]
    // #[ignore]
    #[traced_test]
    async fn test_broker_should_be_able_to_create_new_topic() {
        let broker_heartbeat_interval_ms = 50;
        let peer_listener_port = 5058;
        let data_dir_path: &str = "/tmp/walrs/data";
        let cancellation_token = CancellationToken::new();
        let cluster_info = super::ClusterInfo {
            brokers: HashMap::new(),
            topics_in_cluster: vec![],
        };

        let broker_1_cancellation_token = cancellation_token.child_token();
        let mut broker_1 = Broker::new(
            "127.0.0.1",
            5056,
            peer_listener_port,
            data_dir_path,
            broker_heartbeat_interval_ms,
            broker_1_cancellation_token,
            cluster_info.clone(),
        );
        let (main_to_broker_1_tx, main_to_broker_1_rx) = mpsc::channel::<CommandToBroker>(2);

        tokio::spawn(async move { broker_1.start(main_to_broker_1_rx).await });

        let broker_2_cancellation_token = cancellation_token.child_token();
        let mut broker_2 = Broker::new(
            "127.0.0.1",
            5057,
            peer_listener_port,
            data_dir_path,
            broker_heartbeat_interval_ms,
            broker_2_cancellation_token,
            cluster_info,
        );
        let (main_to_broker_2_tx, main_to_broker_2_rx) = mpsc::channel::<CommandToBroker>(2);

        let broker_2_join_handle = tokio::spawn(async move { broker_2.start(main_to_broker_2_rx).await });

        let peer_listener_address = format!("{}:{}", "127.0.0.1", peer_listener_port);
        let peer_listener_cancellation_token = cancellation_token.child_token();
        tokio::spawn(async move {
            crate::peer::start_peer_listener(
                peer_listener_address,
                main_to_broker_2_tx,
                peer_listener_cancellation_token,
            )
            .await
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
        let register_peer_command = CommandToBroker::RegisterPeer {
            peer_info: BrokerInfo {
                address: "127.0.0.1:5058".to_string(),
                partition_leaders: vec![],
            },
            broker_tx: oneshot_tx,
        };

        main_to_broker_1_tx.send(register_peer_command).await.unwrap();
        match oneshot_rx.await {
            Ok(response) => {
                assert!(matches!(response, BrokerResponse::PeerRegistered));
            }
            Err(e) => {
                panic!("Failed to receive response from Broker 1: {:?}", e);
            }
        }

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
        let topic_to_create = Topic {
            id: None,
            name: "test_topic".to_string(),
            num_partitions: 2,
            replication_factor: 2,
            retention_period_minutes: 1,
            ack_level: AckLevel::Leader,
        };
        let create_topic_command = CommandToBroker::CreateNewTopic {
            topic: topic_to_create,
            broker_tx: oneshot_tx,
        };
        main_to_broker_1_tx.send(create_topic_command).await.unwrap();
        let peer_listener_address = format!("{}:{}", "127.0.0.1", peer_listener_port);
        match oneshot_rx.await {
            Ok(response) => match response {
                BrokerResponse::TopicCreated { partition_leaders } => {
                    assert_eq!(partition_leaders.len(), 2);
                    assert!(partition_leaders.contains_key(&0));
                    assert!(partition_leaders.contains_key(&1));
                    match partition_leaders.get(&0) {
                        Some(leader) => assert_eq!(leader, &peer_listener_address),
                        None => panic!("Expected leader for partition 0"),
                    }
                    match partition_leaders.get(&1) {
                        Some(leader) => assert_eq!(leader, "127.0.0.1:5058"),
                        None => panic!("Expected leader for partition 1"),
                    }
                }
                _ => panic!("Expected TopicCreated response, got: {:?}", response),
            },
            Err(e) => {
                panic!("Failed to receive response from Broker 1: {:?}", e);
            }
        }

        cancellation_token.cancel();
        broker_2_join_handle.await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_broker_should_be_able_to_register_peer() {
        let peer_listener_port = 5058;
        let broker_heartbeat_interval_ms = 50;
        let data_dir_path: &str = "/tmp/walrs/data";
        let cancellation_token = CancellationToken::new();

        let cluster_info = super::ClusterInfo {
            brokers: HashMap::new(),
            topics_in_cluster: vec![],
        };

        let broker_1_cancellation_token = cancellation_token.child_token();
        let mut broker_1 = Broker::new(
            "127.0.0.1",
            5056,
            peer_listener_port,
            data_dir_path,
            broker_heartbeat_interval_ms,
            broker_1_cancellation_token,
            cluster_info.clone(),
        );
        let (main_to_broker_1_tx, main_to_broker_1_rx) = mpsc::channel::<CommandToBroker>(2);

        tokio::spawn(async move { broker_1.start(main_to_broker_1_rx).await });

        let broker_2_cancellation_token = cancellation_token.child_token();
        let mut broker_2 = Broker::new(
            "127.0.0.1",
            5057,
            peer_listener_port,
            data_dir_path,
            broker_heartbeat_interval_ms,
            broker_2_cancellation_token,
            cluster_info,
        );
        let (_, main_to_broker_2_rx) = mpsc::channel::<CommandToBroker>(2);

        tokio::spawn(async move { broker_2.start(main_to_broker_2_rx).await });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
        let register_peer_command = CommandToBroker::RegisterPeer {
            peer_info: BrokerInfo {
                address: "127.0.0.1:5058".to_string(),
                partition_leaders: vec![],
            },
            broker_tx: oneshot_tx,
        };

        main_to_broker_1_tx.send(register_peer_command).await.unwrap();
        match oneshot_rx.await {
            Ok(response) => {
                assert!(matches!(response, BrokerResponse::PeerRegistered));
            }
            Err(e) => {
                panic!("Failed to receive response from Broker 1: {:?}", e);
            }
        }
        let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
        let get_status_command = CommandToBroker::GetStatus { broker_tx: oneshot_tx };
        main_to_broker_1_tx.send(get_status_command).await.unwrap();
        match oneshot_rx.await {
            Ok(response) => match response {
                BrokerResponse::Status { info } => {
                    assert_eq!(info.address, "127.0.0.1");
                    assert_eq!(info.partition_leaders.len(), 0);
                }
                _ => panic!("Expected Status response, got: {:?}", response),
            },
            Err(e) => {
                panic!("Failed to receive response from Broker 1: {:?}", e);
            }
        }

        cancellation_token.cancel();
    }
}
