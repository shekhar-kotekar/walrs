// use std::collections::HashMap;

// use common::models::Topic;
// use tokio::{
//     io::AsyncWriteExt,
//     sync::{mpsc, oneshot},
// };
// use tokio_util::sync::CancellationToken;

// use crate::{
//     models::{BrokerInfo, BrokerResponse, ClusterInfo, CommandToBroker, CommandToPeer, PartitionCommand, PeerResponse},
//     partition_managers::{partition_reader::PartitionReader, partition_writer::PartitionWriter},
// };

// const PARTITION_WRITER_MAX_QUEUE_SIZE: usize = 1000;

// #[derive(Debug)]
// pub struct Broker {
//     partition_managers: HashMap<String, mpsc::Sender<PartitionCommand>>,
//     cancellation_token: CancellationToken,
//     heartbeat_interval: tokio::time::Interval,
//     cluster_info: ClusterInfo,
//     data_dir_path: String,
//     peer_listener_address: String,
// }

// impl Broker {
//     pub fn new(
//         ip_address: &str,
//         broker_port: u16,
//         peer_listener_port: u16,
//         data_dir_path: &str,
//         heartbeat_interval_ms: u16,
//         cancellation_token: CancellationToken,
//         mut cluster_info: ClusterInfo,
//     ) -> Self {
//         let heartbeat_interval = tokio::time::interval(std::time::Duration::from_millis(heartbeat_interval_ms as u64));

//         let data_dir_path = format!(
//             "{}/{}-{}",
//             data_dir_path,
//             ip_address.replace([':', '.'], "-"),
//             broker_port
//         );
//         std::fs::create_dir_all(&data_dir_path).unwrap_or_else(|_| {
//             panic!("Failed to create base directory: {}", &data_dir_path);
//         });

//         let self_status = BrokerInfo::new(ip_address.to_string());
//         cluster_info.brokers.insert(String::from("self"), self_status.clone());

//         Broker {
//             cancellation_token,
//             partition_managers: HashMap::new(),
//             heartbeat_interval,
//             cluster_info,

//             data_dir_path,
//             peer_listener_address: format!("{}:{}", ip_address, peer_listener_port),
//         }
//     }

//     pub async fn start(&mut self, mut main_rx: mpsc::Receiver<CommandToBroker>) {
//         tracing::info!("Starting broker.");
//         loop {
//             tokio::select! {
//                 Some(command) = main_rx.recv() => {
//                     match command {
//                         CommandToBroker::RegisterPeer { peer_info, broker_tx } => {
//                             let peer_to_register = peer_info.address.clone();
//                             self.cluster_info.brokers.insert(peer_info.address.clone(), peer_info);
//                             broker_tx.send(BrokerResponse::PeerRegistered).unwrap_or_else(|e| {
//                                 tracing::error!("Failed to send response for RegisterPeer command: {:?}", e);
//                             });
//                             tracing::info!("peer registered: {}", &peer_to_register);
//                         }
//                         CommandToBroker::GetPartitionLeaders { topics, broker_tx } => {
//                             // tracing::info!("Received request for partition leaders for topics: {:?}", topics);
//                             // let mut partition_leaders: HashMap<String, Vec<String>> = HashMap::new();
//                             // for topic_name in topics {
//                             //     if let Some(topic_metadata) = self.cluster_info.topics_in_cluster.get(&topic_name) {
//                             //         for (_, leader) in &topic_metadata.partition_leaders {
//                             //             partition_leaders.entry(topic_name.clone()).or_default().push(leader.clone());
//                             //         }
//                             //     } else {
//                             //         tracing::warn!("Topic {} not found in cluster metadata", topic_name);
//                             //     }
//                             // }
//                             // broker_tx.send(BrokerResponse::PartitionLeaders { partition_leaders }).unwrap_or_else(|e| {
//                             //     tracing::error!("Failed to send response for GetPartitionLeaders command: {:?}", e);
//                             // });
//                         }

//                         CommandToBroker::CreateNewTopic { topic, broker_tx } => self.create_new_topic(topic, broker_tx, self.cancellation_token.child_token()).await,

//                         CommandToBroker::CreatePartitionWriter { topic_name, partition_number, broker_tx, role } => {
//                             let partition_writer_name = format!("{}-{}-{:?}", topic_name, partition_number, role);
//                             let response = if self.partition_managers.contains_key(&partition_writer_name) {
//                                 tracing::warn!("Partition writer already exists for topic: {}, partition: {}, role: {:?}", topic_name, partition_number, role);
//                                 BrokerResponse::PartitionWriterCreated
//                             } else {
//                                 // let partition_writer_tx_sender = broker_helper::create_partition_writer(&topic_name, partition_number, role).await;
//                                 // self.partition_managers.insert(partition_writer_name, partition_writer_tx_sender);
//                                 BrokerResponse::PartitionWriterCreated
//                             };
//                             broker_tx.send(response).unwrap_or_else(|e| {
//                                 tracing::error!("Failed to send response for CreatePartitionWriter command: {:?}", e);
//                             });
//                         }
//                         CommandToBroker::GetPartitionWriter { topic_name, broker_tx } => {
//                             // search in partition_managers for partition writer starting with given topic_name
//                             let partition_0_writer_name = format!("{}-0-Leader", topic_name);
//                             if let Some(partition_manager) = self.partition_managers.get(&partition_0_writer_name) {
//                                 broker_tx.send(BrokerResponse::PartitionManagerFound { tx: partition_manager.clone() }).unwrap_or_else(|e| {
//                                     tracing::error!("Failed to send response for GetPartitionWriter command: {:?}", e);
//                                 });
//                             } else {
//                                 let response = BrokerResponse::BrokerError {
//                                     message: format!("No partition writer found for topic: {}", topic_name),
//                                 };
//                                 broker_tx.send(response).unwrap_or_else(|e| {
//                                     tracing::error!("Failed to send response for GetPartitionWriter command: {:?}", e);
//                                 });
//                             }
//                         }
//                         CommandToBroker::GetPartitionReader { topic_name, broker_tx } => self.handle_get_partition_reader_command(topic_name, broker_tx).await,
//                         CommandToBroker::Heartbeat { sender_address, sender_status, broker_tx } => self.update_cluster_info(&sender_address, sender_status, broker_tx).await,
//                         CommandToBroker::GetTopicMetadata { topic_name, broker_tx } => self.get_topic_metadata(&topic_name, broker_tx).await
//                     }
//                 }
//                 _ = self.heartbeat_interval.tick() => {
//                     let peers: Vec<String> = self.cluster_info.brokers.iter().map(|(k, _)| k.clone()).filter(|address| address != "self").collect();

//                     match self.cluster_info.brokers.get("self") {
//                         Some(broker_current_status) => {
//                             tracing::info!("Sending heartbeat to peers: {:?}", peers);
//                             let peer_listener_address_clone = self.peer_listener_address.clone();
//                             tokio::spawn(async move {
//                                 Self::send_heartbeat(*broker_current_status, peer_listener_address_clone, peers).await;
//                             });
//                         }
//                         None => {
//                             tracing::warn!("Self broker status not found in cluster info. Cannot send heartbeat.");
//                             continue;
//                         }
//                     }
//                 }
//                 _ = self.cancellation_token.cancelled() => {
//                     tracing::info!("Cancellation token cancelled. broker shutting down...");
//                     break;
//                 }
//             }
//         }
//         tracing::info!("broker stopped.");
//     }

//     async fn create_local_partition_leader(
//         topic_name: String,
//         partition_number: u8,
//         data_dir_path: String,
//         remote_followers: Vec<String>,
//         cancellation_token: CancellationToken,
//     ) -> Option<mpsc::Sender<PartitionCommand>> {
//         let cancellation_token_clone = cancellation_token.clone();
//         tokio::select! {
//             _ = cancellation_token.cancelled() => {
//                 tracing::info!("Cancellation token cancelled. Stopping partition follower creation for topic: {}, partition: {}", topic_name, partition_number);
//                 None
//             }
//             _ = async {
//                 tracing::info!("Creating local partition follower for topic: {}, partition: {}", topic_name, partition_number);
//                 let mut partition_follower = PartitionWriter::new(&topic_name, partition_number, &data_dir_path);
//                 let (partition_follower_tx, partition_follower_rx) =
//                     mpsc::channel::<PartitionCommand>(PARTITION_WRITER_MAX_QUEUE_SIZE);
//                 partition_follower.start(partition_follower_rx, cancellation_token_clone).await;

//                 Some(partition_follower_tx)
//             } => {
//                 None
//             }
//         }
//     }

//     async fn create_local_partition_follower(
//         topic_name: String,
//         partition_number: u8,
//         data_dir_path: String,
//         cancellation_token: CancellationToken,
//     ) -> Option<mpsc::Sender<PartitionCommand>> {
//         let cancellation_token_clone = cancellation_token.clone();
//         tokio::select! {
//             _ = cancellation_token.cancelled() => {
//                 tracing::info!("Cancellation token cancelled. Stopping partition follower creation for topic: {}, partition: {}", topic_name, partition_number);
//                 None
//             }
//             _ = async {
//                 tracing::info!("Creating local partition follower for topic: {}, partition: {}", topic_name, partition_number);
//                 let mut partition_follower = PartitionWriter::new(&topic_name, partition_number, &data_dir_path);
//                 let (partition_follower_tx, partition_follower_rx) =
//                     mpsc::channel::<PartitionCommand>(PARTITION_WRITER_MAX_QUEUE_SIZE);
//                 partition_follower.start(partition_follower_rx, cancellation_token_clone).await;
//                 Some(partition_follower_tx)
//             } => {
//                 None
//             }
//         }
//     }

//     async fn create_remote_partition_leader(
//         topic_name: String,
//         partition_number: u8,
//         cancellation_token: CancellationToken,
//     ) {
//         tokio::select! {
//             _ = cancellation_token.cancelled() => {
//                 tracing::info!("Cancellation token cancelled. Stopping remote partition leader creation for topic: {}, partition: {}", topic_name, partition_number);
//             }
//             _ = async {
//                 tracing::info!("Creating remote partition leader for topic: {}, partition: {}", topic_name, partition_number);
//             } => {}
//         }
//     }

//     async fn create_new_topic(
//         &self,
//         topic: Topic,
//         broker_tx: oneshot::Sender<BrokerResponse>,
//         cancellation_token: CancellationToken,
//     ) {
//         tracing::info!("Creating new topic: {}", topic.name);
//         let cluster_info = self.cluster_info.clone();
//         let data_dir_path = self.data_dir_path.clone();

//         tokio::spawn(async move {
//             let broker_response = if cluster_info.topics_in_cluster.contains_key(&topic.name) {
//                 BrokerResponse::TopicAlreadyExists
//             } else {
//                 BrokerResponse::BrokerError {
//                     message: String::from("Topic creation not implemented yet"),
//                 }
//             };
//             let _ = broker_tx.send(broker_response);
//         });
//     }

//     async fn get_topic_metadata(&self, topic_name: &String, broker_tx: oneshot::Sender<BrokerResponse>) {
//         tracing::info!("Fetching metadata for topic: {}", topic_name);
//         if let Some(topic_metadata) = self.cluster_info.topics_in_cluster.get(topic_name) {
//             broker_tx
//                 .send(BrokerResponse::TopicMetadata {
//                     metadata: topic_metadata.clone(),
//                 })
//                 .unwrap_or_else(|e| {
//                     tracing::error!("Failed to send response for GetTopicMetadata command: {:?}", e);
//                 });
//         } else {
//             broker_tx.send(BrokerResponse::TopicNotFound).unwrap_or_else(|e| {
//                 tracing::error!("Failed to send response for GetTopicMetadata command: {:?}", e);
//             });
//         }
//     }

//     async fn update_cluster_info(
//         &mut self,
//         peer_address: &str,
//         peer_status: BrokerInfo,
//         broker_tx: oneshot::Sender<BrokerResponse>,
//     ) {
//         tracing::info!("heartbeat received: {:?}", peer_status);
//         self.cluster_info
//             .brokers
//             .insert(peer_address.to_string(), peer_status.clone());
//         let _ = broker_tx.send(BrokerResponse::HeartbeatReceived);
//     }

//     async fn handle_get_partition_reader_command(
//         &mut self,
//         topic_name: String,
//         broker_tx: oneshot::Sender<BrokerResponse>,
//     ) {
//         let partition_reader_name = format!("{}-reader", topic_name);
//         let response: BrokerResponse =
//             if let Some(partition_reader_tx) = self.partition_managers.get(&partition_reader_name) {
//                 tracing::info!("Found partition reader for topic: {}", topic_name);
//                 BrokerResponse::PartitionManagerFound {
//                     tx: partition_reader_tx.clone(),
//                 }
//             } else {
//                 tracing::warn!("No partition reader found for topic: {}. Creating new", topic_name);
//                 let mut partition_reader = PartitionReader::new(topic_name.clone(), 0, self.data_dir_path.clone(), 100);
//                 let (partition_reader_tx, partition_reader_rx) =
//                     mpsc::channel::<PartitionCommand>(PARTITION_WRITER_MAX_QUEUE_SIZE);

//                 let partition_cancellation_token = self.cancellation_token.child_token();
//                 tokio::spawn(async move {
//                     partition_reader
//                         .start(partition_reader_rx, partition_cancellation_token)
//                         .await;
//                 });
//                 let partition_number = 0; // Assuming partition 0 for simplicity
//                 let partition_reader_name = format!("{}-{}-reader", topic_name, partition_number);
//                 self.partition_managers
//                     .insert(partition_reader_name, partition_reader_tx.clone());
//                 BrokerResponse::PartitionManagerFound {
//                     tx: partition_reader_tx,
//                 }
//             };
//         broker_tx.send(response).unwrap_or_else(|e| {
//             tracing::error!("Failed to send response for GetPartitionReader command: {:?}", e);
//         });
//     }

//     async fn send_heartbeat(broker_status: BrokerInfo, peer_listener_address: String, peers: Vec<String>) {
//         let message_to_peer: CommandToPeer = CommandToPeer::Heartbeat {
//             peer_listener_address,
//             broker_status,
//         };
//         let serialized_heartbeat = common::to_bytes(&message_to_peer);
//         let mut successful_peers = 0;
//         for peer in &peers {
//             match tokio::net::TcpStream::connect(peer).await {
//                 Ok(mut stream) => {
//                     tracing::debug!("Sending heartbeat to peer: {}", peer);
//                     if let Err(e) = stream.write_all(&serialized_heartbeat).await {
//                         tracing::error!("Failed to send heartbeat to {}: {}", peer, e);
//                         continue;
//                     }
//                     tracing::debug!("Heartbeat sent to peer: {}. Waiting for response.", peer);
//                     match common::read_command_from_socket::<PeerResponse>(&mut stream).await {
//                         Some(PeerResponse::HeartbeatReceived) => {
//                             tracing::debug!("Heartbeat accepted by peer: {}", peer);
//                             successful_peers += 1;
//                         }
//                         Some(PeerResponse::Error { message }) => {
//                             tracing::error!("Error response for heartbeat signal from peer {}: {}", peer, message);
//                         }
//                         _ => {
//                             tracing::error!("Failed to read response from peer: {}", peer);
//                         }
//                     }
//                 }
//                 Err(e) => {
//                     tracing::error!("Failed to connect to {}: {}", peer, e);
//                 }
//             }
//         }
//         tracing::info!(
//             "Heartbeat sent to {} out of {} peers. Total peers: {:?}",
//             successful_peers,
//             peers.len(),
//             peers
//         );
//     }
// }

// #[cfg(test)]
// mod should {

//     use common::models::{AckLevel, Topic};
//     use tokio::sync::{mpsc, oneshot};
//     use tokio_util::sync::CancellationToken;
//     use tracing_test::traced_test;

//     use crate::models::{BrokerInfo, BrokerResponse, ClusterInfo, CommandToBroker};

//     use super::Broker;

//     #[tokio::test]
//     // #[ignore]
//     #[traced_test]
//     async fn test_broker_should_be_able_to_send_partition_leader_info() {
//         let peer_listener_port = 5052;
//         let broker_heartbeat_interval_ms = 5000;
//         let data_dir_path: &str = "/tmp/walrs/test_data";
//         let cancellation_token = CancellationToken::new();
//         let cluster_info =
//             ClusterInfo::new().with_peers(vec!["127.0.0.1:5051".to_string(), "127.0.0.1:5052".to_string()]);

//         let broker_1_cancellation_token = cancellation_token.child_token();
//         let mut broker_1 = Broker::new(
//             "127.0.0.1:5051",
//             5050,
//             peer_listener_port,
//             data_dir_path,
//             broker_heartbeat_interval_ms,
//             broker_1_cancellation_token,
//             cluster_info.clone(),
//         );
//         let (main_to_broker_1_tx, main_to_broker_1_rx) = mpsc::channel::<CommandToBroker>(3);

//         tokio::spawn(async move { broker_1.start(main_to_broker_1_rx).await });

//         // let broker_2_cancellation_token = cancellation_token.child_token();
//         // let mut broker_2 = Broker::new(
//         //     "127.0.0.1",
//         //     5051,
//         //     peer_listener_port,
//         //     data_dir_path,
//         //     broker_heartbeat_interval_ms,
//         //     broker_2_cancellation_token,
//         //     cluster_info,
//         // );
//         // let (_, main_to_broker_2_rx) = mpsc::channel::<CommandToBroker>(2);

//         // tokio::spawn(async move { broker_2.start(main_to_broker_2_rx).await });

//         let (oneshot_tx, _) = oneshot::channel::<BrokerResponse>();
//         let create_topic_1_command: CommandToBroker = CommandToBroker::CreateNewTopic {
//             topic: Topic {
//                 name: "test_topic_1".to_string(),
//                 num_partitions: 2,
//                 replication_factor: 1,
//                 retention_period_minutes: 1,
//                 ack_level: AckLevel::Leader,
//             },
//             broker_tx: oneshot_tx,
//         };
//         main_to_broker_1_tx.send(create_topic_1_command).await.unwrap();

//         let (oneshot_tx, _) = oneshot::channel::<BrokerResponse>();
//         let create_topic_2_command: CommandToBroker = CommandToBroker::CreateNewTopic {
//             topic: Topic {
//                 name: "test_topic_2".to_string(),
//                 num_partitions: 2,
//                 replication_factor: 1,
//                 retention_period_minutes: 1,
//                 ack_level: AckLevel::Leader,
//             },
//             broker_tx: oneshot_tx,
//         };
//         main_to_broker_1_tx.send(create_topic_2_command).await.unwrap();

//         let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
//         let get_partition_leaders_command = CommandToBroker::GetPartitionLeaders {
//             topics: vec![
//                 "test_topic_1".to_string(),
//                 "test_topic_2".to_string(),
//                 "test_topic_3".to_string(),
//             ],
//             broker_tx: oneshot_tx,
//         };
//         main_to_broker_1_tx.send(get_partition_leaders_command).await.unwrap();
//         match oneshot_rx.await {
//             Ok(response) => match response {
//                 BrokerResponse::PartitionLeaders { partition_leaders } => {
//                     tracing::info!("Received partition leaders: {:?}", partition_leaders);
//                     assert!(partition_leaders.contains_key("test_topic_1"));
//                     assert!(partition_leaders.contains_key("test_topic_2"));
//                     assert!(!partition_leaders.contains_key("test_topic_3"));
//                 }
//                 _ => panic!("Expected PartitionLeaders response, got: {:?}", response),
//             },
//             Err(e) => {
//                 panic!("Failed to receive response from Broker 1: {:?}", e);
//             }
//         }

//         cancellation_token.cancel();
//     }

//     #[tokio::test]
//     #[ignore]
//     #[traced_test]
//     async fn test_broker_should_be_able_to_return_handle_to_partition_writer() {
//         let peer_listener_port = 5052;
//         let broker_heartbeat_interval_ms = 50;
//         let data_dir_path: &str = "/tmp/walrs/data";
//         let cancellation_token = CancellationToken::new();
//         let cluster_info = ClusterInfo::new();

//         let broker_1_cancellation_token = cancellation_token.child_token();
//         let mut broker_1 = Broker::new(
//             "127.0.0.1:5051",
//             5050,
//             peer_listener_port,
//             data_dir_path,
//             broker_heartbeat_interval_ms,
//             broker_1_cancellation_token,
//             cluster_info.clone(),
//         );
//         let (main_to_broker_1_tx, main_to_broker_1_rx) = mpsc::channel::<CommandToBroker>(2);

//         tokio::spawn(async move { broker_1.start(main_to_broker_1_rx).await });

//         let broker_2_cancellation_token = cancellation_token.child_token();
//         let mut broker_2 = Broker::new(
//             "127.0.0.1",
//             5051,
//             peer_listener_port,
//             data_dir_path,
//             broker_heartbeat_interval_ms,
//             broker_2_cancellation_token,
//             cluster_info,
//         );
//         let (main_to_broker_2_tx, main_to_broker_2_rx) = mpsc::channel::<CommandToBroker>(2);

//         let broker_2_join_handle = tokio::spawn(async move { broker_2.start(main_to_broker_2_rx).await });

//         let peer_listener_address = format!("{}:{}", "127.0.0.1", peer_listener_port);
//         let peer_listener_cancellation_token = cancellation_token.child_token();
//         tokio::spawn(async move {
//             crate::peer::start_peer_listener(
//                 peer_listener_address,
//                 main_to_broker_2_tx,
//                 peer_listener_cancellation_token,
//             )
//             .await
//         });

//         let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
//         let register_peer_command = CommandToBroker::RegisterPeer {
//             peer_info: BrokerInfo {
//                 address: "127.0.0.1:5052".to_string(),
//                 registed_topics: vec![],
//             },
//             broker_tx: oneshot_tx,
//         };

//         main_to_broker_1_tx.send(register_peer_command).await.unwrap();
//         match oneshot_rx.await {
//             Ok(response) => {
//                 assert!(matches!(response, BrokerResponse::PeerRegistered));
//             }
//             Err(e) => {
//                 panic!("Failed to receive response from Broker 1: {:?}", e);
//             }
//         }

//         let (oneshot_tx, _) = oneshot::channel::<BrokerResponse>();
//         let topic_to_create = Topic {
//             name: "test_topic".to_string(),
//             num_partitions: 2,
//             replication_factor: 2,
//             retention_period_minutes: 1,
//             ack_level: AckLevel::Leader,
//         };
//         let create_topic_command = CommandToBroker::CreateNewTopic {
//             topic: topic_to_create,
//             broker_tx: oneshot_tx,
//         };
//         main_to_broker_1_tx.send(create_topic_command).await.unwrap();

//         let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
//         let get_partition_writer_command = CommandToBroker::GetPartitionWriter {
//             topic_name: "test_topic".to_string(),
//             broker_tx: oneshot_tx,
//         };
//         main_to_broker_1_tx.send(get_partition_writer_command).await.unwrap();

//         match oneshot_rx.await {
//             Ok(response) => match response {
//                 BrokerResponse::PartitionManagerFound { tx } => {
//                     tracing::info!("Received partition manager handle for topic: test_topic: {:?}", tx);
//                 }
//                 _ => panic!("Expected PartitionManagerFound response, got: {:?}", response),
//             },
//             Err(e) => {
//                 panic!("Failed to receive response from Broker 1: {:?}", e);
//             }
//         }

//         cancellation_token.cancel();
//         broker_2_join_handle.await.unwrap();
//     }

//     #[tokio::test]
//     #[ignore]
//     #[traced_test]
//     async fn test_broker_should_be_able_to_create_new_topic() {
//         let broker_heartbeat_interval_ms = 50;
//         let peer_listener_port = 5058;
//         let data_dir_path: &str = "/tmp/walrs/data";
//         let cancellation_token = CancellationToken::new();
//         let cluster_info = ClusterInfo::new();

//         let broker_1_cancellation_token = cancellation_token.child_token();
//         let mut broker_1 = Broker::new(
//             "127.0.0.1",
//             5056,
//             peer_listener_port,
//             data_dir_path,
//             broker_heartbeat_interval_ms,
//             broker_1_cancellation_token,
//             cluster_info.clone(),
//         );
//         let (main_to_broker_1_tx, main_to_broker_1_rx) = mpsc::channel::<CommandToBroker>(2);

//         tokio::spawn(async move { broker_1.start(main_to_broker_1_rx).await });

//         let broker_2_cancellation_token = cancellation_token.child_token();
//         let mut broker_2 = Broker::new(
//             "127.0.0.1",
//             5057,
//             peer_listener_port,
//             data_dir_path,
//             broker_heartbeat_interval_ms,
//             broker_2_cancellation_token,
//             cluster_info,
//         );
//         let (main_to_broker_2_tx, main_to_broker_2_rx) = mpsc::channel::<CommandToBroker>(2);

//         let broker_2_join_handle = tokio::spawn(async move { broker_2.start(main_to_broker_2_rx).await });

//         let peer_listener_address = format!("{}:{}", "127.0.0.1", peer_listener_port);
//         let peer_listener_cancellation_token = cancellation_token.child_token();
//         tokio::spawn(async move {
//             crate::peer::start_peer_listener(
//                 peer_listener_address,
//                 main_to_broker_2_tx,
//                 peer_listener_cancellation_token,
//             )
//             .await
//         });

//         let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
//         let register_peer_command = CommandToBroker::RegisterPeer {
//             peer_info: BrokerInfo {
//                 address: "127.0.0.1:5058".to_string(),
//                 registed_topics: vec![],
//             },
//             broker_tx: oneshot_tx,
//         };

//         main_to_broker_1_tx.send(register_peer_command).await.unwrap();
//         match oneshot_rx.await {
//             Ok(response) => {
//                 assert!(matches!(response, BrokerResponse::PeerRegistered));
//             }
//             Err(e) => {
//                 panic!("Failed to receive response from Broker 1: {:?}", e);
//             }
//         }

//         let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
//         let topic_to_create = Topic {
//             name: "test_topic".to_string(),
//             num_partitions: 2,
//             replication_factor: 2,
//             retention_period_minutes: 1,
//             ack_level: AckLevel::Leader,
//         };
//         let create_topic_command = CommandToBroker::CreateNewTopic {
//             topic: topic_to_create,
//             broker_tx: oneshot_tx,
//         };
//         main_to_broker_1_tx.send(create_topic_command).await.unwrap();
//         let peer_listener_address = format!("{}:{}", "127.0.0.1", peer_listener_port);
//         match oneshot_rx.await {
//             Ok(response) => match response {
//                 BrokerResponse::TopicCreated { partition_leaders } => {
//                     assert_eq!(partition_leaders.len(), 2);
//                     assert!(partition_leaders.contains_key(&0));
//                     assert!(partition_leaders.contains_key(&1));
//                     match partition_leaders.get(&0) {
//                         Some(leader) => assert_eq!(leader, &peer_listener_address),
//                         None => panic!("Expected leader for partition 0"),
//                     }
//                     match partition_leaders.get(&1) {
//                         Some(leader) => assert_eq!(leader, "127.0.0.1:5058"),
//                         None => panic!("Expected leader for partition 1"),
//                     }
//                 }
//                 _ => panic!("Expected TopicCreated response, got: {:?}", response),
//             },
//             Err(e) => {
//                 panic!("Failed to receive response from Broker 1: {:?}", e);
//             }
//         }

//         cancellation_token.cancel();
//         broker_2_join_handle.await.unwrap();
//     }

//     #[tokio::test]
//     #[ignore]
//     #[traced_test]
//     async fn test_broker_should_be_able_to_register_peer() {
//         let peer_listener_port = 5058;
//         let broker_heartbeat_interval_ms = 50;
//         let data_dir_path: &str = "/tmp/walrs/data";
//         let cancellation_token = CancellationToken::new();

//         let cluster_info = ClusterInfo::new();

//         let broker_1_cancellation_token = cancellation_token.child_token();
//         let mut broker_1 = Broker::new(
//             "127.0.0.1",
//             5056,
//             peer_listener_port,
//             data_dir_path,
//             broker_heartbeat_interval_ms,
//             broker_1_cancellation_token,
//             cluster_info.clone(),
//         );
//         let (main_to_broker_1_tx, main_to_broker_1_rx) = mpsc::channel::<CommandToBroker>(2);

//         tokio::spawn(async move { broker_1.start(main_to_broker_1_rx).await });

//         let broker_2_cancellation_token = cancellation_token.child_token();
//         let mut broker_2 = Broker::new(
//             "127.0.0.1",
//             5057,
//             peer_listener_port,
//             data_dir_path,
//             broker_heartbeat_interval_ms,
//             broker_2_cancellation_token,
//             cluster_info,
//         );
//         let (_, main_to_broker_2_rx) = mpsc::channel::<CommandToBroker>(2);

//         tokio::spawn(async move { broker_2.start(main_to_broker_2_rx).await });

//         let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
//         let register_peer_command = CommandToBroker::RegisterPeer {
//             peer_info: BrokerInfo {
//                 address: "127.0.0.1:5058".to_string(),
//                 registed_topics: vec![],
//             },
//             broker_tx: oneshot_tx,
//         };

//         main_to_broker_1_tx.send(register_peer_command).await.unwrap();
//         match oneshot_rx.await {
//             Ok(response) => {
//                 assert!(matches!(response, BrokerResponse::PeerRegistered));
//             }
//             Err(e) => {
//                 panic!("Failed to receive response from Broker 1: {:?}", e);
//             }
//         }
//         // let get_status_command = CommandToBroker::GetStatus { broker_tx: oneshot_tx };
//         // main_to_broker_1_tx.send(get_status_command).await.unwrap();
//         // match oneshot_rx.await {
//         //     Ok(response) => match response {
//         //         BrokerResponse::Status { info } => {
//         //             assert_eq!(info.address, "127.0.0.1");
//         //             assert_eq!(info.partition_leaders.len(), 0);
//         //         }
//         //         _ => panic!("Expected Status response, got: {:?}", response),
//         //     },
//         //     Err(e) => {
//         //         panic!("Failed to receive response from Broker 1: {:?}", e);
//         //     }
//         // }

//         cancellation_token.cancel();
//     }
// }
