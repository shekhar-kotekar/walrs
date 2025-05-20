// use common::models::AckLevel;
// use tokio::sync::mpsc;
// use tokio_util::sync::CancellationToken;

// pub struct Leader {
//     partition_num: u8,
//     topic_name: String,
//     follower_address: Vec<String>,
//     ack_level: AckLevel,
// }

// trait PartitionTask {
//     async fn start(
//         &self,
//         main_rx: mpsc::Receiver<PartitionCommand>,
//         cancellation_token: CancellationToken,
//     );
// }

// impl Leader {
//     async fn write_messages(&self, messages: Vec<Message>) -> PartitionResponse {
//         tracing::info!("Leader received messages: {:?}", messages);
//         match self.ack_level {
//             AckLevel::None => {
//                 tracing::info!("Leader ack level is set to None. Sending ack immediately.");
//                 PartitionResponse::MessagesPersisted
//             }
//             AckLevel::Leader => {
//                 tracing::info!(
//                     "Leader ack level is set to Leader. Sending ack after leader has written messages to its WAL."
//                 );
//                 PartitionResponse::MessagesPersisted
//             }
//             AckLevel::All => {
//                 tracing::info!(
//                     "Leader ack level is set to All. Sending ack after all the followers have written messages to its WAL."
//                 );
//                 PartitionResponse::MessagesPersisted
//             }
//         }
//     }
// }

// impl PartitionTask for Leader {
//     async fn start(
//         &self,
//         mut main_rx: mpsc::Receiver<PartitionCommand>,
//         cancellation_token: CancellationToken,
//     ) {
//         tracing::info!(
//             "Starting leader for partition {} of topic {}",
//             self.partition_num,
//             self.topic_name
//         );
//         loop {
//             tokio::select! {
//                 Some(partition_command) = main_rx.recv() => {
//                     match partition_command {
//                         PartitionCommand::WriteMessages {messages, tx} => {
//                             let response = self.write_messages(messages).await;
//                             let _ = tx.send(response);
//                         }
//                     }
//                 }
//                 _ = cancellation_token.cancelled() => {
//                     tracing::info!("Leader task of topic {} for {} partition cancelled.", self.topic_name, self.partition_num);
//                     break;
//                 }
//             }
//         }
//     }
// }

// #[cfg(test)]
// mod should {
//     use super::*;
//     use crate::models::Message;
//     use tokio::sync::oneshot;
//     use tracing_test::traced_test;

//     #[test]
//     fn test_create_leader_object() {
//         let dummy_leader = Leader {
//             partition_num: 0,
//             topic_name: "test_topic".to_string(),
//             follower_address: vec!["follower_1".to_string(), "follower_2".to_string()],
//             ack_level: AckLevel::None,
//         };
//         assert!(dummy_leader.partition_num == 0);
//         assert!(dummy_leader.topic_name == "test_topic");
//         assert!(dummy_leader.follower_address.len() == 2);
//         assert!(dummy_leader.follower_address[0] == "follower_1");
//         assert!(dummy_leader.follower_address[1] == "follower_2");
//     }

//     #[tokio::test]
//     #[traced_test]
//     async fn test_leader_should_immediately_send_ack_when_ack_level_set_to_none() {
//         let (tx, rx) = mpsc::channel::<PartitionCommand>(1);
//         let dummy_leader = Leader {
//             partition_num: 0,
//             topic_name: "test_topic".to_string(),
//             follower_address: vec!["follower_1".to_string(), "follower_2".to_string()],
//             ack_level: AckLevel::None,
//         };
//         let cancellation_token = CancellationToken::new();
//         tokio::spawn(async move {
//             dummy_leader.start(rx, cancellation_token).await;
//         });

//         let message: Message = Message::new(
//             b"msg_key".to_vec(),
//             b"msg_value".to_vec(),
//             vec![("header1".to_string(), "value1".to_string())]
//                 .into_iter()
//                 .collect(),
//         );
//         tracing::info!("Sending message: {:?}", message);
//         let (message_tx, message_rx) = oneshot::channel::<PartitionResponse>();
//         let partition_command: PartitionCommand = PartitionCommand::WriteMessages {
//             messages: vec![message],
//             tx: message_tx,
//         };
//         let _ = tx.send(partition_command).await;
//         tracing::info!("Waiting for response");
//         match message_rx.await {
//             Ok(response) => {
//                 tracing::info!("Received response: {:?}", response);
//                 assert!(matches!(response, PartitionResponse::MessagesPersisted));
//             }
//             Err(e) => {
//                 panic!("Failed to receive response: {:?}", e);
//             }
//         }
//     }

//     #[tokio::test]
//     #[traced_test]
//     async fn test_when_ack_level_set_to_leader_then_it_should_send_ack_after_leader_has_written_message(
//     ) {
//         let dummy_leader = Leader {
//             partition_num: 0,
//             topic_name: "test_topic".to_string(),
//             follower_address: vec!["follower_1".to_string(), "follower_2".to_string()],
//             ack_level: AckLevel::Leader,
//         };
//         let (tx, rx) = mpsc::channel::<PartitionCommand>(1);
//         let cancellation_token = CancellationToken::new();
//         tokio::spawn(async move {
//             dummy_leader.start(rx, cancellation_token).await;
//         });

//         let message: Message = Message::new(
//             b"msg_key".to_vec(),
//             b"msg_value".to_vec(),
//             vec![("header1".to_string(), "value1".to_string())]
//                 .into_iter()
//                 .collect(),
//         );
//         tracing::info!("Sending message: {:?}", message);
//         let (message_tx, message_rx) = oneshot::channel::<PartitionResponse>();
//         let partition_command: PartitionCommand = PartitionCommand::WriteMessages {
//             messages: vec![message],
//             tx: message_tx,
//         };
//         let _ = tx.send(partition_command).await;
//         tracing::info!("Waiting for response");
//         match message_rx.await {
//             Ok(response) => {
//                 tracing::info!("Received response: {:?}", response);
//                 assert!(matches!(response, PartitionResponse::MessagesPersisted));
//             }
//             Err(e) => {
//                 panic!("Failed to receive response: {:?}", e);
//             }
//         }
//     }

//     #[tokio::test]
//     #[traced_test]
//     async fn test_when_ack_level_set_to_none_leader_should_send_ack_after_leader_has_written_message(
//     ) {
//         let dummy_leader = Leader {
//             partition_num: 0,
//             topic_name: "test_topic".to_string(),
//             follower_address: vec!["follower_1".to_string(), "follower_2".to_string()],
//             ack_level: AckLevel::All,
//         };
//         let (tx, rx) = mpsc::channel::<PartitionCommand>(1);
//         let cancellation_token = CancellationToken::new();
//         tokio::spawn(async move {
//             dummy_leader.start(rx, cancellation_token).await;
//         });

//         let message: Message = Message::new(
//             b"msg_key".to_vec(),
//             b"msg_value".to_vec(),
//             vec![("header1".to_string(), "value1".to_string())]
//                 .into_iter()
//                 .collect(),
//         );
//         tracing::info!("Sending message: {:?}", message);
//         let (message_tx, message_rx) = oneshot::channel::<PartitionResponse>();
//         let partition_command: PartitionCommand = PartitionCommand::WriteMessages {
//             messages: vec![message],
//             tx: message_tx,
//         };
//         let _ = tx.send(partition_command).await;
//         tracing::info!("Waiting for response");
//         match message_rx.await {
//             Ok(response) => {
//                 tracing::info!("Received response: {:?}", response);
//                 assert!(matches!(response, PartitionResponse::MessagesPersisted));
//             }
//             Err(e) => {
//                 panic!("Failed to receive response: {:?}", e);
//             }
//         }
//     }
// }
