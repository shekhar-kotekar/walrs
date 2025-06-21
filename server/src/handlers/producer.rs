use std::io::{Error, ErrorKind};

use commons::models::{
    Message, PartitionInfo, PeerCommand, ProducerCommand, ProducerResponse, Topic,
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    models::{PartitionCommand, PartitionWriterResponse},
    node_manager::{NodeManagerCommand, NodeManagerResponse},
};

pub async fn handle_producer_request(
    command: ProducerCommand,
    node_address: &str,
    node_manager_tx: mpsc::Sender<NodeManagerCommand>,
) -> ProducerResponse {
    match command {
        ProducerCommand::WriteMessages {
            topic,
            partition_number,
            messages,
        } => {
            // Assumption is that producer will always send write messages command to leader node.
            // but we have to validate if producer has sent messages to the correct leader node.
            // If the node which is handling this request is not the leader for the given topic and partition,
            // it should return redirect response with the correct leader address.
            tracing::info!("Writing {} messages for: {}", messages.len(), topic);
            let (writer, topic_info) =
                match get_info_from_node_manager(&topic, partition_number, node_manager_tx).await {
                    Ok((writer, topic_info)) => (writer, topic_info),
                    Err(err) => {
                        return ProducerResponse::Error {
                            message: err.to_string(),
                        };
                    }
                };
            tracing::debug!(
                "Information retrieved from node manager: topic: {}, partition: {}",
                topic,
                partition_number
            );

            if topic_info.num_partitions <= partition_number {
                return ProducerResponse::Error {
                    message: format!(
                        "Partition number {} is out of bounds for topic '{}'",
                        partition_number, topic
                    ),
                };
            }

            let partition_info: PartitionInfo =
                topic_info.partitions[partition_number as usize].clone();
            if partition_info.leader_address != node_address {
                return ProducerResponse::Redirect {
                    leader_address: partition_info.leader_address,
                };
            }
            // TODO: For time being we will compulsorily write messages to local partition writer AND
            // also to followers. In the future we will use ack_level to determine
            // if we need to write messages to followers or not.
            match write_messages_to_local(&writer, messages.clone()).await {
                PartitionWriterResponse::MessagesPersisted { count } => {
                    tracing::info!(
                        "Successfully persisted {} messages for topic: {}, partition: {}",
                        count,
                        topic,
                        partition_number
                    );
                    // Now send messages to followers
                    let mut send_follower_tasks = Vec::new();
                    // spawn a TOKIO task to send messages to each follower
                    for follower_address in partition_info.follower_addresses.iter() {
                        let follower_address = follower_address.clone();
                        let topic = topic.clone();
                        let messages = messages.clone();
                        let task = tokio::spawn(async move {
                            send_messages_to_follower(
                                follower_address,
                                topic,
                                partition_number,
                                messages,
                            )
                            .await;
                        });
                        send_follower_tasks.push(task);
                    }
                    // Wait for all follower tasks to complete
                    for task in send_follower_tasks {
                        if let Err(err) = task.await {
                            //TODO: Should we remove the messages from local partition writer if sending to followers fails?
                            tracing::error!("Failed to send messages to follower: {}", err);
                            return ProducerResponse::Error {
                                message: format!("Failed to send messages to followers: {}", err),
                            };
                        }
                    }
                    ProducerResponse::MessagesPersisted { count }
                }
                PartitionWriterResponse::Error { message } => ProducerResponse::Error {
                    message: format!("Failed to write messages: {}", message),
                },
            }
        }
    }
}

async fn send_messages_to_follower(
    follower_address: String,
    topic_name: String,
    partition_number: u8,
    messages: Vec<Message>,
) {
    let peer_command = PeerCommand::SyncMessages {
        topic_name,
        partition_number,
        messages,
    };
    commons::send_and_receive_peer_command(peer_command, &follower_address).await;
}

async fn get_info_from_node_manager(
    topic: &str,
    partition_number: u8,
    node_manager_tx: mpsc::Sender<NodeManagerCommand>,
) -> Result<(mpsc::Sender<PartitionCommand>, Topic), std::io::Error> {
    let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeManagerResponse>();
    let command = NodeManagerCommand::GetPartitionWriter {
        topic_name: topic.to_string(),
        partition_number,
        tx: oneshot_tx,
    };
    if let Err(err) = node_manager_tx.send(command).await {
        return Err(Error::new(
            ErrorKind::Other,
            format!("Failed to send command to node manager: {}", err),
        ));
    }
    match oneshot_rx.await {
        Ok(NodeManagerResponse::PartitionWriter { writer }) => {
            let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeManagerResponse>();
            let get_topic_info_command = NodeManagerCommand::GetTopicInfo {
                topic_names: vec![topic.to_string()],
                tx: oneshot_tx,
            };
            if let Err(err) = node_manager_tx.send(get_topic_info_command).await {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Failed to send GetTopicInfo command to node manager: {}",
                        err
                    ),
                ));
            }
            match oneshot_rx.await {
                Ok(NodeManagerResponse::TopicInfo { topics }) => {
                    if let Some(topic_info) = topics.into_iter().find(|t| t.name == topic) {
                        Ok((writer, topic_info))
                    } else {
                        Err(Error::new(
                            ErrorKind::NotFound,
                            format!("Topic '{}' not found", topic),
                        ))
                    }
                }
                Ok(other) => Err(Error::new(
                    ErrorKind::Other,
                    format!("Node manager error: {:?}", other),
                )),
                Err(err) => Err(Error::new(
                    ErrorKind::Other,
                    format!("Failed to receive response from node manager: {}", err),
                )),
            }
        }
        Ok(other) => Err(Error::new(
            ErrorKind::Other,
            format!("Node manager error: {:?}", other),
        )),
        Err(err) => Err(Error::new(
            ErrorKind::Other,
            format!("Failed to receive response from node manager: {}", err),
        )),
    }
}

async fn write_messages_to_local(
    writer: &mpsc::Sender<PartitionCommand>,
    messages: Vec<Message>,
) -> PartitionWriterResponse {
    let (pw_oneshot_tx, pw_oneshot_rx) = oneshot::channel::<PartitionWriterResponse>();
    let partition_command: PartitionCommand = PartitionCommand::WriteMessages {
        messages,
        tx: pw_oneshot_tx,
    };
    if let Err(err) = writer.send(partition_command).await {
        return PartitionWriterResponse::Error {
            message: format!("Failed to send command to partition writer: {}", err),
        };
    }
    match pw_oneshot_rx.await {
        Ok(PartitionWriterResponse::MessagesPersisted { count }) => {
            PartitionWriterResponse::MessagesPersisted { count }
        }
        Ok(PartitionWriterResponse::Error { message }) => {
            PartitionWriterResponse::Error { message }
        }
        Err(err) => PartitionWriterResponse::Error {
            message: format!("Response not received from partition writer: {}", err),
        },
    }
}

// pub async fn write_messages(
//     topic: &str,
//     partition_number: u8,
//     messages: Vec<Message>,
//     node_manager_tx: mpsc::Sender<NodeManagerCommand>,
// ) -> ProducerResponse {
//     tracing::info!("Writing {} messages for: {}", messages.len(), topic);
//     let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeManagerResponse>();
//     let command = NodeManagerCommand::GetPartitionWriter {
//         topic_name: topic.to_string(),
//         partition_number,
//         tx: oneshot_tx,
//     };
//     if let Err(err) = node_manager_tx.send(command).await {
//         return ProducerResponse::Error {
//             message: format!("Failed to send command to node manager: {}", err),
//         };
//     }
//     match oneshot_rx.await {
//         Ok(NodeManagerResponse::PartitionWriter {
//             writer,
//             partition_info,
//         }) => {
//             let (pw_oneshot_tx, pw_oneshot_rx) = oneshot::channel::<PartitionWriterResponse>();
//             let partition_command: PartitionCommand = PartitionCommand::WriteMessages {
//                 messages,
//                 tx: pw_oneshot_tx,
//             };
//             if let Err(err) = writer.send(partition_command).await {
//                 return ProducerResponse::Error {
//                     message: format!("Failed to send command to partition writer: {}", err),
//                 };
//             }
//             match pw_oneshot_rx.await {
//                 Ok(PartitionWriterResponse::MessagesPersisted { count }) => {
//                     ProducerResponse::MessagesPersisted { count }
//                 }
//                 Err(err) => ProducerResponse::Error {
//                     message: format!("Response not received from partition writer: {}", err),
//                 },
//             }
//         }
//         Ok(other) => ProducerResponse::Error {
//             message: format!("Node manager error:{:?}", other),
//         },
//         Err(err) => ProducerResponse::Error {
//             message: format!("Failed to receive response from node manager: {}", err),
//         },
//     }
// }
