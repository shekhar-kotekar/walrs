use std::io::{Error, ErrorKind};

use commons::models::{
    Message, PartitionInfo, PeerCommand, PeerResponse, ProducerCommand, ProducerResponse, Topic,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use crate::{
    broker::{BrokerCommand, BrokerResponse},
    metrics::server::MetricEvent,
    models::{PartitionCommand, PartitionWriterResponse},
    TASK_TIMEOUT_SECONDS,
};

pub async fn handle_producer_request(
    command: ProducerCommand,
    node_address: &str,
    broker_tx: mpsc::Sender<BrokerCommand>,
    metrics_tx: mpsc::Sender<MetricEvent>,
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
                match get_info_from_broker(&topic, partition_number, broker_tx).await {
                    Ok((writer, topic_info)) => (writer, topic_info),
                    Err(err) => {
                        return ProducerResponse::Error {
                            message: err.to_string(),
                        };
                    }
                };
            tracing::debug!(
                "Information retrieved from broker: topic: {}, partition: {}",
                topic,
                partition_number
            );

            if topic_info.partitions.len() <= partition_number as usize {
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
            let message_count = messages.len();
            match write_messages_to_local(&writer, messages.clone()).await {
                PartitionWriterResponse::MessagesPersisted { count } => {
                    tracing::info!(
                        "Persisted {} messages in local for topic: {}, partition: {}",
                        count,
                        topic,
                        partition_number
                    );
                    // Now send messages to followers

                    let mut task_join_set: JoinSet<PeerResponse> = JoinSet::new();

                    partition_info.followers.iter().for_each(|follower| {
                        let follower_address = follower.clone();
                        let topic = topic.clone();
                        let messages = messages.clone();
                        task_join_set.spawn(async move {
                            tracing::debug!("Starting a sync to follower: {}", follower_address);
                            let timeout_duration =
                                std::time::Duration::from_secs(TASK_TIMEOUT_SECONDS);
                            let result = tokio::time::timeout(
                                timeout_duration,
                                send_messages_to_follower(
                                    follower_address.clone(),
                                    topic,
                                    partition_number,
                                    messages,
                                ),
                            )
                            .await;

                            match result {
                                Ok(response) => response,
                                Err(e) => PeerResponse::Error {
                                    message: format!(
                                        "Error while syncing messages to follower: {}: {}",
                                        follower_address, e
                                    ),
                                },
                            }
                        });
                    });

                    let mut synced_peer_count = 0;
                    let timeout_duration = std::time::Duration::from_secs(TASK_TIMEOUT_SECONDS);
                    let _ = tokio::time::timeout(timeout_duration, async {
                        let task_result = task_join_set.join_all().await;
                        tracing::debug!("Follower sync tasks completed: {:?}", task_result);
                        for result in task_result {
                            match result {
                                PeerResponse::MessagesSynced {
                                    topic_name: _,
                                    partition_number: _,
                                    count,
                                } => {
                                    if count == message_count as u8 {
                                        synced_peer_count += 1;
                                    } else {
                                        tracing::warn!(
                                            "Follower did not sync all messages. Expected: {}, Synced: {}",
                                            message_count,
                                            count
                                        );
                                    }
                                }
                                other => tracing::error!("Follower sent invalid response: {:?}", other)
                            }
                        }
                    })
                    .await;
                    if synced_peer_count == partition_info.followers.len() {
                        send_metrics(metrics_tx, topic, partition_number, message_count).await;
                        ProducerResponse::MessagesPersisted { count }
                    } else {
                        ProducerResponse::Error {
                            message: format!(
                                "Failed to sync all followers. Synced to {} out of {} followers.",
                                synced_peer_count,
                                partition_info.followers.len()
                            ),
                        }
                    }
                }
                PartitionWriterResponse::Error { message } => ProducerResponse::Error {
                    message: format!("Failed to write messages: {}", message),
                },
            }
        }
    }
}

async fn send_metrics(
    metrics_tx: mpsc::Sender<MetricEvent>,
    topic: String,
    partition: u8,
    message_count: usize,
) {
    let topic_name = topic.clone();
    match tokio::spawn(async move {
        let metrics: MetricEvent = MetricEvent::MessagesWritten {
            topic: topic.clone(),
            partition,
            message_count,
        };
        metrics_tx.send(metrics).await.unwrap_or_else(|err| {
            tracing::error!("Failed to send metrics for topic '{}': {}", topic, err);
        });
    })
    .await
    {
        Ok(_) => tracing::debug!(
            "Metrics sent for topic: {}, partition: {}, message_count: {}",
            topic_name,
            partition,
            message_count
        ),
        Err(err) => tracing::error!("Failed to send metrics for topic '{}': {}", topic_name, err),
    }
}

async fn send_messages_to_follower(
    follower_address: String,
    topic_name: String,
    partition_number: u8,
    messages: Vec<Message>,
) -> PeerResponse {
    let peer_command = PeerCommand::SyncMessages {
        topic_name: topic_name.clone(),
        partition_number,
        messages,
    };
    tracing::debug!(
        "Asking follower {} to sync messages. topic: {}, partition: {}",
        follower_address,
        topic_name,
        partition_number
    );
    commons::send_and_receive_peer_command(peer_command, &follower_address).await
}

async fn get_info_from_broker(
    topic: &str,
    partition_number: u8,
    node_manager_tx: mpsc::Sender<BrokerCommand>,
) -> Result<(mpsc::Sender<PartitionCommand>, Topic), std::io::Error> {
    let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
    let command = BrokerCommand::GetPartitionWriter {
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
        Ok(BrokerResponse::PartitionWriter { writer }) => {
            let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
            let get_topic_info_command = BrokerCommand::GetTopicInfo {
                topic_names: vec![topic.to_string()],
                broker_response_tx: oneshot_tx,
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
                Ok(BrokerResponse::TopicInfo { topics }) => {
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
                    format!("Broker error: {:?}", other),
                )),
                Err(err) => Err(Error::new(
                    ErrorKind::Other,
                    format!("Failed to receive response from broker: {}", err),
                )),
            }
        }
        Ok(other) => Err(Error::new(
            ErrorKind::Other,
            format!("Broker error: {:?}", other),
        )),
        Err(err) => Err(Error::new(
            ErrorKind::Other,
            format!("Failed to receive response from broker: {}", err),
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
