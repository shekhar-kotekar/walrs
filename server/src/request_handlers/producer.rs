use common::models::{ClusterResponse, ProducerCommand};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::models::{BrokerResponse, CommandToBroker};

pub async fn handle_producer_request(
    socket: &mut TcpStream,
    broker_tx: mpsc::Sender<CommandToBroker>,
) -> ClusterResponse {
    tracing::info!("Producer connected.");
    let (broker_oneshot_tx, broker_rx) = oneshot::channel::<BrokerResponse>();
    let response: ClusterResponse =
        match common::read_command_from_socket::<ProducerCommand>(socket).await {
            Some(ProducerCommand::GetPartitionLeaders { topics }) => {
                tracing::info!(
                    "Received request for partition leaders for topics: {:?}",
                    topics
                );
                let broker_command = CommandToBroker::GetPartitionLeaders {
                    topics,
                    broker_tx: broker_oneshot_tx,
                };
                broker_tx.send(broker_command).await.unwrap();
                match broker_rx.await {
                    Ok(response) => response.to_cluster_response(),
                    Err(e) => ClusterResponse::InternalError {
                        message: format!("Error details: {:?}", e),
                    },
                }
            }
            _ => {
                tracing::error!("Invalid command received from producer");
                ClusterResponse::InternalError {
                    message: "Invalid command received".to_string(),
                }
            }
        };
    response
    // let mut message_batch_codec = MessageBatchCodec;

    // match read_messages_from_socket(socket, &mut message_batch_codec).await {
    //     Some(message_batch) => {
    //         let message_count = message_batch.messages.len();
    //         tracing::info!("Received {} messages for {}", message_count, message_batch.topic_name);
    //         let find_partition_manager_command: CommandToBroker = CommandToBroker::GetPartitionWriter {
    //             topic_name: message_batch.topic_name,
    //             broker_tx: broker_oneshot_tx,
    //         };
    //         broker_tx.send(find_partition_manager_command).await.unwrap();
    //         match broker_rx.await {
    //             Ok(response) => match response {
    //                 BrokerResponse::PartitionManagerFound { tx } => {
    //                     send_messages_to_partition_writer(message_batch.messages, tx).await
    //                 }
    //                 _ => response.to_cluster_response(),
    //             },
    //             Err(e) => ClusterResponse::InternalError {
    //                 message: format!("Error details: {:?}", e),
    //             },
    //         }
    //     }
    //     None => {
    //         tracing::error!("Failed to decode message batch from socket");
    //         ClusterResponse::InternalError {
    //             message: "Failed to decode message batch".to_string(),
    //         }
    //     }
    // }
}

// async fn send_messages_to_partition_writer(
//     messages_to_persist: Vec<Message>,
//     partition_manager_sender: mpsc::Sender<PartitionCommand>,
// ) -> ClusterResponse {
//     let (tx, rx) = oneshot::channel();

//     let command = PartitionCommand::WriteMessages {
//         messages: messages_to_persist,
//         tx,
//     };

//     partition_manager_sender.send(command).await.unwrap();

//     match rx.await {
//         Ok(response) => match response {
//             PartitionWriterResponse::MessagesPersisted { count } => ClusterResponse::MessagesPersisted { count },
//         },
//         Err(e) => ClusterResponse::InternalError {
//             message: format!("Failed to receive partition response: {:?}", e),
//         },
//     }
// }

// async fn read_messages_from_socket(socket: &mut TcpStream, codec: &mut MessageBatchCodec) -> Option<MessageBatch> {
//     let mut buffer = BytesMut::with_capacity(1024);
//     let bytes_read = socket.read_buf(&mut buffer).await.ok()?;
//     tracing::info!("Received {} bytes from socket", bytes_read);
//     codec.decode(&mut buffer).unwrap_or_else(|e| {
//         tracing::error!("Couldn't decode message batch: {:?}", e);
//         None
//     })
// }
