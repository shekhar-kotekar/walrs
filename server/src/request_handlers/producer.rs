use common::models::{ClusterResponse, ProducerCommand};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::models::{BrokerResponse, CommandToBroker, PartitionCommand, PartitionWriterResponse};

pub async fn handle_producer_request(
    socket: &mut TcpStream,
    broker_tx: mpsc::Sender<CommandToBroker>,
) -> ClusterResponse {
    tracing::info!("Producer connected.");
    let (broker_oneshot_tx, broker_rx) = oneshot::channel::<BrokerResponse>();
    let response: ClusterResponse =
        match common::read_command_from_socket::<ProducerCommand>(socket).await {
            Some(ProducerCommand::WriteMessages {
                topic_name,
                messages,
            }) => {
                let get_partition_writer_command = CommandToBroker::GetPartitionWriter {
                    topic_name: topic_name.clone(),
                    broker_tx: broker_oneshot_tx,
                };
                broker_tx.send(get_partition_writer_command).await.unwrap();
                match broker_rx.await {
                    Ok(BrokerResponse::PartitionManagerFound { tx }) => {
                        let (partition_writer_oneshot_tx, partition_writer_rx) =
                            oneshot::channel::<PartitionWriterResponse>();
                        tracing::info!("partition writer found writer for topic: {}", topic_name);
                        let partition_writer_command = PartitionCommand::WriteMessages {
                            messages,
                            tx: partition_writer_oneshot_tx,
                        };
                        tx.send(partition_writer_command).await.unwrap();
                        match partition_writer_rx.await {
                            Ok(PartitionWriterResponse::MessagesPersisted { count }) => {
                                tracing::info!(
                                    "Successfully wrote {} messages to topic: {}",
                                    count,
                                    topic_name
                                );
                                ClusterResponse::MessagesPersisted { count }
                            }
                            Err(e) => {
                                let message = format!(
                                    "Failed to write messages for topic {}: {:?}",
                                    topic_name, e
                                );
                                tracing::error!(message);
                                ClusterResponse::Error { message }
                            }
                        }
                    }
                    Ok(BrokerResponse::BrokerError { message }) => {
                        ClusterResponse::Error { message }
                    }
                    Err(e) => ClusterResponse::Error {
                        message: format!("Error details: {:?}", e),
                    },
                    _ => ClusterResponse::Error {
                        message: format!("Unexpected error."),
                    },
                }
            }
            _ => {
                tracing::error!("Invalid command received from producer.");
                ClusterResponse::Error {
                    message: "Invalid command received".to_string(),
                }
            }
        };
    response
}
