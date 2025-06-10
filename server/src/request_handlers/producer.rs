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
                    Err(e) => ClusterResponse::Error {
                        message: format!("Error details: {:?}", e),
                    },
                }
            }
            _ => {
                tracing::error!("Invalid command received from producer");
                ClusterResponse::Error {
                    message: "Invalid command received".to_string(),
                }
            }
        };
    response
}
