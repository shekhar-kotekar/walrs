use common::models::{ClientCommand, ClusterResponse};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::models::{BrokerResponse, CommandToBroker};

pub async fn handle_admin_request(
    socket: &mut TcpStream,
    broker_tx: mpsc::Sender<CommandToBroker>,
) -> ClusterResponse {
    tracing::debug!(
        "Handling admin request from: {}",
        socket.peer_addr().unwrap()
    );

    let next_command = common::read_command_from_socket::<ClientCommand>(socket).await;
    let (broker_oneshot_tx, broker_rx) = oneshot::channel::<BrokerResponse>();
    match next_command {
        Some(command) => match command {
            ClientCommand::CreateTopic { topic_details } => {
                let broker_command: CommandToBroker = CommandToBroker::CreateNewTopic {
                    topic: topic_details,
                    broker_tx: broker_oneshot_tx,
                };
                broker_tx.send(broker_command).await.unwrap();
                // Wait for the broker's response
                match broker_rx.await {
                    Ok(response) => response.to_cluster_response(),
                    Err(e) => ClusterResponse::Error {
                        message: format!("Error details: {:?}", e),
                    },
                }
            }
            ClientCommand::GetStatus { topic_name } => {
                let broker_command: CommandToBroker = CommandToBroker::GetTopicStatus {
                    topic_name: topic_name.to_string(),
                    broker_tx: broker_oneshot_tx,
                };
                broker_tx.send(broker_command).await.unwrap();
                // Wait for the broker's response
                match broker_rx.await {
                    Ok(response) => response.to_cluster_response(),
                    Err(e) => ClusterResponse::Error {
                        message: format!("Error details: {:?}", e),
                    },
                }
            }
            ClientCommand::GetTopicMetadata { topics } => {
                let broker_command: CommandToBroker = CommandToBroker::GetTopicMetadata {
                    topics,
                    broker_tx: broker_oneshot_tx,
                };
                broker_tx.send(broker_command).await.unwrap();
                // Wait for the broker's response
                match broker_rx.await {
                    Ok(response) => response.to_cluster_response(),
                    Err(e) => ClusterResponse::Error {
                        message: format!("Error details: {:?}", e),
                    },
                }
            }
            _ => {
                let message = format!("Unsupported admin command: {:?}", command);
                tracing::warn!(message);
                ClusterResponse::Error { message }
            }
        },
        None => ClusterResponse::Error {
            message: "Failed to read admin command".to_string(),
        },
    }
}
