use common::models::{ClientCommand, ClusterResponse};
use tokio::net::TcpStream;

use crate::models::{BrokerResponse, CommandToBroker};
use crate::request_handlers::commons;

pub async fn handle_admin_request(
    socket: &mut TcpStream,
    broker_tx: tokio::sync::mpsc::Sender<CommandToBroker>,
) -> ClusterResponse {
    tracing::debug!(
        "Handling admin request from: {}",
        socket.peer_addr().unwrap()
    );

    let next_command = commons::read_client_command(socket).await;
    match next_command {
        Some(command) => match command {
            ClientCommand::CreateTopic { topic_details } => {
                let (broker_oneshot_tx, broker_rx) =
                    tokio::sync::oneshot::channel::<BrokerResponse>();
                let broker_command: CommandToBroker = CommandToBroker::CreateNewTopic {
                    topic: topic_details,
                    broker_tx: broker_oneshot_tx,
                };
                broker_tx.send(broker_command).await.unwrap();
                // Wait for the broker's response
                match broker_rx.await {
                    Ok(response) => response.to_cluster_response(),
                    Err(e) => ClusterResponse::InternalError {
                        message: format!("Error details: {:?}", e),
                    },
                }
            }
            _ => ClusterResponse::InternalError {
                message: "Unknown admin command".to_string(),
            },
        },
        None => ClusterResponse::InternalError {
            message: "Failed to read admin command".to_string(),
        },
    }
}
