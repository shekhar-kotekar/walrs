use common::models::{ClientCommand, ClusterResponse};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::{io::AsyncReadExt, sync::oneshot};

use crate::models::{BrokerResponse, CommandToBroker};

pub async fn read_client_command(socket: &mut TcpStream) -> Option<ClientCommand> {
    let mut buffer = [0u8; 1024];
    let bytes_read = socket.read(&mut buffer).await.ok()?;
    let client_command: ClientCommand = bincode::deserialize(&buffer[..bytes_read]).ok()?;
    Some(client_command)
}

pub async fn handle_get_topic_metadata_request(
    topic_name: &str,
    broker_tx: mpsc::Sender<CommandToBroker>,
) -> ClusterResponse {
    tracing::info!("Client requested metadata for topic: {}", topic_name);
    let (broker_oneshot_tx, broker_oneshot_rx) = oneshot::channel::<BrokerResponse>();
    let command_to_broker = CommandToBroker::GetTopicMetadata {
        topic_name: topic_name.to_string(),
        broker_tx: broker_oneshot_tx,
    };
    broker_tx.send(command_to_broker).await.unwrap();
    match broker_oneshot_rx.await {
        Ok(response) => match response {
            BrokerResponse::TopicNotFound => ClusterResponse::TopicNotFound,
            _ => ClusterResponse::Error {
                message: "Unexpected response from broker".to_string(),
            },
        },
        Err(e) => {
            tracing::error!("Error receiving response from broker: {:?}", e);
            ClusterResponse::Error {
                message: "Failed to get topic metadata".to_string(),
            }
        }
    }
}
