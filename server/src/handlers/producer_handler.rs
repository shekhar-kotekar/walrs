use common::message_batch::{MessageBatch, MessageBatchCodec};
use common::models::{ClusterResponse, Message};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::bytes::BytesMut;
use tokio_util::codec::Decoder;

use crate::models::{BrokerCommand, BrokerResponse, PartitionCommand, PartitionResponse};

pub async fn handle_producer_request(
    socket: &mut TcpStream,
    broker_tx: mpsc::Sender<BrokerCommand>,
) -> ClusterResponse {
    tracing::info!("Producer connected.");
    let (broker_oneshot_tx, broker_rx) = oneshot::channel::<BrokerResponse>();
    let mut message_batch_codec = MessageBatchCodec;

    match fetch_next_message_batch(socket, &mut message_batch_codec).await {
        Some(message_batch) => {
            let message_count = message_batch.messages.len();
            tracing::info!("Received message batch of messages: {:?}", message_count);
            let find_partition_manager_command: BrokerCommand = BrokerCommand::GetPartitionManager {
                topic_name: message_batch.topic_name,
                broker_tx: broker_oneshot_tx,
            };
            broker_tx.send(find_partition_manager_command).await.unwrap();
            match broker_rx.await {
                Ok(response) => match response {
                    BrokerResponse::PartitionManagerFound { tx } => {
                        tracing::info!("Partition manager found.");
                        return send_messages_to_partition_manager(message_batch.messages, tx).await;
                    }
                    _ => response.to_cluster_response(),
                },
                Err(e) => ClusterResponse::InternalError {
                    message: format!("Error details: {:?}", e),
                },
            }
        }
        None => ClusterResponse::InternalError {
            message: "Failed to decode message batch".to_string(),
        },
    }
}

async fn send_messages_to_partition_manager(
    messages_to_persist: Vec<Message>,
    partition_manager_sender: mpsc::Sender<PartitionCommand>,
) -> ClusterResponse {
    let (tx, rx) = oneshot::channel();

    let command = PartitionCommand::WriteMessages {
        messages: messages_to_persist,
        tx,
    };

    partition_manager_sender.send(command).await.unwrap();

    match rx.await {
        Ok(response) => match response {
            PartitionResponse::MessagesPersisted => ClusterResponse::MessagesPersisted,
        },
        Err(e) => ClusterResponse::InternalError {
            message: format!("Failed to receive partition response: {:?}", e),
        },
    }
}

async fn fetch_next_message_batch(socket: &mut TcpStream, codec: &mut MessageBatchCodec) -> Option<MessageBatch> {
    let mut buf = BytesMut::new();
    socket.readable().await.unwrap();
    socket.try_read_buf(&mut buf).unwrap();

    codec.decode(&mut buf).unwrap()
}
