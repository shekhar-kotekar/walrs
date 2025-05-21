use common::consumer::ConsumerResponse;
use tokio::sync::{mpsc, oneshot};

use crate::models::{BrokerCommand, BrokerResponse, PartitionCommand, PartitionReaderResponse};

pub async fn handle_consumer_request(topic_name: &String, broker_tx: mpsc::Sender<BrokerCommand>) -> ConsumerResponse {
    tracing::info!("Consumer client connected.");

    let (broker_oneshot_tx, broker_rx) = oneshot::channel::<BrokerResponse>();

    let find_partition_reader_command: BrokerCommand = BrokerCommand::GetPartitionReader {
        topic_name: topic_name.to_string(),
        broker_tx: broker_oneshot_tx,
    };
    broker_tx.send(find_partition_reader_command).await.unwrap();

    match broker_rx.await {
        Ok(response) => match response {
            BrokerResponse::PartitionManagerFound { tx } => {
                let (partition_tx, partition_rx) = oneshot::channel::<PartitionReaderResponse>();
                let partition_reader_command = PartitionCommand::ReadMessages {
                    topic_name: topic_name.to_string(),
                    tx: partition_tx,
                };
                tx.send(partition_reader_command).await.unwrap();
                match partition_rx.await {
                    Ok(response) => match response {
                        PartitionReaderResponse::MessagesRead { messages } => {
                            tracing::debug!("Fetched {} messages from partition of {}", messages.len(), topic_name);
                            ConsumerResponse::MessagesFetched { messages: messages }
                        }
                        PartitionReaderResponse::InternalError { message } => {
                            ConsumerResponse::InternalError { message }
                        }
                    },
                    Err(e) => ConsumerResponse::InternalError {
                        message: format!("Error receiving response from partition reader: {:?}", e),
                    },
                }
            }
            _ => ConsumerResponse::InternalError {
                message: "Unexpected response from broker".to_string(),
            },
        },
        Err(e) => ConsumerResponse::InternalError {
            message: format!("Error details: {:?}", e),
        },
    }
}
