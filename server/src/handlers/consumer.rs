use commons::models::{ConsumerCommand, ConsumerResponse};
use tokio::sync::{mpsc, oneshot};

use crate::{
    broker::{BrokerCommand, BrokerResponse},
    models::{PartitionCommand, PartitionReaderResponse},
};

pub async fn handle_consumer_request(
    command: ConsumerCommand,
    broker_tx: mpsc::Sender<BrokerCommand>,
) -> ConsumerResponse {
    tracing::info!("Received consumer command: {:?}", command);
    match command {
        ConsumerCommand::FetchMessages {
            topic,
            partition_number,
            offset: _,
        } => {
            let (nm_oneshot_tx, nm_oneshot_rx) = oneshot::channel::<BrokerResponse>();
            let nm_command = BrokerCommand::GetPartitionReader {
                topic_name: topic.clone(),
                partition_number,
                tx: nm_oneshot_tx,
            };
            broker_tx.send(nm_command).await.unwrap_or_else(|err| {
                tracing::error!("Failed to send command to node manager: {}", err);
            });
            match nm_oneshot_rx.await {
                Ok(BrokerResponse::PartitionReader { reader }) => {
                    tracing::info!("Got partition reader for topic.");
                    let (pr_oneshot_tx, pr_oneshot_rx) =
                        oneshot::channel::<PartitionReaderResponse>();
                    let partition_reader_command: PartitionCommand =
                        PartitionCommand::FetchMessages {
                            topic_name: topic.clone(),
                            tx: pr_oneshot_tx,
                        };
                    reader
                        .send(partition_reader_command)
                        .await
                        .unwrap_or_else(|err| {
                            tracing::error!("Failed to send command to partition reader: {}", err);
                        });

                    match pr_oneshot_rx.await {
                        Ok(PartitionReaderResponse::MessagesRead { messages }) => {
                            ConsumerResponse::MessagesFetched { messages }
                        }
                        Ok(PartitionReaderResponse::InternalError { message }) => {
                            ConsumerResponse::Error(message)
                        }
                        Err(err) => ConsumerResponse::Error(format!(
                            "Failed to receive response from partition reader: {}",
                            err
                        )),
                    }
                }
                Ok(_) => ConsumerResponse::Error("Unexpected response from node manager".into()),
                Err(err) => ConsumerResponse::Error(format!(
                    "Failed to receive response from node manager: {}",
                    err
                )),
            }
        }
    }
}
