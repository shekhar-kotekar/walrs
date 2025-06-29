use commons::models::{Message, PartitionRole, PeerCommand, PeerResponse};
use tokio::sync::{mpsc, oneshot};

use crate::{
    broker::{BrokerCommand, BrokerResponse},
    models::{PartitionCommand, PartitionWriterResponse},
};

pub async fn handle_peer_request(
    command: PeerCommand,
    broker_tx: mpsc::Sender<BrokerCommand>,
) -> PeerResponse {
    match command {
        PeerCommand::CreatePartition {
            topic_name,
            partition_number,
            role,
        } => create_partition(topic_name, partition_number, role, broker_tx).await,
        PeerCommand::SyncMessages {
            topic_name,
            partition_number,
            messages,
        } => sync_messages(topic_name, partition_number, messages, broker_tx).await,
        PeerCommand::Heartbeat { broker_info } => {
            let broker_command = BrokerCommand::Heartbeat { broker_info };
            broker_tx.send(broker_command).await.unwrap();
            PeerResponse::HeartbeatAcknowledged
        }
    }
}

async fn sync_messages(
    topic_name: String,
    partition_number: u8,
    messages: Vec<Message>,
    broker_tx: mpsc::Sender<BrokerCommand>,
) -> PeerResponse {
    let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
    let broker_command = BrokerCommand::GetPartitionWriter {
        topic_name: topic_name.clone(),
        partition_number,
        tx: oneshot_tx,
    };
    if let Err(err) = broker_tx.send(broker_command).await {
        return PeerResponse::Error {
            message: format!(
                "Failed to send GetPartitionWriter command to broker: {}",
                err
            ),
        };
    }
    match oneshot_rx.await {
        Ok(response) => match response {
            BrokerResponse::PartitionWriter { writer } => {
                let (oneshot_tx, oneshot_rx) = oneshot::channel::<PartitionWriterResponse>();
                let partition_writer_command = PartitionCommand::WriteMessages {
                    messages: messages.clone(),
                    tx: oneshot_tx,
                };
                if let Err(err) = writer.send(partition_writer_command).await {
                    return PeerResponse::Error {
                        message: format!("Failed to send WriteMessages command: {}", err),
                    };
                }
                match oneshot_rx.await {
                    Ok(PartitionWriterResponse::MessagesPersisted { count }) => {
                        PeerResponse::MessagesSynced {
                            topic_name: topic_name.clone(),
                            partition_number,
                            count,
                        }
                    }
                    Ok(PartitionWriterResponse::Error { message }) => PeerResponse::Error {
                        message: format!("Failed to persist messages: {}", message),
                    },
                    Err(err) => PeerResponse::Error {
                        message: format!(
                            "Failed to receive response from partition writer: {}",
                            err
                        ),
                    },
                }
            }
            BrokerResponse::Error(err) => PeerResponse::Error {
                message: format!("Failed to get partition writer: {}", err),
            },
            other => PeerResponse::Error {
                message: format!("Unexpected response type: {:?}", other),
            },
        },
        Err(err) => PeerResponse::Error {
            message: format!("Failed to receive partition writer response: {}", err),
        },
    }
}

async fn create_partition(
    topic_name: String,
    partition_number: u8,
    role: PartitionRole,
    broker_tx: mpsc::Sender<BrokerCommand>,
) -> PeerResponse {
    let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
    let broker_command = BrokerCommand::CreatePartition {
        topic_name,
        partition_number,
        role,
        broker_response_tx: oneshot_tx,
    };
    if let Err(err) = broker_tx.send(broker_command).await {
        return PeerResponse::Error {
            message: format!("Failed to send CreatePartition command to broker: {}", err),
        };
    }
    match oneshot_rx.await {
        Ok(response) => match response {
            BrokerResponse::PartitionCreated {
                topic_name,
                partition_number,
            } => PeerResponse::PartitionCreated {
                topic_name,
                partition_number,
            },
            BrokerResponse::Error(err) => PeerResponse::Error {
                message: format!("Partition not created. Broker error: {}", err),
            },
            other => PeerResponse::Error {
                message: format!("Unexpected response type: {:?}", other),
            },
        },
        Err(err) => PeerResponse::Error {
            message: format!("Failed to receive partition creation response: {}", err),
        },
    }
}
