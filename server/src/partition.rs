use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::models::{PartitionCommand, PartitionResponse};

pub struct Partition {
    topic: String,
}

impl Partition {
    pub fn new(topic: String) -> Self {
        Partition { topic }
    }

    pub async fn start(
        &mut self,
        mut main_rx: mpsc::Receiver<PartitionCommand>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!("Starting partition manager for topic {}", self.topic);
        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        PartitionCommand::WriteMessages { messages, tx } => {
                            for message in messages {
                                tracing::info!("Writing message to partition {}: {:?}", self.topic, message);
                            }
                            let _ = tx.send(PartitionResponse::MessagesPersisted);
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token called. Partition manager for topic {} shutting down...", self.topic);
                    break;
                }
            }
        }
        tracing::info!("Partition manager for topic {} stopped.", self.topic);
    }
}
