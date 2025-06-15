use commons::models::{
    ConsumerCommand, ConsumerResponse, PeerCommand, PeerResponse, ProducerCommand,
    ProducerResponse, WalrsCommand, WalrsResponse,
};
use tokio::{
    net::{TcpListener, TcpStream},
    signal::{
        self,
        unix::{Signal, SignalKind, signal},
    },
    sync::{mpsc, oneshot},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    handlers::admin,
    models::{PartitionCommand, PartitionReaderResponse, PartitionWriterResponse},
    node_manager::{NodeManagerCommand, NodeManagerResponse},
};

pub struct MainListener {
    pub address: String,
    pub node_manager_tx: mpsc::Sender<NodeManagerCommand>,
}

impl MainListener {
    pub async fn start(&self, task_tracker: TaskTracker, cancellation_token: CancellationToken) {
        let main_tcp_listener = TcpListener::bind(&self.address).await.unwrap();
        tracing::debug!("Listening on: {}", main_tcp_listener.local_addr().unwrap());

        let mut sigterm: Signal =
            signal(SignalKind::terminate()).expect("Failed to create signal handler");

        tokio::select! {
            _ = async {
                loop {
                    let (stream, _) = main_tcp_listener.accept().await.unwrap();
                    tracing::info!("connection accepted from: {:?}", stream.peer_addr());
                    let client_request_cancellation_token = cancellation_token.child_token();
                    let node_manager_tx_clone = self.node_manager_tx.clone();
                    task_tracker.spawn(async move {
                        MainListener::process_request(stream, node_manager_tx_clone, client_request_cancellation_token).await;
                    });
                }
            } => {
                tracing::info!("Main listener closed.");
            }
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM signal. Cancelling all tasks.");
                cancellation_token.cancel();
                task_tracker.close();
                task_tracker.wait().await;
                tracing::info!("All tasks cancelled.");
            }
            _ = signal::ctrl_c() => {
                tracing::info!("Received Ctrl-C signal. Cancelling all tasks.");

                cancellation_token.cancel();
                tracing::info!("Cancellation token cancelled.");

                task_tracker.close();
                tracing::info!("Task tracker closed.");

                task_tracker.wait().await;
                tracing::info!("Task tracker wait is over. All tasks cancelled.");
            }
        }
    }

    async fn process_request(
        mut stream: TcpStream,
        node_manager_tx: mpsc::Sender<NodeManagerCommand>,
        cancellation_token: CancellationToken,
    ) {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Cancellation token cancelled. Stopped processing client request.");
            }
            _ = async {
                tracing::debug!("Processing client request...");
                match commons::read_from_socket::<WalrsCommand>(&mut stream).await {
                    Ok(command) => {
                        tracing::info!("Received command: {:?}", command);
                        let response: WalrsResponse = match command {
                            WalrsCommand::Admin(admin_command) =>{
                                WalrsResponse::Admin(admin::handle_admin_request(admin_command, node_manager_tx).await)
                            }
                            WalrsCommand::Producer(producer_command) => {
                                WalrsResponse::Producer(handle_producer_request(producer_command, node_manager_tx).await)
                            }
                            WalrsCommand::Consumer(consumer_command) => {
                                WalrsResponse::Consumer(handle_consumer_request(consumer_command, node_manager_tx).await)
                            }
                            WalrsCommand::Peer(peer_command) => {
                                WalrsResponse::Peer(handle_peer_request(peer_command, node_manager_tx).await)
                            }
                        };
                        commons::write_to_socket::<WalrsResponse>(&response, &mut stream).await.unwrap_or_else(|err| {
                            tracing::error!("Failed to write response to socket: {}", err);
                        });
                    }
                    Err(err) => {
                        tracing::error!("Failed to read command from socket: {}", err);
                    }
                }
            } => {
                tracing::info!("Client request processing completed.");
            }
        }
    }
}

async fn handle_peer_request(
    command: PeerCommand,
    node_manager_tx: mpsc::Sender<NodeManagerCommand>,
) -> PeerResponse {
    match command {
        PeerCommand::Heartbeat { node_info } => {
            let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeManagerResponse>();
            let node_manager_command = NodeManagerCommand::Heartbeat {
                peer_info: node_info.clone(),
                tx: oneshot_tx,
            };

            if let Err(err) = node_manager_tx.send(node_manager_command).await {
                return PeerResponse::Error {
                    message: format!("Failed to send heartbeat command to node manager: {}", err),
                };
            }

            match oneshot_rx.await {
                Ok(NodeManagerResponse::HeartbeatAcknowledged) => {
                    PeerResponse::HeartbeatAcknoweledged
                }
                Ok(_) => PeerResponse::Error {
                    message: String::from("Unexpected response from node manager"),
                },
                Err(err) => PeerResponse::Error {
                    message: format!("Failed to receive response from node manager: {}", err),
                },
            }
        }
        _ => PeerResponse::Error {
            message: String::from("Unsupported peer command"),
        },
    }
}

async fn handle_consumer_request(
    command: ConsumerCommand,
    node_manager_tx: mpsc::Sender<NodeManagerCommand>,
) -> ConsumerResponse {
    tracing::info!("Received consumer command: {:?}", command);
    match command {
        ConsumerCommand::FetchMessages { topic, offset: _ } => {
            tracing::info!("Fetching messages from topic: {}", topic);

            let (nm_oneshot_tx, nm_oneshot_rx) = oneshot::channel::<NodeManagerResponse>();
            let nm_command = NodeManagerCommand::GetPartitionReader {
                topic_name: topic.clone(),
                tx: nm_oneshot_tx,
            };
            node_manager_tx
                .send(nm_command)
                .await
                .unwrap_or_else(|err| {
                    tracing::error!("Failed to send command to node manager: {}", err);
                });
            match nm_oneshot_rx.await {
                Ok(NodeManagerResponse::PartitionReader { reader }) => {
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

async fn handle_producer_request(
    command: ProducerCommand,
    node_manager_tx: mpsc::Sender<NodeManagerCommand>,
) -> ProducerResponse {
    tracing::info!("Received producer command: {:?}", command);
    match command {
        ProducerCommand::WriteMessages { topic, messages } => {
            tracing::info!("Writing {} messages to topic: {}", messages.len(), topic);
            // get partition writer for the topic from node manager
            // send messages to the partition writer
            let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeManagerResponse>();
            let command = NodeManagerCommand::GetPartitionWriter {
                topic_name: topic,
                tx: oneshot_tx,
            };
            if let Err(err) = node_manager_tx.send(command).await {
                return ProducerResponse::Error(format!(
                    "Failed to send command to node manager: {}",
                    err
                ));
            }
            match oneshot_rx.await {
                Ok(NodeManagerResponse::PartitionWriter { writer }) => {
                    tracing::info!("Got partition writer for topic.");
                    let (pw_oneshot_tx, pw_oneshot_rx) =
                        oneshot::channel::<PartitionWriterResponse>();
                    let partition_command: PartitionCommand = PartitionCommand::WriteMessages {
                        messages,
                        tx: pw_oneshot_tx,
                    };
                    if let Err(err) = writer.send(partition_command).await {
                        return ProducerResponse::Error(format!(
                            "Failed to send command to partition writer: {}",
                            err
                        ));
                    }
                    match pw_oneshot_rx.await {
                        Ok(PartitionWriterResponse::MessagesPersisted { count }) => {
                            ProducerResponse::MessagesPersisted { count }
                        }
                        Err(err) => ProducerResponse::Error(format!(
                            "Failed to receive response from partition writer: {}",
                            err
                        )),
                    }
                }
                Ok(_) => ProducerResponse::Error("Unexpected response from node manager".into()),
                Err(err) => ProducerResponse::Error(format!(
                    "Failed to receive response from node manager: {}",
                    err
                )),
            }
        }
    }
}

#[cfg(test)]
mod should {
    use commons::models::{AckLevel, AdminCommand, AdminResponse, Topic, WalrsCommand};
    use tokio::{io::AsyncWriteExt, net::TcpStream};
    use tracing_test::traced_test;

    use super::*;

    #[tokio::test]
    // #[ignore]
    #[traced_test]
    async fn return_accepted_response_when_a_command_is_sent() {
        let address: String = "127.0.0.1:8080".into();
        let (node_manager_tx, _) = mpsc::channel::<NodeManagerCommand>(2);
        let node: MainListener = MainListener {
            address: address.clone(),
            node_manager_tx,
        };

        let task_tracker = TaskTracker::new();
        let cancellation_token: CancellationToken = CancellationToken::new();
        let node_cancellation_token = cancellation_token.child_token();
        tokio::spawn(async move {
            node.start(task_tracker, node_cancellation_token).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut sender_stream = TcpStream::connect(address).await.unwrap();

        let topic: Topic = Topic::new(
            "test_topic".into(),
            None,
            None,
            Some(60000),
            Some(AckLevel::Leader),
        )
        .unwrap();

        let create_topic_command = WalrsCommand::Admin(AdminCommand::CreateTopic {
            topic: topic.clone(),
        });
        let serialized_command =
            bincode::encode_to_vec(&create_topic_command, bincode::config::standard()).unwrap();
        sender_stream.write_all(&serialized_command).await.unwrap();

        let response_from_node: WalrsResponse =
            commons::read_from_socket::<WalrsResponse>(&mut sender_stream)
                .await
                .unwrap();

        assert_eq!(
            response_from_node,
            WalrsResponse::Admin(AdminResponse::RequestAccepted)
        );

        cancellation_token.cancel();
    }
}
