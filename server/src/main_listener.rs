use commons::models::{AdminCommand, AdminResponse, WalrsCommand, WalrsResponse};
use tokio::{
    net::{TcpListener, TcpStream},
    signal::{
        self,
        unix::{signal, Signal, SignalKind},
    },
    sync::{mpsc, oneshot},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    broker::{BrokerCommand, BrokerResponse},
    handlers::{consumer, peer, producer},
    metrics::server::MetricEvent,
};

pub struct MainListener {
    pub address: String,
    pub broker_tx: mpsc::Sender<BrokerCommand>,
    pub metrics_tx: mpsc::Sender<MetricEvent>,
}

impl MainListener {
    pub async fn start(&self, task_tracker: TaskTracker, cancellation_token: CancellationToken) {
        tracing::info!("Starting main listener on: {}", self.address);
        // let main_tcp_listener = TcpListener::bind(&self.address).await.unwrap();
        let main_tcp_listener = TcpListener::bind("0.0.0.0:5056").await.unwrap();
        tracing::debug!("Listening on: {}", main_tcp_listener.local_addr().unwrap());

        let mut sigterm: Signal =
            signal(SignalKind::terminate()).expect("Failed to create signal handler");

        tokio::select! {
            _ = async {
                loop {
                    let (stream, _) = main_tcp_listener.accept().await.unwrap();
                    let client_request_cancellation_token = cancellation_token.child_token();
                    let broker_tx_clone = self.broker_tx.clone();
                    let self_address = self.address.clone();
                    let metrics_tx_clone = self.metrics_tx.clone();
                    task_tracker.spawn(async move {
                        MainListener::process_request(stream, self_address, broker_tx_clone, metrics_tx_clone, client_request_cancellation_token).await;
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
                task_tracker.close();
                task_tracker.wait().await;
                tracing::info!("Task tracker wait is over. All tasks cancelled.");
            }
            _ = self.terminate_signal() => {
                tracing::info!("Received termination signal, shutting down server...");
                cancellation_token.cancel();
                task_tracker.close();
                task_tracker.wait().await;
                tracing::info!("Task tracker wait is over. All tasks cancelled.");
            }
        }
    }

    async fn terminate_signal(&self) -> Result<(), std::io::Error> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            signal(SignalKind::terminate())?.recv().await;
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    async fn process_request(
        mut stream: TcpStream,
        self_address: String,
        broker_tx: mpsc::Sender<BrokerCommand>,
        metrics_tx: mpsc::Sender<MetricEvent>,
        cancellation_token: CancellationToken,
    ) {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Cancellation token cancelled. Stopped processing client request.");
            }
            _ = async {
                match commons::read_from_socket::<WalrsCommand>(&mut stream).await {
                    Ok(command) => {
                        let response: WalrsResponse = match command {
                            WalrsCommand::Admin(admin_command) =>{
                                WalrsResponse::Admin(MainListener::handle_admin_request(admin_command, broker_tx).await)
                            }
                            WalrsCommand::Peer(peer_command) => {
                                WalrsResponse::Peer(peer::handle_peer_request(peer_command, broker_tx).await)
                            }
                            WalrsCommand::Producer(producer_command) => {
                                WalrsResponse::Producer(producer::handle_producer_request(producer_command, &self_address, broker_tx, metrics_tx).await)
                            }
                            WalrsCommand::Consumer(consumer_command) => {
                                WalrsResponse::Consumer(consumer::handle_consumer_request(consumer_command, broker_tx).await)
                            }
                        };
                        tracing::debug!("Sending response: {:?}", response);
                        commons::write_to_socket::<WalrsResponse>(&response, &mut stream).await.unwrap_or_else(|err| {
                            tracing::error!("Failed to write response to socket: {}", err);
                        });
                    }
                    Err(err) => tracing::error!("Failed to read command from socket: {}", err)

                }
            } => {
                tracing::debug!("Client request processing completed.");
            }
        }
    }

    async fn handle_admin_request(
        command: AdminCommand,
        broker_tx: mpsc::Sender<BrokerCommand>,
    ) -> AdminResponse {
        match command {
            AdminCommand::CreateTopic {
                name,
                num_partitions,
                replication_factor,
                retention_period_minutes,
                ack_level,
            } => {
                let (oneshot_tx, oneshot_rx) = oneshot::channel::<BrokerResponse>();
                let create_topic_command = BrokerCommand::CreateTopic {
                    name,
                    num_partitions,
                    replication_factor,
                    retention_period_minutes,
                    ack_level,
                    broker_response_tx: oneshot_tx,
                };
                broker_tx
                    .send(create_topic_command)
                    .await
                    .unwrap_or_else(|err| {
                        tracing::error!("Failed to send CreateTopic command to broker: {}", err);
                    });
                tracing::debug!("Waiting for response from broker for create topic command...");
                match oneshot_rx.await {
                    Ok(response) => match response {
                        BrokerResponse::TopicCreationInProgress(_topic) => {
                            AdminResponse::RequestAccepted
                        }
                        BrokerResponse::TopicInfo { topics } => AdminResponse::TopicInfo { topics },
                        BrokerResponse::Error(err) => AdminResponse::Error(err),
                        other => AdminResponse::Error(format!("Unexpected response: {:?}", other)),
                    },
                    Err(err) => AdminResponse::Error(err.to_string()),
                }
            }
            AdminCommand::GetTopicInfo { topic_names } => {
                let (tx, rx) = oneshot::channel::<BrokerResponse>();
                let command = BrokerCommand::GetTopicInfo {
                    topic_names,
                    broker_response_tx: tx,
                };
                broker_tx.send(command).await.unwrap_or_else(|err| {
                    tracing::error!("Failed to send GetTopicInfo command to broker: {}", err);
                });
                match rx.await {
                    Ok(response) => match response {
                        BrokerResponse::TopicInfo { topics } => AdminResponse::TopicInfo { topics },
                        other => AdminResponse::Error(format!("Unexpected response: {:?}", other)),
                    },
                    Err(err) => {
                        AdminResponse::Error(format!("topic info response not received: {}", err))
                    }
                }
            }
        }
    }
}
