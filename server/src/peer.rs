use crate::models::{to_bytes, BrokerResponse, CommandToBroker, CommandToPeer, PeerResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::bytes::BytesMut;
use tokio_util::sync::CancellationToken;

pub async fn start_peer_listener(
    address: String,
    broker_tx: mpsc::Sender<CommandToBroker>,
    cancellation_token: CancellationToken,
) {
    let peer_listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect(format!("Failed to bind TCP listener on: {}", address).as_str());
    tracing::info!("Peer listener started on {}", address);

    tokio::select! {
        _ = async {
            loop {
                let (socket, _) = peer_listener.accept().await.unwrap();
                tracing::info!("Accepted new peer connection from: {}", socket.peer_addr().unwrap());
                let broker_tx_clone = broker_tx.clone();
                let cancellation_token_clone = cancellation_token.clone();
                tokio::spawn(async move {
                    handle_peer_request(socket, broker_tx_clone, cancellation_token_clone).await;
                });
            }
        } => {
            tracing::info!("Peer listener closed: {}", address);
        },
        _ = cancellation_token.cancelled() => {
            tracing::info!("Peer listener cancelled: {}", address);
            return;
        }
    }
    tracing::info!("Peer listener stopped: {}", address);
}

// async fn send_heartbeat_to_broker_task(
//     heartbeat_sender_address: String,
//     heartbeat: Heartbeat,
//     broker_tx: mpsc::Sender<CommandToBroker>,
// ) -> Option<BrokerResponse> {
//     let (broker_oneshot_tx, broker_rx) = oneshot::channel::<BrokerResponse>();
//     let broker_command = CommandToBroker::Heartbeat {
//         sender_address: heartbeat_sender_address,
//         message: heartbeat,
//         broker_tx: broker_oneshot_tx,
//     };
//     broker_tx.send(broker_command).await.unwrap();
//     match broker_rx.await {
//         Ok(response) => Some(response),
//         Err(e) => {
//             tracing::error!("Failed to receive response from broker: {:?}", e);
//             None
//         }
//     }
// }

async fn handle_peer_request(
    mut socket: TcpStream,
    broker_tx: mpsc::Sender<CommandToBroker>,
    cancellation_token: CancellationToken,
) {
    tokio::select! {
        _ = cancellation_token.cancelled() => {
            tracing::info!("Peer request handler cancelled.");
            return;
        }
        _ = async {
            let command = read_command_from_socket(&mut socket).await;

            let response: PeerResponse = match command {
                Some(CommandToPeer::CreatePartitionWriter { topic_name, partition_number, role }) => {
                    tracing::info!("Received create partition writer command from peer: {}", socket.peer_addr().unwrap());
                    let (broker_oneshot_tx, broker_rx) = oneshot::channel::<BrokerResponse>();
                    let broker_command = CommandToBroker::CreatePartitionWriter {
                        topic_name,
                        partition_number,
                        role,
                        broker_tx: broker_oneshot_tx,
                    };
                    broker_tx.send(broker_command).await.expect("Failed to send create partition writer command to broker");

                    match broker_rx.await {
                        Ok(BrokerResponse::PartitionWriterCreated) => PeerResponse::PartitionWriterCreated,
                        Ok(BrokerResponse::BrokerError { message }) => PeerResponse::Error {
                            message,
                        },
                        _ => PeerResponse::Error {
                            message: "Failed to create partition writer".to_string(),
                        },
                    }
                }

                None => PeerResponse::Error {
                    message: "Failed to read command from socket".to_string(),
                },
            };
            let response_bytes = to_bytes(&response);
            let response_size = response_bytes.len();

            tracing::debug!("Sending response of size {} bytes", response_size);

            socket.write_u32(response_size as u32)
                .await
                .expect("Failed to write response size to socket");

            socket
                .write_all(&response_bytes)
                .await
                .expect("Failed to write response to socket");

            socket.flush().await.expect("Failed to flush socket");

            // let heartbeat = read_heartbeat_from_socket(&mut socket).await;
            // let response: ClusterResponse = match heartbeat {
            //     Some(hb) => {
            //         tracing::info!("Received heartbeat: {:?}", hb);
            //         let heartbeat_sender_address = socket.peer_addr().unwrap().to_string();
            //         let broker_response = send_heartbeat_to_broker_task(heartbeat_sender_address, hb, broker_tx).await;
            //         match broker_response {
            //             Some(BrokerResponse::HeartbeatReceived) => ClusterResponse::Success {
            //                 message: "Heartbeat received successfully".to_string(),
            //             },
            //             _ => ClusterResponse::InternalError {
            //                 message: "Unknown broker response".to_string(),
            //             },
            //         }
            //     }
            //     None => {
            //         tracing::error!("Failed to read heartbeat from socket");
            //         ClusterResponse::InternalError {
            //             message: "Failed to read heartbeat".to_string(),
            //         }
            //     }
            // };
            // let serialized_response = bincode::serialize(&response).expect("Failed to serialize cluster response");
            // socket.write_all(&serialized_response).await.unwrap();
        } => {
            tracing::info!("Peer request handler completed successfully.");
        }
    }
}

// async fn read_heartbeat_from_socket(socket: &mut TcpStream) -> Option<Heartbeat> {
//     let mut buffer = BytesMut::with_capacity(1024);
//     let bytes_read = socket.read_buf(&mut buffer).await.ok()?;
//     tracing::debug!("Received {} bytes from socket", bytes_read);
//     Some(Heartbeat::from_bytes(&mut buffer))
// }

async fn read_command_from_socket(socket: &mut TcpStream) -> Option<CommandToPeer> {
    let mut buffer = BytesMut::with_capacity(1024);
    let bytes_read = socket.read_buf(&mut buffer).await.ok()?;
    tracing::debug!("Received {} bytes from socket", bytes_read);
    Some(crate::models::from_bytes(&mut buffer))
}
