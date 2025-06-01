use std::collections::HashMap;

use broker::Broker;
use common::models::{ClientCommand, ClientType, ClusterResponse};
use models::{BrokerConfig, BrokerConfigBuilder, BrokerInfo, ClusterInfo, CommandToBroker};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    signal::{
        self,
        unix::{signal, Signal, SignalKind},
    },
    sync::mpsc,
};

use request_handlers::{
    admin::handle_admin_request, commons::read_client_command, consumer::handle_consumer_request,
    producer::handle_producer_request,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing_subscriber::prelude::*;

mod broker;
mod models;
mod partition_managers;
mod peer;
mod request_handlers;

// TODO: Read all the constants from a config file
const MPSC_MAX_Q_SIZE: usize = 100;
// const K8S_SERVICE_NAME: &str = "walrs-headless-service.walrs.svc.cluster.local";
// const BASE_PATH_FOR_DATA: &str = "/tmp/walrs/data";

// Topic names are case sensitive.
#[tokio::main]
async fn main() {
    // #[cfg(all(debug_assertions, feature = "console"))]
    // {
    //     console_subscriber::init();
    //     tracing::warn!("Console subscriber initialized.");
    // }
    // init_tracing_with_console();
    common::init_tracing();
    let pod_ip: String = std::env::var("POD_IP").expect("POD_IP environment variable not set.");

    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let broker_config: BrokerConfig = get_broker_config(&pod_ip);
    let broker_address = format!("{}:{}", &pod_ip, broker_config.port);
    let broker_cancellation_token = cancellation_token.child_token();

    let cluster_peers: HashMap<String, BrokerInfo> = broker_config
        .peers
        .iter()
        .map(|peer| {
            (
                peer.clone(),
                BrokerInfo {
                    address: peer.clone(),
                    partition_leaders: Vec::new(),
                },
            )
        })
        .collect();

    let cluster_info = ClusterInfo {
        brokers: cluster_peers,
        topics_in_cluster: vec![],
    };
    let mut broker = Broker::new(
        &pod_ip,
        broker_config.port,
        broker_config.peer_listener_port,
        &broker_config.base_path_for_data,
        broker_config.heartbeat_interval_ms,
        broker_cancellation_token,
        cluster_info,
    );

    let (main_tx, main_rx) = mpsc::channel::<CommandToBroker>(MPSC_MAX_Q_SIZE);
    task_tracker.spawn(async move { broker.start(main_rx).await });

    let peer_listener_cancellation_token = cancellation_token.child_token();
    let broker_tx = main_tx.clone();
    let peer_listener_address = format!("{}:{}", &pod_ip, &broker_config.peer_listener_port);
    task_tracker.spawn(async move {
        peer::start_peer_listener(peer_listener_address, broker_tx, peer_listener_cancellation_token).await
    });

    let main_tcp_listener = TcpListener::bind(broker_address).await.unwrap();
    tracing::debug!("Listening on: {}", main_tcp_listener.local_addr().unwrap());

    let mut sigterm: Signal = signal(SignalKind::terminate()).expect("Failed to create signal handler");

    tokio::select! {
        _ = async {
            loop {
                let (socket, _) = main_tcp_listener.accept().await.unwrap();
                let main_tx_clone = main_tx.clone();
                let client_request_cancellation_token = cancellation_token.child_token();
                task_tracker.spawn(async move {
                    process_client_request(socket, main_tx_clone, client_request_cancellation_token).await;
                });
            }
        } => {
            tracing::info!("Main listener closed.");
        },
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
    tracing::info!("Exiting main.");
}

fn get_broker_config(pod_ip: &str) -> BrokerConfig {
    let config_file_path = std::env::var("BROKER_CONFIG_FILE").unwrap_or_else(|_| {
        tracing::warn!("BROKER_CONFIG_FILE environment variable not set. Using default config file path.");
        "./configs/broker_conf.yml".to_string()
    });

    let builder = BrokerConfigBuilder::from_yaml_file(&config_file_path)
        .expect("Failed to read broker config from YAML file")
        .ip(pod_ip.to_string());
    // .base_path_for_data(BASE_PATH_FOR_DATA.to_string())
    // .mpsc_max_queue_size(MPSC_MAX_Q_SIZE);
    builder.build()
}

async fn validate_client_request_to_connect(client_type: &ClientType, socket: &mut TcpStream) {
    let response = match client_type {
        ClientType::Producer => ClusterResponse::ConnectionAccepted,
        ClientType::Consumer { topic_name: _ } => ClusterResponse::ConnectionAccepted,
        ClientType::Admin => ClusterResponse::ConnectionAccepted,
    };
    socket.write_all(&bincode::serialize(&response).unwrap()).await.unwrap();
}

async fn process_client_request(
    mut socket: TcpStream,
    broker_tx: mpsc::Sender<CommandToBroker>,
    cancellation_token: CancellationToken,
) {
    tracing::debug!("Processing request from: {}", socket.peer_addr().unwrap());
    tokio::select! {
        _ = cancellation_token.cancelled() => {
            tracing::info!("Cancellation token called. Stopping processing request.");
            let internal_server_error = ClusterResponse::InternalError {
                message: "Request processing was cancelled".to_string(),
            };
            socket.write_all(&bincode::serialize(&internal_server_error).unwrap()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        _ = async {
            let client_command = read_client_command(&mut socket).await;
            let response = match client_command {
                Some(command) => {
                    tracing::debug!("Received client command: {:?}", command);
                    match command {
                        ClientCommand::RequestToConnect { client_type } => {
                            tracing::info!("Client requested to connect: {:?}", client_type);
                            validate_client_request_to_connect(&client_type, &mut socket).await;

                            match &client_type {
                                ClientType::Producer =>  handle_producer_request(&mut socket, broker_tx).await,
                                ClientType::Consumer { topic_name } => {
                                    let consumer_response = handle_consumer_request(topic_name, broker_tx).await;
                                    ClusterResponse::ConsumerResponse(consumer_response)
                                },
                                ClientType::Admin => handle_admin_request(&mut socket, broker_tx).await,
                            }
                        }
                        _ => {
                            tracing::warn!("Unknown client command: {:?}", command);
                            ClusterResponse::InternalError {
                                message: "Unknown client command".to_string(),
                            }
                        }
                    }
                }
                None => {
                    tracing::error!("Failed to deserialize client command");
                    ClusterResponse::InternalError {
                        message: "Failed to deserialize client command".to_string(),
                    }
                }
            };
            tracing::debug!("response from cluster: {:?}", response);
            socket.write_all(&bincode::serialize(&response).unwrap()).await.unwrap();
            tracing::debug!("Response sent to client: {}", socket.peer_addr().unwrap());
        } => {
            tracing::info!("Client request processing completed.");
        }
    }
}

fn init_tracing_with_console() {
    let console_layer = console_subscriber::spawn();

    let tracing_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_thread_ids(true)
        .with_filter(if cfg!(debug_assertions) {
            tracing_subscriber::filter::LevelFilter::DEBUG
        } else {
            tracing_subscriber::filter::LevelFilter::INFO
        });

    tracing_subscriber::registry()
        .with(console_layer)
        .with(tracing_layer)
        .init();
    tracing::info!("Console and tracing subscriber initialized.");
}

// Main will be responsible for external facing communication like producer or consumer requests.
// Internal communication will be handled by Node and partition managers.
// So we will need 2-4 sockets in total.
// Advantage of this approach is that Main thread does not need to manage all type of requests.
// #[tokio::main]
// async fn main() {
//     enable_tracing();

//     let pod_ip: String = std::env::var("POD_IP").unwrap();

//     let task_tracker = TaskTracker::new();
//     let cancellation_token = CancellationToken::new();

//     let (main_tx, main_rx) = mpsc::channel::<MainCommands>(MPSC_MAX_Q_SIZE);

//     // let local_node = Partition::new_follower(node_address);
//     // let node_cancellation_token = cancellation_token.child_token();
//     // task_tracker.spawn(async move {
//     //     local_node
//     //         .run(main_rx, sleep_interval, node_cancellation_token)
//     //         .await;
//     // });

//     let search_nodes_cancellation_token = cancellation_token.child_token();
//     let pod_ip_clone = pod_ip.clone();
//     task_tracker.spawn(async move {
//         search_nodes_in_cluster(
//             K8S_SERVICE_NAME,
//             main_tx,
//             search_nodes_cancellation_token,
//             &pod_ip_clone,
//         )
//         .await;
//     });

//     //TODO: How to get the list of peers from the cluster periodically?
//     // let peers = get_cluster_info(&pod_ip, Duration::from_secs(SLEEP_TIME_IN_SECONDS));

//     let main_tcp_listener = TcpListener::bind(format!("{}:{}", pod_ip, WALRS_PORT))
//         .await
//         .unwrap();

//     tracing::debug!("Listening on: {}", main_tcp_listener.local_addr().unwrap());

//     let mut sigterm: Signal =
//         signal(SignalKind::terminate()).expect("Failed to create signal handler");

//     tokio::select! {
//         _ = async {
//             loop {
//                 let (socket, _) = main_tcp_listener.accept().await.unwrap();
//                 task_tracker.spawn(async move {
//                     process_client_request(socket).await;
//                 });
//             }
//         } => {
//             tracing::info!("Main listener closed.");
//         },
//         _ = sigterm.recv() => {
//             tracing::info!("Received SIGTERM signal. Cancelling all tasks.");
//             cancellation_token.cancel();
//             task_tracker.close();
//             task_tracker.wait().await;
//             tracing::info!("All tasks cancelled.");
//         }
//         _ = signal::ctrl_c() => {
//             tracing::info!("Received Ctrl-C signal. Cancelling all tasks.");
//             cancellation_token.cancel();
//             task_tracker.close();
//             task_tracker.wait().await;
//             tracing::info!("All tasks cancelled.");
//         }
//     }
//     tracing::info!("Exiting main.");
// }

// async fn process_client_request(socket: TcpStream) {
//     let (reader, writer) = socket.into_split();
//     let mut buffer = [0u8; 48];
//     tracing::debug!(
//         "Processing external request from: {}",
//         reader.peer_addr().unwrap()
//     );
//     match reader.try_read(&mut buffer) {
//         Ok(0) => {
//             tracing::info!("Connection closed for: {}", reader.peer_addr().unwrap());
//         }
//         Ok(bytes_read) => {
//             let client_command: ClientCommand =
//                 bincode::deserialize(&buffer[..bytes_read]).unwrap();
//             let broker_response: BrokerResponse = match client_command {
//                 ClientCommand::CreateTopic {
//                     topic_name,
//                     num_partitions,
//                     retention_period_hours,
//                 } => {
//                     tracing::info!(
//                         "Received command to create topic '{}' with {} partitions and {} hours retention period.",
//                         topic_name,
//                         num_partitions,
//                         retention_period_hours
//                     );
//                     BrokerResponse::InternalError {
//                         message: "Not Implemented".to_string(),
//                     }
//                 }
//                 ClientCommand::RequestToProduce { topic_name } => {
//                     tracing::info!("Received command to produce to topic: {}", topic_name);
//                     BrokerResponse::InternalError {
//                         message: "Not Implemented".to_string(),
//                     }
//                 }
//                 ClientCommand::RequestToStop { topic_name } => {
//                     tracing::info!(
//                         "Received command to stop producing to topic: {}",
//                         topic_name
//                     );
//                     BrokerResponse::InternalError {
//                         message: "Not Implemented".to_string(),
//                     }
//                 }
//             };
//             let response = bincode::serialize(&broker_response);
//             writer.try_write(&response.unwrap()).unwrap();
//         }
//         Err(e) => {
//             tracing::error!("Error reading from stream: {:?}", e);
//         }
//     }
// }

// async fn search_nodes_in_cluster(
//     service_name: &str,
//     main_tx: mpsc::Sender<MainCommands>,
//     cancellation_token: CancellationToken,
//     pod_ip: &str,
// ) {
//     let mut interval = tokio::time::interval(Duration::from_secs(SLEEP_TIME_IN_SECONDS));
//     interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
//     loop {
//         tokio::select! {
//             _ = interval.tick() => {
//                 let peers_in_cluster = dig_cluster_nodes(service_name, pod_ip);
//                 tracing::debug!("Found these peers in cluster: {:?}", peers_in_cluster);
//                 for peer in peers_in_cluster {
//                     let peer_address = format!("{}:{}", peer, NODE_MANAGER_PORT);
//                     let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeResponse>();
//                     main_tx.send(MainCommands::AddPeer {
//                         peer_address,
//                         tx:oneshot_tx,
//                     }).await.unwrap();
//                     match oneshot_rx.await {
//                         Ok(NodeResponse::PeerAdded) => {
//                             tracing::info!("Peer added successfully.");
//                         }
//                         Err(e) => {
//                             tracing::error!("Failed to add peer.: {:?}", e);
//                         }
//                     }
//                 }
//             }
//             _ = cancellation_token.cancelled() => {
//                 tracing::debug!("Cancelling search_nodes_in_cluster task.");
//                 break;
//             }
//         }
//     }
// }

// fn dig_cluster_nodes(service_name: &str, pod_ip: &str) -> Vec<String> {
//     let output = Command::new("dig")
//         .args(["+short", "+search", service_name])
//         .output()
//         .expect("Failed to execute dig command...");

//     if output.status.success() {
//         let output = String::from_utf8_lossy(&output.stdout);
//         output
//             .split("\n")
//             .map(|ip| ip.trim().to_string())
//             .filter(|ip| !ip.is_empty() && ip != pod_ip)
//             .collect()
//     } else {
//         Vec::new()
//     }
// }

// pub enum MainCommands {
//     AddPeer {
//         peer_address: String,
//         tx: oneshot::Sender<NodeResponse>,
//     },
// }

// pub enum NodeResponse {
//     PeerAdded,
// }
