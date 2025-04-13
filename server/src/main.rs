use common::{
    enable_tracing,
    models::{BrokerResponse, ClientCommand},
};
use models::MainCommands;
use node::Node;
use rand::{thread_rng, Rng};
use std::{process::Command, thread, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    signal,
    sync::mpsc,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

mod models;
mod node;
mod partitions;

// TODO: Read all the constants from a config file
const NODE_MANAGER_PORT: u16 = 5056;
const WALRS_PORT: u16 = 5055;
const MAX_RETRIES: u8 = 30;
const SLEEP_TIME_IN_SECONDS: u64 = 5;
const K8S_SERVICE_NAME: &str = "walrs-headless-service.walrs.svc.cluster.local";
const MPSC_MAX_Q_SIZE: usize = 100;
const MIN_HEARTBEAT_INTERVAL_MS: u64 = 1000;
const MAX_HEARTBEAT_INTERVAL_MS: u64 = 15000;

// Main will be responsible for external facing communication like producer or consumer requests.
// Internal communication will be handled by Node and partition managers.
// So we will need 2-4 sockets in total.
// Advantage of this approach is that Main thread does not need to manage all type of requests.
#[tokio::main]
async fn main() {
    enable_tracing();

    let pod_ip: String = std::env::var("POD_IP").unwrap();
    let sleep_interval: u64 =
        thread_rng().gen_range(MIN_HEARTBEAT_INTERVAL_MS..MAX_HEARTBEAT_INTERVAL_MS);
    let node_address = format!("{}:{}", pod_ip, NODE_MANAGER_PORT);
    let mut local_node = Node::new(node_address, sleep_interval);
    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    //TODO: How to get the list of peers from the cluster periodically?
    let peers = get_cluster_info(&pod_ip, Duration::from_secs(SLEEP_TIME_IN_SECONDS));

    let (_, main_rx) = mpsc::channel::<MainCommands>(MPSC_MAX_Q_SIZE);

    let node_cancellation_token = cancellation_token.child_token();
    task_tracker.spawn(async move {
        local_node
            .run(peers, main_rx, node_cancellation_token)
            .await;
    });

    let main_tcp_listener = TcpListener::bind(format!("{}:{}", pod_ip, WALRS_PORT))
        .await
        .unwrap();

    tracing::debug!("Listening on: {}", main_tcp_listener.local_addr().unwrap());

    tokio::select! {
        _ = async {
            loop {
                let (socket, _) = main_tcp_listener.accept().await.unwrap();
                task_tracker.spawn(async move {
                    process_external_request(socket).await;
                });
            }
        } => {
            tracing::info!("Main listener closed.");
        },
        _ = signal::ctrl_c() => {
            tracing::info!("Received Ctrl-C signal. Cancelling all tasks.");
            cancellation_token.cancel();
            task_tracker.close();
            task_tracker.wait().await;
            tracing::info!("All tasks cancelled.");
        }
    }
    tracing::info!("Exiting main.");
}

async fn process_external_request(socket: TcpStream) {
    let (reader, writer) = socket.into_split();
    let mut buffer = [0u8; 48];
    tracing::debug!(
        "Processing external request from: {}",
        reader.peer_addr().unwrap()
    );
    match reader.try_read(&mut buffer) {
        Ok(0) => {
            tracing::info!("Connection closed for: {}", reader.peer_addr().unwrap());
        }
        Ok(bytes_read) => {
            let client_command: ClientCommand =
                bincode::deserialize(&buffer[..bytes_read]).unwrap();
            let broker_response: BrokerResponse = match client_command {
                ClientCommand::CreateTopic {
                    topic_name,
                    num_partitions,
                    retention_period_hours,
                } => {
                    tracing::info!(
                        "Received command to create topic '{}' with {} partitions and {} hours retention period.",
                        topic_name,
                        num_partitions,
                        retention_period_hours
                    );
                    BrokerResponse::InternalError {
                        message: "Not Implemented".to_string(),
                    }
                }
                ClientCommand::RequestToProduce { topic_name } => {
                    tracing::info!("Received command to produce to topic: {}", topic_name);
                    BrokerResponse::InternalError {
                        message: "Not Implemented".to_string(),
                    }
                }
                ClientCommand::RequestToStop { topic_name } => {
                    tracing::info!(
                        "Received command to stop producing to topic: {}",
                        topic_name
                    );
                    BrokerResponse::InternalError {
                        message: "Not Implemented".to_string(),
                    }
                }
            };
            let response = bincode::serialize(&broker_response);
            writer.try_write(&response.unwrap()).unwrap();
        }
        Err(e) => {
            tracing::error!("Error reading from stream: {:?}", e);
        }
    }
}

fn get_cluster_info(pod_ip: &str, sleep_duration_seconds: Duration) -> Vec<Node> {
    let mut try_count = 0;
    loop {
        let nodes: Vec<String> = dig_cluster_nodes(K8S_SERVICE_NAME, pod_ip);
        if nodes.is_empty() {
            try_count += 1;
            if try_count >= MAX_RETRIES {
                panic!(
                    "No nodes found in the cluster after {} tries. Exiting...",
                    MAX_RETRIES
                );
            }
            tracing::warn!(
                "No nodes found in the cluster. Will retry after {} seconds.",
                SLEEP_TIME_IN_SECONDS
            );
            thread::sleep(sleep_duration_seconds);
        } else {
            tracing::info!("Found {} nodes in the cluster.", nodes.len());
            return nodes
                .iter()
                .map(|peer_ip_address| {
                    let peer_address = format!("{}:{}", peer_ip_address, NODE_MANAGER_PORT);
                    tracing::info!("peer address: {}", peer_address);
                    Node::new(peer_address, 0)
                })
                .collect::<Vec<Node>>();
        }
    }
}

fn dig_cluster_nodes(service_name: &str, pod_ip: &str) -> Vec<String> {
    tracing::debug!("Running dig command for {} service.", service_name);
    let output = Command::new("dig")
        .args(["+short", "+search", service_name])
        .output()
        .expect("Failed to execute dig command...");

    if output.status.success() {
        let output = String::from_utf8_lossy(&output.stdout);
        output
            .split("\n")
            .map(|ip| ip.trim().to_string())
            .filter(|ip| !ip.is_empty() && ip != pod_ip)
            .collect()
    } else {
        Vec::new()
    }
}
