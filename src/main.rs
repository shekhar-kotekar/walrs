use common::enable_tracing;
use models::{Cluster, ClusterMessage, ClusterStateQuery, Node, NodeState};
use std::{process::Command, thread, time::Duration};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::{net::TcpListener, signal};
use tokio_util::{bytes::BytesMut, sync::CancellationToken, task::TaskTracker};

mod cluster_state_keeper;
mod common;
mod models;
mod node_manager;

// TODO: Read all the constants from a config file
const MAIN_PORT: u32 = 5056;
const NODE_MANAGER_PORT: u32 = 5057;

const MAX_RETRIES: u8 = 30;
const SLEEP_TIME_IN_SECONDS: u64 = 3;
const K8S_SERVICE_NAME: &str = "kraft-rs-service";
const MPSC_MAX_Q_SIZE: usize = 100;

#[tokio::main]
async fn main() {
    enable_tracing();
    console_subscriber::init();

    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let cluster_info: Cluster = get_cluster_info();

    let cluster_state_keeper_cancellation_token = cancellation_token.clone();
    let (cluster_state_keeper_tx, cluster_state_keeper_rx) =
        mpsc::channel::<ClusterStateQuery>(MPSC_MAX_Q_SIZE);

    task_tracker.spawn(async move {
        cluster_state_keeper::maintain_cluster_state(
            cluster_info,
            cluster_state_keeper_rx,
            cluster_state_keeper_cancellation_token,
        )
        .await;
    });

    let node_manager_cancellation_token = cancellation_token.clone();
    task_tracker.spawn(async move {
        node_manager::start_node_manager(
            cluster_state_keeper_tx,
            NODE_MANAGER_PORT,
            node_manager_cancellation_token,
        )
        .await;
    });

    let receiver_cancellation_token = cancellation_token.clone();
    let broker_address = format!("0.0.0.0:{}", MAIN_PORT);
    let listener = TcpListener::bind(broker_address).await.unwrap();
    loop {
        tokio::select! {
            Ok((socket, _)) = listener.accept() => {
                tracing::info!("New connection accepted.");
                task_tracker.spawn(async move {
                    let mut buf_stream = tokio::io::BufStream::new(socket);
                    let mut message_buffer = BytesMut::with_capacity(56);
                    buf_stream.read_buf(&mut message_buffer).await.unwrap();
                    let _ = ClusterMessage::from(message_buffer.to_vec());
                });
            }
            _ = receiver_cancellation_token.cancelled() => {
                tracing::info!("Receiver shutting down!");
                break;
            }
        }
    }

    match signal::ctrl_c().await {
        Ok(_) => {
            tracing::info!("Received Ctrl-C signal. Cancelling all tasks.");
            cancellation_token.cancel();
            task_tracker.close();
            task_tracker.wait().await;
            tracing::info!("All tasks cancelled.");
        }
        Err(e) => {
            tracing::error!("Error occurred while waiting for Ctrl-C signal: {:?}", e);
        }
    }
    tracing::info!("Exiting main.");
}

fn get_cluster_info() -> Cluster {
    let pod_uid = std::env::var("POD_UID").unwrap();
    let pod_ip: String = std::env::var("POD_IP").unwrap();
    let current_node = Node {
        id: uuid::Uuid::parse_str(&pod_uid).unwrap(),
        ip_address: pod_ip,
        state: NodeState::Follower,
        term: 0,
        is_local: true,
    };
    tracing::info!("Current Node: {:?}", current_node);
    let mut nodes_in_cluster: Vec<Node> = wait_until_nodes_are_added_to_cluster();
    nodes_in_cluster.push(current_node);
    Cluster {
        nodes: nodes_in_cluster,
    }
}

fn wait_until_nodes_are_added_to_cluster() -> Vec<Node> {
    let mut try_count = 0;
    loop {
        let nodes: Vec<Node> = dig_cluster_nodes(K8S_SERVICE_NAME);
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
            thread::sleep(Duration::from_secs(SLEEP_TIME_IN_SECONDS));
            continue;
        } else {
            tracing::info!("Found {} nodes in the cluster.", nodes.len());
            return nodes;
        }
    }
}

fn dig_cluster_nodes(service_name: &str) -> Vec<Node> {
    let output = Command::new("dig")
        .args(["+short", "+search", service_name])
        .output()
        .expect("Failed to execute dig command...");

    if output.status.success() {
        let output = String::from_utf8_lossy(&output.stdout);
        output
            .split("\n")
            .map(|ip| ip.trim().to_string())
            .filter(|ip| !ip.is_empty())
            .map(|node_ip| Node::new(Some(node_ip)))
            .collect()
    } else {
        Vec::new()
    }
}
