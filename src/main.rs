use common::enable_tracing;
use models::{NodeQuery, NodeState};
use node::Node;
use rand::{thread_rng, Rng};
use std::{process::Command, thread, time::Duration};
use tokio::{signal, sync::mpsc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

mod common;
mod models;
mod node;
mod partition;
mod topic;

// TODO: Read all the constants from a config file
const NODE_MANAGER_PORT: u16 = 5056;

const MAX_RETRIES: u8 = 30;
const SLEEP_TIME_IN_SECONDS: u64 = 5;
const K8S_SERVICE_NAME: &str = "kraft-rs-service";
const MPSC_MAX_Q_SIZE: usize = 100;
const HEARTBEAT_MAX_INTERVAL_MS: u64 = 10000;

#[tokio::main]
async fn main() {
    enable_tracing();

    let pod_ip: String = std::env::var("POD_IP").unwrap();
    let mut local_node = Node {
        id: Uuid::new_v4(),
        state: NodeState::Follower,
        term: 0,
        address: format!("{}:{}", pod_ip, NODE_MANAGER_PORT),
    };
    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();
    let peers = get_cluster_info(&pod_ip, Duration::from_secs(SLEEP_TIME_IN_SECONDS));
    let interval_ms = thread_rng().gen_range(100..HEARTBEAT_MAX_INTERVAL_MS);
    let (_, rx) = mpsc::channel::<NodeQuery>(MPSC_MAX_Q_SIZE);

    let node_cancellation_token = cancellation_token.clone();
    task_tracker.spawn(async move {
        local_node
            .run(interval_ms, peers, rx, node_cancellation_token)
            .await;
    });

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

fn get_cluster_info(pod_ip: &str, sleep_duration_seconds: Duration) -> Vec<String> {
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
            return nodes;
        }
    }
}

fn dig_cluster_nodes(service_name: &str, pod_ip: &str) -> Vec<String> {
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
            .map(|ip| format!("{}:{}", ip, NODE_MANAGER_PORT))
            .collect()
    } else {
        Vec::new()
    }
}
