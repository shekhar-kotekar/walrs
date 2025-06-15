use std::env;

use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    main_listener::MainListener,
    models::{ClusterInfo, NodeInfo},
    node_manager::{NodeManager, NodeManagerCommand},
};

mod handlers;
mod main_listener;
mod models;
mod node_manager;
mod partition_managers;

const MPSC_CHANNEL_SIZE: usize = 100;

#[tokio::main]
async fn main() {
    commons::init_tracing(Some(tracing::Level::INFO));
    let pod_ip: String = env::var("POD_IP").expect("POD_IP environment variable not set.");
    let address: String = format!("{}:8085", pod_ip);

    let node_info = NodeInfo::new(address.clone());

    let mut cluster_info = ClusterInfo::new();
    cluster_info.add_node(node_info);

    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let mut node_manager = NodeManager::new(
        address.clone(),
        "/tmp/walrs/data".to_string(),
        3_000,
        cluster_info,
    );
    let node_manager_cancellation_token = cancellation_token.child_token();
    let (node_manager_tx, node_manager_rx) = mpsc::channel::<NodeManagerCommand>(MPSC_CHANNEL_SIZE);
    task_tracker.spawn(async move {
        node_manager
            .start(node_manager_rx, node_manager_cancellation_token)
            .await;
    });

    let main_listener = MainListener {
        address,
        node_manager_tx,
    };
    main_listener.start(task_tracker, cancellation_token).await;

    tracing::info!("Main Exited.");
}
