use commons::models::NodeInfo;
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    main_listener::MainListener,
    models::{ClusterInfo, NodeConfig, NodeConfigBuilder},
    node_manager::{NodeManager, NodeManagerCommand},
};

mod handlers;
mod main_listener;
mod models;
mod node_manager;
mod partition_managers;

// const K8S_SERVICE_NAME: &str = "walrs-headless-service.walrs.svc.cluster.local";

#[tokio::main]
async fn main() {
    commons::init_tracing(Some(tracing::Level::DEBUG));
    // let pod_ip: String = env::var("POD_IP").unwrap_or("127.0.0.1".to_string());

    let node_config: NodeConfig = get_node_config();
    let node_address = node_config.address.clone();
    let node_info: NodeInfo = NodeInfo::new(node_address.clone());

    let mut cluster_info = ClusterInfo::new();
    cluster_info.add_node(node_info);
    cluster_info.set_peers(node_config.peers.clone());

    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let mpsc_queue_size = node_config.mpsc_queue_size;
    let mut node_manager = NodeManager::new(node_config, cluster_info);
    let node_manager_cancellation_token = cancellation_token.child_token();
    let (node_manager_tx, node_manager_rx) = mpsc::channel::<NodeManagerCommand>(mpsc_queue_size);
    task_tracker.spawn(async move {
        node_manager
            .start(node_manager_rx, node_manager_cancellation_token)
            .await;
    });

    let main_listener = MainListener {
        address: node_address,
        node_manager_tx,
    };
    main_listener.start(task_tracker, cancellation_token).await;

    tracing::info!("Main Exited.");
}

fn get_node_config() -> NodeConfig {
    let config_file_path = std::env::var("NODE_CONFIG_FILE").unwrap_or_else(|_| {
        tracing::warn!(
            "NODE_CONFIG_FILE environment variable not set. Using default config file path."
        );
        "./configs/broker_conf.yml".to_string()
    });

    let builder = NodeConfigBuilder::from_yaml_file(&config_file_path)
        .expect("Failed to read node config from YAML file");
    builder.build()
}
