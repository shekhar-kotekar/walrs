use commons::models::BrokerInfo;
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    broker::{Broker, BrokerCommand},
    metrics::server::{MetricEvent, MetricsServer},
    models::ClusterInfo,
};

mod broker;
mod handlers;
mod main_listener;
mod metrics;
mod models;
mod partition_managers;
mod topic_creation;

const DATA_DIR_PATH: &str = "/tmp/walrs/data/";
const POD_NAME_PREFIX_ENV: &str = "POD_NAME_PREFIX";
const REPLICA_COUNT_ENV: &str = "REPLICA_COUNT";
const TASK_TIMEOUT_SECONDS: u64 = 20;

const MPSC_CHANNEL_SIZE: usize = 100;
const PORT: u16 = 5056;
const METRICS_PORT: u16 = 8000;

#[tokio::main]
async fn main() {
    commons::init_tracing(None);

    let cluster_info = get_cluster_info();

    let self_address = cluster_info.self_info.address.clone();

    tracing::info!("Starting WAL-rs server: {}", &self_address);

    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let (broker_tx, broker_rx) = mpsc::channel::<BrokerCommand>(MPSC_CHANNEL_SIZE);
    let broker_cancellation_token = cancellation_token.child_token();

    // Start metrics server
    let (metrics_tx, metrics_rx) = mpsc::channel::<MetricEvent>(MPSC_CHANNEL_SIZE);
    let metrics_server = MetricsServer::new();
    let metrics_cancellation_token = cancellation_token.child_token();

    task_tracker.spawn(async move {
        metrics_server
            .start(metrics_rx, METRICS_PORT, metrics_cancellation_token)
            .await;
    });

    let heartbeat_interval_seconds = 300;
    let mut broker = Broker::new(
        DATA_DIR_PATH.to_string(),
        Some(heartbeat_interval_seconds),
        Some(cluster_info),
    );
    task_tracker.spawn(async move {
        broker.start(broker_rx, broker_cancellation_token).await;
    });
    let main_listener = main_listener::MainListener {
        address: self_address.clone(),
        broker_tx,
        metrics_tx,
    };
    main_listener.start(task_tracker, cancellation_token).await;

    tracing::info!("WAL-rs server has stopped.");
}

fn get_cluster_info() -> ClusterInfo {
    let self_name = std::env::var("POD_NAME").unwrap();
    let service_name: String =
        std::env::var("SERVICE_NAME").unwrap_or_else(|_| "walrs-headless-service".into());
    let namespace: String = std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".into());

    let cluster_domain: String =
        std::env::var("CLUSTER_DOMAIN").unwrap_or_else(|_| "cluster.local".into());

    let pod_name_prefix: String = std::env::var(POD_NAME_PREFIX_ENV).unwrap();
    let replica_count: u32 = std::env::var(REPLICA_COUNT_ENV)
        .unwrap_or_else(|_| "3".into())
        .parse()
        .unwrap();

    let mut cluster_info = ClusterInfo::new();
    for i in 0..replica_count {
        let pod_name = format!("{}-{}", pod_name_prefix, i);
        let broker_address = format!(
            "{}.{}.{}.svc.{}:{}",
            pod_name, service_name, namespace, cluster_domain, PORT
        );
        let broker_info: BrokerInfo = BrokerInfo::new(broker_address);
        if pod_name == self_name {
            cluster_info.self_info = broker_info.clone();
        } else {
            cluster_info.add_peer(broker_info);
        }
    }
    tracing::debug!("Cluster info: {:?}", cluster_info);
    cluster_info
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_cluster_information() {
        // set environment variables for testing
        std::env::set_var("POD_NAME", "walrs-srvr-0");
        std::env::set_var("SERVICE_NAME", "walrs-headless-service");
        std::env::set_var("POD_NAMESPACE", "dummy-namespace");
        std::env::set_var("CLUSTER_DOMAIN", "cluster.local");
        std::env::set_var(POD_NAME_PREFIX_ENV, "walrs-srvr");
        std::env::set_var(REPLICA_COUNT_ENV, "5");

        let cluster_info = get_cluster_info();
        assert_eq!(
            cluster_info.self_info.address,
            "walrs-srvr-0.walrs-headless-service.dummy-namespace.svc.cluster.local:5056"
        );
        assert_eq!(cluster_info.peers.len(), 4);
    }
}
