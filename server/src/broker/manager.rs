use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use crate::{
    broker::{create_topic::create_topic, heartbeat},
    models::{BrokerInfo, ClusterInfo, CommandToBroker},
};

pub struct Broker {
    cluster_info: ClusterInfo,
    data_dir_path: String,
    address: String,
    heartbeat_interval: tokio::time::Interval,
}

impl Broker {
    pub fn new(
        ip: String,
        peer_listener_port: u16,
        peers: Vec<String>,
        data_dir_path: String,
        heartbeat_interval_ms: u16,
    ) -> Self {
        let data_dir_path = format!(
            "{}/{}-{}",
            data_dir_path,
            ip.replace([':', '.'], "-"),
            peer_listener_port
        );
        std::fs::create_dir_all(&data_dir_path).unwrap_or_else(|e| {
            panic!(
                "Failed to create base directory: {} because: {}",
                &data_dir_path, e
            );
        });

        let self_address = format!("{}:{}", ip, peer_listener_port);
        let self_info = BrokerInfo::new();

        let mut cluster_info = ClusterInfo::new().with_peers(peers);
        cluster_info.add_broker(self_address.clone(), self_info);

        let heartbeat_interval =
            tokio::time::interval(std::time::Duration::from_millis(heartbeat_interval_ms as u64));

        Broker {
            cluster_info,
            data_dir_path,
            address: self_address,
            heartbeat_interval,
        }
    }
    pub async fn start(
        &mut self,
        mut command_rx: Receiver<CommandToBroker>,
        cancellation_token: CancellationToken,
    ) {
        loop {
            tokio::select! {
                Some(command) = command_rx.recv() => {
                    match command {
                        CommandToBroker::CreateNewTopic { topic, broker_role, broker_tx } => {
                            tracing::info!("Received command to create new topic: {:?}", topic);
                            let cluster_info = self.cluster_info.clone();
                            let self_address = self.address.clone();
                            let broker_data_dir_path = self.data_dir_path.clone();
                            let topic_cancellation_token = cancellation_token.child_token();
                            tokio::spawn(async move {
                                create_topic(
                                    topic,
                                    self_address,
                                    broker_data_dir_path,
                                    broker_role,
                                    cluster_info,
                                    topic_cancellation_token,
                                ).await;
                            });
                        }
                        _ => {
                            tracing::info!("Command not implemented (yet): {:?}", command);
                        }
                    }
                },
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token cancelled. Broker shutting down : {}", self.address);
                    break;
                }
                _ = self.heartbeat_interval.tick() => {
                    let cluster_info = self.cluster_info.clone();
                    let self_address = self.address.clone();
                    tokio::spawn(async move {heartbeat::send_heartbeat(self_address, cluster_info).await;});
                }
            }
        }
        tracing::info!("Broker stopped: {}", self.address);
    }
}
