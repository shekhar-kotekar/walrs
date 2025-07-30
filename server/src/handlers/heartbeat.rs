use std::io::Error;

use commons::models::{PeerCommand, PeerResponse};
use tokio::task::JoinSet;

use crate::models::ClusterInfo;

pub async fn send_heartbeat(cluster_info: ClusterInfo) -> Result<(), Error> {
    let mut task_join_set: JoinSet<PeerResponse> = tokio::task::JoinSet::new();
    let heartbeat_message = PeerCommand::Heartbeat {
        broker_info: cluster_info.self_info.clone(),
    };

    let peer_addresses: Vec<_> = cluster_info.peers.keys().cloned().collect();
    let peers_in_cluster = peer_addresses.len();
    peer_addresses.into_iter().for_each(|peer_address| {
        let heartbeat_message_clone = heartbeat_message.clone();
        task_join_set.spawn(async move {
            commons::send_and_receive_peer_command(heartbeat_message_clone, &peer_address).await
        });
    });

    let task_result = task_join_set.join_all().await;
    let mut successful_heartbeats = 0;
    task_result.into_iter().for_each(|result| match result {
        PeerResponse::HeartbeatAcknowledged => {
            tracing::info!("Heartbeat acknowledged by peer.");
            successful_heartbeats += 1;
        }
        other => tracing::warn!("Unexpected response: {:?}", other),
    });
    if successful_heartbeats == peers_in_cluster {
        tracing::info!("All peers acknowledged the heartbeat ♥️");
    } else {
        tracing::warn!(
            "Only {}/{} peers acknowledged the heartbeat 💔",
            successful_heartbeats,
            peers_in_cluster
        );
    }
    Ok(())
}
