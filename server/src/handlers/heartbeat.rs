use commons::models::{PeerCommand, PeerResponse, WalrsCommand, WalrsResponse};

use crate::models::ClusterInfo;

pub async fn send_heartbeat(self_address: String, cluster_info: ClusterInfo) {
    match cluster_info.nodes.get(&self_address) {
        Some(broker_info) => {
            let peers: Vec<String> = cluster_info
                .nodes
                .keys()
                .filter(|&peer| peer != &self_address)
                .cloned()
                .collect();
            let heartbeat = WalrsCommand::Peer(PeerCommand::Heartbeat {
                node_info: broker_info.clone(),
            });
            tracing::debug!(
                "Sending heartbeat to peers: {:?},message: {:?}",
                peers,
                heartbeat
            );
            let serialized_heartbeat = commons::to_bytes(&heartbeat);

            let mut successful_peers = 0;
            for peer in &peers {
                match commons::send_serialized_message::<WalrsResponse>(&serialized_heartbeat, peer)
                    .await
                {
                    Ok(WalrsResponse::Peer(PeerResponse::HeartbeatAcknoweledged)) => {
                        tracing::debug!("Heartbeat acknowledged by peer: {}", peer);
                        successful_peers += 1;
                    }
                    Ok(WalrsResponse::Peer(PeerResponse::Error { message })) => {
                        tracing::error!(
                            "Error response for heartbeat signal from peer {}: {}",
                            peer,
                            message
                        );
                    }
                    Ok(other) => {
                        tracing::error!("Invalid response from peer: {}: {:?}", peer, other);
                    }
                    Err(e) => {
                        tracing::error!("Failed to send heartbeat to {}: {}", peer, e);
                    }
                }
            }
            if successful_peers == peers.len() {
                tracing::info!(
                    "Heartbeat sent to {} out of {} peers. Total peers: {:?}",
                    successful_peers,
                    peers.len(),
                    peers
                );
            } else {
                tracing::warn!(
                    "Heartbeat sent to {} out of {} peers. Some peers may not be reachable.",
                    successful_peers,
                    peers.len()
                );
            }
        }
        None => tracing::error!(
            "Broker {} not found in cluster info, Heartbeat not sent.",
            self_address
        ),
    }
}

#[cfg(test)]
mod tests {
    use commons::models::NodeInfo;
    use tokio::io::AsyncWriteExt;
    use tracing_test::traced_test;

    use super::*;

    #[tokio::test]
    #[ignore]
    #[traced_test]
    async fn test_send_heartbeat() {
        let mut cluster_info = ClusterInfo::new();

        let self_address = "127.0.0.1:8080".to_string();
        let mut self_info = NodeInfo::new(self_address.clone());
        self_info.registed_topics.push("test_topic".to_string());
        cluster_info.add_node(self_info);

        let remote_address = "127.0.0.1:8081".to_string();
        cluster_info.set_peers(vec![remote_address.clone()]);

        let self_address_clone = self_address.clone();
        let heartbeat_task_join_handle = tokio::spawn(async move {
            send_heartbeat(self_address_clone, cluster_info).await;
        });

        let remote_listener = tokio::net::TcpListener::bind(&remote_address)
            .await
            .unwrap();
        tracing::debug!("Remote listener started on {}", remote_address);
        let mut remote_stream = remote_listener.accept().await.unwrap().0;
        tracing::debug!("Remote stream accepted from {}", remote_address);

        match commons::read_from_socket::<PeerCommand>(&mut remote_stream).await {
            Ok(command) => {
                tracing::debug!("Received command from remote stream: {:?}", command);
                match command {
                    PeerCommand::Heartbeat { node_info } => {
                        tracing::debug!("Received heartbeat from peer: {}", node_info.address);
                        assert_eq!(node_info.address, self_address);
                        assert_eq!(node_info.registed_topics, vec!["test_topic".to_string()]);

                        let serialized_response =
                            commons::to_bytes(&PeerResponse::HeartbeatAcknoweledged);
                        remote_stream.write_all(&serialized_response).await.unwrap();
                        remote_stream.flush().await.unwrap();
                    }
                    _ => {
                        heartbeat_task_join_handle.await.unwrap();
                        panic!("Unexpected command received: {:?}", command);
                    }
                }
            }
            Err(e) => {
                heartbeat_task_join_handle.await.unwrap();
                panic!("Failed to read command from remote stream: {}", e);
            }
        }
        remote_stream.shutdown().await.unwrap();
        tracing::debug!("Remote stream shutdown");
        heartbeat_task_join_handle.await.unwrap();
    }
}
