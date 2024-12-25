use std::{sync::Arc, time::Duration};

use tokio::{net::UdpSocket, time::interval};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{Answer, Cluster, ClusterMessage},
    SLEEP_TIME_IN_SECONDS,
};

const NODE_MANAGER_PORT: u32 = 5057;

pub async fn start_node_manager(cluster: Cluster, cancellation_token: CancellationToken) {
    let mut interval_timer = interval(Duration::from_millis(SLEEP_TIME_IN_SECONDS));
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let arc_socket = Arc::new(socket);

    let socket_for_leadership_request = arc_socket.clone();
    if cluster.lealder.is_none() {
        tracing::info!("No leader found in the cluster.");
        send_request_for_leadership(cluster.clone(), socket_for_leadership_request).await;
    }

    tokio::spawn(async move {
        let mut buffer = [0u8; 1024];
        loop {
            let socket_for_receiving = arc_socket.clone();
            tokio::select! {
                recv_result = socket_for_receiving.recv_from(&mut buffer) => {
                    match recv_result {
                        Ok((size, peer)) => {
                            tracing::info!("Received message from {}: {}", peer, String::from_utf8_lossy(&buffer[..size]));
                            let received_message = bincode::deserialize::<ClusterMessage>(&buffer[..size]).unwrap();
                            tokio::spawn(async move {
                                match received_message {
                                    ClusterMessage::VoteRequest(node) => {
                                        tracing::info!("Received vote request from node: {:?}", node);
                                    }
                                    ClusterMessage::VoteResponse {node, answer} => {
                                        tracing::info!("Received vote response from node");
                                        match answer {
                                            Answer::LeaderAccepted => {
                                                tracing::info!("Node {:?} accepted the leader.", node);
                                            }
                                            Answer::LeaderRejected => {
                                                tracing::info!("Node {:?} rejected the leader.", node);
                                            }
                                        }
                                    }
                                    ClusterMessage::LeaderElected(node) => {
                                        tracing::info!("Node {:?} elected as a leader.", node);
                                    }
                                    ClusterMessage::Heartbeat(node) => {
                                        tracing::info!("Received heartbeat from node: {:?}", node);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Error receiving from socket: {}", e);
                            break;
                        }
                    }
                }
                _ = interval_timer.tick() => {
                    send_heartbeat(cluster.clone());
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Node manager shutting down!");
                    break;
                }
            }
        }
    });
}

async fn send_request_for_leadership(cluster: Cluster, socket: Arc<UdpSocket>) {
    let mut current_node = cluster.current_node.clone();
    current_node.next_candidate();
    tracing::info!("Nominating this node {:?} as a leader.", current_node);
    let message_to_send = bincode::serialize(&ClusterMessage::VoteRequest(current_node)).unwrap();
    for node in cluster.other_nodes.iter() {
        tracing::info!("Sending vote request to node: {:?}", node);
        let destination = format!("{}:{}", node.ip_address, NODE_MANAGER_PORT);
        socket
            .send_to(&message_to_send, destination)
            .await
            .expect(format!("Couldn't send vote request message to {:?} node.", node).as_str());
    }
    tracing::info!(
        "Vote request sent to {} nodes in the cluster.",
        cluster.other_nodes.len()
    );
}

fn send_heartbeat(cluster: Cluster) {
    for node in cluster.other_nodes {
        tracing::info!("Sending heartbeat to node: {:?}", node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Node, NodeState};
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_node_should_be_able_to_send_request_for_leadership() {
        let other_node = Node::new(Some("0.0.0.0".to_string()));
        let current_node = Node::new(Some("0.0.0.0".to_string()));
        let cluster = Cluster {
            current_node,
            other_nodes: vec![other_node],
            lealder: None,
        };

        let socket = UdpSocket::bind(format!("0.0.0.0:{}", NODE_MANAGER_PORT))
            .await
            .unwrap();
        let arc_socket = Arc::new(socket);
        let arc_socket_clone = arc_socket.clone();
        tokio::spawn(async move {
            send_request_for_leadership(cluster, arc_socket_clone).await;
        });

        let mut buffer = [0u8; 2048];
        arc_socket.recv_from(&mut buffer).await.unwrap();
        let decoded_message = bincode::deserialize::<ClusterMessage>(&buffer).unwrap();
        match decoded_message {
            ClusterMessage::VoteRequest(node) => {
                assert_eq!(node.state, NodeState::Candidate);
                assert_eq!(node.term, 1);
            }
            _ => panic!(
                "Expected VoteRequest message, but got {:?}",
                decoded_message
            ),
        }
    }
}
