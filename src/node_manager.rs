use std::{sync::Arc, time::Duration};

use tokio::{
    net::UdpSocket,
    sync::{mpsc::Sender, oneshot},
    time::interval,
};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{Answer, ClusterMessage, ClusterStateQuery, Node},
    SLEEP_TIME_IN_SECONDS,
};

const NODE_MANAGER_PORT: u32 = 5057;

pub async fn start_node_manager(
    state_keeper_tx: Sender<ClusterStateQuery>,
    cancellation_token: CancellationToken,
) {
    let mut interval_timer = interval(Duration::from_millis(SLEEP_TIME_IN_SECONDS));
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let arc_socket = Arc::new(socket);

    let socket_for_leadership_request = arc_socket.clone();

    let (oneshot_tx, oneshot_rx) = oneshot::channel::<Option<Node>>();
    let query = ClusterStateQuery::GetLeader { tx: oneshot_tx };
    state_keeper_tx.send(query).await.unwrap();
    match oneshot_rx.await.unwrap() {
        Some(leader) => {
            tracing::info!("Leader found in the cluster: {:?}", leader);
        }
        None => {
            tracing::info!("No leader found in the cluster.");

            let (oneshot_tx, oneshot_rx) = oneshot::channel::<Node>();
            let query = ClusterStateQuery::GetLocalNode { tx: oneshot_tx };
            state_keeper_tx.send(query).await.unwrap();
            let local_node = oneshot_rx.await.unwrap();

            let (oneshot_tx, oneshot_rx) = oneshot::channel::<Vec<Node>>();
            let query = ClusterStateQuery::GetOtherNodes { tx: oneshot_tx };
            state_keeper_tx.send(query).await.unwrap();
            let other_nodes = oneshot_rx.await.unwrap();

            send_request_for_leadership(socket_for_leadership_request, local_node, other_nodes)
                .await;
        }
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
                    let (oneshot_tx, oneshot_rx) = oneshot::channel::<Vec<Node>>();
                    let query = ClusterStateQuery::GetOtherNodes { tx: oneshot_tx };
                    state_keeper_tx.send(query).await.unwrap();
                    let other_nodes = oneshot_rx.await.unwrap();
                    send_heartbeat(other_nodes);
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Node manager shutting down!");
                    break;
                }
            }
        }
    });
}

async fn send_request_for_leadership(
    socket: Arc<UdpSocket>,
    mut local_node: Node,
    other_nodes: Vec<Node>,
) {
    local_node.next_candidate();
    tracing::info!("Nominating {:?} as a leader.", local_node);
    let message_to_send = bincode::serialize(&ClusterMessage::VoteRequest(local_node)).unwrap();

    for node in other_nodes.iter() {
        tracing::info!("Sending vote request to node: {:?}", node);
        let destination = format!("{}:{}", node.ip_address, NODE_MANAGER_PORT);
        socket
            .send_to(&message_to_send, destination)
            .await
            .unwrap_or_else(|_| {
                tracing::error!("Error sending vote request to node: {:?}", node);
                0 // Return a default value of type usize
            });
    }
    tracing::info!(
        "Vote request sent to {} nodes in the cluster.",
        other_nodes.len()
    );
}

fn send_heartbeat(other_nodes_in_cluster: Vec<Node>) {
    for node in other_nodes_in_cluster {
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
        let other_node_1 = Node::new(Some("0.0.0.0".to_string()));
        let other_node_2 = Node::new(Some("0.0.0.0".to_string()));

        let current_node = Node::new(Some("0.0.0.0".to_string()));

        let socket = UdpSocket::bind(format!("0.0.0.0:{}", NODE_MANAGER_PORT))
            .await
            .unwrap();
        let arc_socket = Arc::new(socket);
        let arc_socket_clone = arc_socket.clone();
        tokio::spawn(async move {
            send_request_for_leadership(
                arc_socket_clone,
                current_node,
                vec![other_node_1, other_node_2],
            )
            .await;
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
