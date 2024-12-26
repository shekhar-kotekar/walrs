use std::{sync::Arc, time::Duration};

use tokio::{
    net::UdpSocket,
    sync::{mpsc::Sender, oneshot},
    time::interval,
};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{Answer, ClusterMessage, ClusterStateQuery, Node, NodeState},
    NODE_MANAGER_PORT, SLEEP_TIME_IN_SECONDS,
};

pub async fn start_node_manager(
    state_keeper_tx: Sender<ClusterStateQuery>,
    node_manager_port: u32,
    cancellation_token: CancellationToken,
) {
    let mut interval_timer = interval(Duration::from_millis(SLEEP_TIME_IN_SECONDS * 5));
    let socket = UdpSocket::bind(format!("0.0.0.0:{}", node_manager_port))
        .await
        .unwrap();
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

            send_request_for_leadership(
                socket_for_leadership_request,
                local_node,
                other_nodes,
                node_manager_port,
            )
            .await;
        }
    }

    tokio::spawn(async move {
        let mut buffer = [0u8; 1024];
        loop {
            let socket_for_receiving = arc_socket.clone();
            let state_keeper_tx = state_keeper_tx.clone();
            tokio::select! {
                recv_result = socket_for_receiving.recv_from(&mut buffer) => {
                    match recv_result {
                        Ok((size, _)) => {
                            let received_message = bincode::deserialize::<ClusterMessage>(&buffer[..size]).unwrap();
                            tokio::spawn(async move {
                                match received_message {
                                    ClusterMessage::VoteRequest(node) => {
                                        tracing::info!("Received vote request from node: {:?}", node);
                                        let leader_node = Node {
                                            state: NodeState::Leader,
                                            ..node.clone()
                                        };
                                        let (oneshot_tx, oneshot_rx) = oneshot::channel::<bool>();
                                        let update_node_query = ClusterStateQuery::UpdateNode {
                                            node_details: leader_node,
                                            tx: oneshot_tx,
                                        };
                                        state_keeper_tx.send(update_node_query).await.unwrap();
                                        let result = oneshot_rx.await.unwrap();
                                        let response = ClusterMessage::VoteResponse {
                                            node: node.clone(),
                                            answer: if result { Answer::LeaderAccepted } else { Answer::LeaderRejected },
                                        };
                                        let destination = format!("{}:{}", node.ip_address, node_manager_port);
                                        socket_for_receiving.send_to(&bincode::serialize(&response).unwrap(), destination).await.unwrap();
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

                    let (oneshot_tx, oneshot_rx) = oneshot::channel::<Node>();
                    let query = ClusterStateQuery::GetLocalNode { tx: oneshot_tx };
                    state_keeper_tx.send(query).await.unwrap();
                    let local_node = oneshot_rx.await.unwrap();
                    send_heartbeat(socket_for_receiving,other_nodes, local_node).await;
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
    node_manager_port: u32,
) {
    local_node.next_candidate();
    tracing::info!("Nominating {:?} as a leader.", local_node);
    let message_to_send = bincode::serialize(&ClusterMessage::VoteRequest(local_node)).unwrap();

    for node in other_nodes.iter() {
        tracing::info!("Sending vote request to node: {:?}", node);
        let destination = format!("{}:{}", node.ip_address, node_manager_port);
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

async fn send_heartbeat(
    socket: Arc<UdpSocket>,
    other_nodes_in_cluster: Vec<Node>,
    local_node: Node,
) {
    let message_to_send =
        bincode::serialize(&ClusterMessage::Heartbeat(local_node.clone())).unwrap();
    for node in other_nodes_in_cluster {
        tracing::info!("Sending heartbeat to node: {:?}", node);
        let destination = format!("{}:{}", node.ip_address, NODE_MANAGER_PORT);
        socket
            .send_to(&message_to_send, destination)
            .await
            .unwrap_or_else(|_| {
                tracing::error!("Error sending heartbeat to node: {:?}", node);
                0 // Return a default value of type usize
            });
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

        let node_manager_port = 5056;
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", node_manager_port))
            .await
            .unwrap();
        let arc_socket = Arc::new(socket);
        let arc_socket_clone = arc_socket.clone();
        tokio::spawn(async move {
            send_request_for_leadership(
                arc_socket_clone,
                current_node,
                vec![other_node_1, other_node_2],
                node_manager_port,
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
