use std::{sync::Arc, time::Duration};

use rand::Rng;
use tokio::{
    net::UdpSocket,
    sync::{mpsc::Sender, oneshot},
    time::interval,
};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{Answer, ClusterMessage, ClusterStateQuery, Node, NodeState},
    NODE_MANAGER_PORT,
};

pub async fn start_node_manager(
    state_keeper_tx: Sender<ClusterStateQuery>,
    node_manager_port: u32,
    cancellation_token: CancellationToken,
) {
    let random_num = rand::thread_rng().gen_range(5000..30000);
    let mut interval_timer = interval(Duration::from_millis(random_num));
    tracing::info!("heartbeat interval time is: {} seconds.", random_num / 1000);

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

            let (oneshot_tx, oneshot_rx) = oneshot::channel::<Vec<Node>>();
            let query = ClusterStateQuery::GetOtherNodes { tx: oneshot_tx };
            state_keeper_tx.send(query).await.unwrap();
            let other_nodes = oneshot_rx.await.unwrap();

            send_request_for_leadership(
                state_keeper_tx.clone(),
                socket_for_leadership_request,
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
                                        let (oneshot_tx, oneshot_rx) = oneshot::channel::<bool>();
                                        let update_node_query = ClusterStateQuery::UpdateNode {
                                            node_details: node.clone(),
                                            tx: oneshot_tx,
                                        };
                                        state_keeper_tx.send(update_node_query).await.unwrap();
                                        let result = oneshot_rx.await.unwrap();
                                        if result {
                                            tracing::info!("Node {:?} updated successfully.", node);
                                        } else {
                                            tracing::error!("Error updating node: {:?}", node);
                                        }
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

                    let (oneshot_tx, oneshot_rx) = oneshot::channel::<Option<Node>>();
                    let query = ClusterStateQuery::GetLocalNode { tx: oneshot_tx };
                    state_keeper_tx.send(query).await.unwrap();
                    match oneshot_rx.await.unwrap() {
                        Some(local_node) => {
                            send_heartbeat(socket_for_receiving, other_nodes, local_node).await;
                        }
                        None => {
                            tracing::error!("Local node not found.");
                        }
                    }
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
    state_keeper_tx: Sender<ClusterStateQuery>,
    socket: Arc<UdpSocket>,
    other_nodes: Vec<Node>,
    node_manager_port: u32,
) {
    let (oneshot_tx, oneshot_rx) = oneshot::channel::<Option<Node>>();
    let nominate_local_node_as_leader_query =
        ClusterStateQuery::NominateLocalNodeAsLeader { tx: oneshot_tx };
    state_keeper_tx
        .send(nominate_local_node_as_leader_query)
        .await
        .unwrap();
    let result = oneshot_rx.await.unwrap();
    let local_node: Node = result.unwrap();
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
    use tokio::sync::mpsc;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_node_should_be_able_to_send_request_for_leadership() {
        let other_node_1 = Node::new(Some("0.0.0.0".to_string()));
        let other_node_2 = Node::new(Some("0.0.0.0".to_string()));

        let local_node = Node::new(Some("0.0.0.0".to_string()));

        let node_manager_port = 5056;
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", node_manager_port))
            .await
            .unwrap();
        let arc_socket = Arc::new(socket);
        let arc_socket_clone = arc_socket.clone();
        let (state_keeper_tx, mut state_keeper_rx) = mpsc::channel::<ClusterStateQuery>(3);
        tokio::spawn(async move {
            send_request_for_leadership(
                state_keeper_tx,
                arc_socket_clone,
                vec![other_node_1, other_node_2],
                node_manager_port,
            )
            .await;
        });

        match state_keeper_rx.recv().await.unwrap() {
            ClusterStateQuery::NominateLocalNodeAsLeader { tx } => {
                let local_leader_node = Node {
                    state: NodeState::Candidate,
                    term: 1,
                    ..local_node.clone()
                };
                tx.send(Some(local_leader_node)).unwrap();
            }
            other => panic!(
                "Expected NominateLocalNodeAsLeader query, but got {:?}",
                other
            ),
        }

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
