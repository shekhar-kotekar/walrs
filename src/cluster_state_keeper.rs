use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::models::{Cluster, ClusterStateQuery, NodeState};

pub async fn maintain_cluster_state(
    mut cluster: Cluster,
    mut rx: mpsc::Receiver<ClusterStateQuery>,
    cancellation_token: CancellationToken,
) {
    tracing::info!("Starting cluster state keeper!");
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Cluster state keeper shutting down!");
                break;
            }
            Some(message) = rx.recv() => {
                match message {
                    ClusterStateQuery::GetClusterState{tx} => {
                        tx.send(cluster.clone()).unwrap();
                    }
                    ClusterStateQuery::GetLeader {tx} => {
                        let leader = cluster.nodes.iter().find(|node| node.state == NodeState::Leader).cloned();
                        tx.send(leader).unwrap();
                    }
                    ClusterStateQuery::GetOtherNodes {tx} => {
                        let other_nodes_in_cluster = cluster.nodes.iter().filter(|node| !node.is_local).cloned().collect();
                        tx.send(other_nodes_in_cluster).unwrap();
                    }
                    ClusterStateQuery::UpdateNodeState {node_id, new_state, tx} => {
                        match cluster.nodes.iter_mut().find(|node| node.id == node_id) {
                            Some(node) => {
                                node.state = new_state;
                                tracing::info!("Node state updated successfully!");
                                tx.send(true).unwrap();
                            }
                            None => {
                                tracing::info!("Node not found in the cluster!");
                                tx.send(false).unwrap();
                            }
                        }
                    }
                    ClusterStateQuery::NominateLocalNodeAsLeader {tx} => {
                        match cluster.nodes.iter_mut().find(|node| node.is_local ) {
                            Some(node) => {
                                node.state = NodeState::Leader;
                                node.term += 1;
                                tracing::info!("Local node nominated as leader!");
                                tx.send(node.term).unwrap();
                            }
                            None => {
                                tracing::info!("Local node not found in the cluster!");
                                tx.send(0).unwrap();
                            }
                        }
                    }
                    ClusterStateQuery::GetLocalNode {tx} => {
                        let local_node = cluster.nodes.iter().find(|node| node.is_local).cloned().unwrap();
                        tx.send(local_node).unwrap();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::models::{Node, NodeState};

    use super::*;
    use tokio::sync::oneshot;
    use tracing_test::traced_test;
    use uuid::Uuid;

    #[tokio::test]
    #[traced_test]
    async fn test_cluster_state_keeper_should_be_able_to_update_the_leader() {
        let one_node = Node::new(Some("127.0.0.0".to_string()));
        let cluster = Cluster {
            nodes: vec![one_node.clone()],
        };
        let cancellation_token = CancellationToken::new();
        let (tx, rx) = mpsc::channel::<ClusterStateQuery>(2);
        tokio::spawn(async move {
            maintain_cluster_state(cluster, rx, cancellation_token).await;
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<bool>();
        let query = ClusterStateQuery::UpdateNodeState {
            node_id: one_node.id,
            new_state: NodeState::Leader,
            tx: oneshot_tx,
        };
        tx.send(query).await.unwrap();
        let result = oneshot_rx.await.unwrap();
        assert_eq!(result, true);

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<Option<Node>>();
        let query = ClusterStateQuery::GetLeader { tx: oneshot_tx };
        tx.send(query).await.unwrap();
        let leader_node = oneshot_rx.await.unwrap().unwrap();
        assert_eq!(leader_node.id, one_node.id);
        assert_eq!(leader_node.state, NodeState::Leader);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cluster_state_keeper_should_be_able_to_return_cluster_state() {
        let cluster = Cluster { nodes: vec![] };
        let cancellation_token = CancellationToken::new();
        let (tx, rx) = mpsc::channel::<ClusterStateQuery>(10);
        let cluster_clone = cluster.clone();
        tokio::spawn(async move {
            maintain_cluster_state(cluster_clone, rx, cancellation_token).await;
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<Cluster>();
        let query = ClusterStateQuery::GetClusterState { tx: oneshot_tx };
        tx.send(query).await.unwrap();

        let cluster_state = oneshot_rx.await.unwrap();
        assert_eq!(cluster_state, cluster);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cluster_state_keeper_should_return_none_when_cluster_has_no_leader() {
        let one_node = Node::new(Some("127.0.0.0".to_string()));
        let cluster = Cluster {
            nodes: vec![one_node.clone()],
        };
        let cancellation_token = CancellationToken::new();
        let (tx, rx) = mpsc::channel::<ClusterStateQuery>(10);
        let cluster_clone = cluster.clone();
        let cancellation_token_clone = cancellation_token.clone();
        tokio::spawn(async move {
            maintain_cluster_state(cluster_clone, rx, cancellation_token_clone).await;
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<Option<Node>>();
        let query = ClusterStateQuery::GetLeader { tx: oneshot_tx };
        tx.send(query).await.unwrap();

        let leader = oneshot_rx.await.unwrap();
        assert_eq!(leader, None);
        cancellation_token.cancel();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cluster_state_keeper_should_return_leader_node_details() {
        let leader = Node {
            id: Uuid::new_v4(),
            ip_address: "127.0.0.2".to_string(),
            state: NodeState::Leader,
            term: 0,
            is_local: true,
        };
        let cluster = Cluster {
            nodes: vec![leader.clone()],
        };
        let cancellation_token = CancellationToken::new();
        let (tx, rx) = mpsc::channel::<ClusterStateQuery>(10);
        let cancellation_token_clone = cancellation_token.clone();
        tokio::spawn(async move {
            maintain_cluster_state(cluster, rx, cancellation_token_clone).await;
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<Option<Node>>();
        let query = ClusterStateQuery::GetLeader { tx: oneshot_tx };
        tx.send(query).await.unwrap();

        let actual_leader = oneshot_rx.await.unwrap().unwrap();
        assert_eq!(actual_leader, leader);
        cancellation_token.cancel();
    }
}
