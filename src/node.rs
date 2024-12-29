use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::models::{NodeCommand, NodeQuery, NodeState, VoteResult};

#[derive(Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub address: String,
    pub state: NodeState,
    pub term: u64,
}

impl Node {
    pub async fn run(
        &mut self,
        interval_ms: u64,
        node_manager_port: u16,
        mut peers: Vec<String>,
        mut rx: mpsc::Receiver<NodeQuery>,
        cancellation_token: CancellationToken,
    ) {
        assert_ne!(peers.len(), 0, "At least one peer is required.");
        assert_ne!(interval_ms, 0, "Interval must be greater than 0");
        tracing::info!(
            "Node {} starting with heartbeat interval: {} milliseconds",
            self.id,
            interval_ms
        );

        let mut heartbeat_interval = interval(Duration::from_millis(interval_ms));
        // let socket = UdpSocket::bind(format!("0.0.0.0:{}", node_manager_port))
        //     .await
        //     .unwrap();

        let socket = UdpSocket::bind(format!("{}:{}", self.address, node_manager_port))
            .await
            .unwrap();

        let num_peers = u32::try_from(peers.len()).unwrap();
        let minimum_votes_needed: u32 = (num_peers / 2) + 1;
        tracing::debug!("Minimum votes needed: {}", minimum_votes_needed);

        let mut total_votes_received = 0;
        let mut nomination_accepted_count = 0;

        loop {
            let mut buffer = [0u8; 35];
            tokio::select! {
                recv_result = socket.recv_from(&mut buffer) => {
                    match recv_result {
                        Ok((bytes_read, peer_address)) => {
                            tracing::debug!("Received {} bytes from {}", bytes_read, peer_address);
                            let command: NodeCommand = bincode::deserialize(&buffer[..bytes_read]).unwrap();
                            match command {
                                NodeCommand::RequetForVote { candidate_id, term } => {
                                    tracing::info!("Node {} received vote request from {}", self.id, candidate_id);
                                    if term > self.term {
                                        self.term = term;
                                        self.state = NodeState::Follower;
                                    }
                                    let vote_result = if self.state == NodeState::Follower {
                                        VoteResult::Accepted
                                    } else {
                                        VoteResult::Rejected
                                    };
                                    let serialized_vote_result = bincode::serialize(&vote_result).unwrap();
                                    let _ = socket.send_to(&serialized_vote_result, peer_address).await;
                                }
                                NodeCommand::VoteResponse { voter_id, vote } => {
                                    tracing::info!("Node {} received vote response from {}", self.id, voter_id);
                                    if self.state == NodeState::Candidate {
                                        total_votes_received += 1;
                                        if vote == VoteResult::Accepted {
                                            nomination_accepted_count += 1;
                                        }
                                        if nomination_accepted_count >= minimum_votes_needed {
                                            tracing::info!("Node {} won the election", self.id);
                                            total_votes_received = 0;
                                            nomination_accepted_count = 0;
                                            self.state = NodeState::Leader;
                                        } else if total_votes_received >= num_peers {
                                            tracing::info!("Node {} lost the election", self.id);
                                            total_votes_received = 0;
                                            nomination_accepted_count = 0;
                                            self.state = NodeState::Follower;
                                            heartbeat_interval.reset();
                                        }
                                    } else {
                                        tracing::warn!("Node {} received vote response from {} but no election in progress", self.id, voter_id);
                                    }
                                }
                                NodeCommand::AddPeer { peer_address } => {
                                    tracing::info!("Node {} received request to add peer: {}", self.id, peer_address);
                                    peers.push(peer_address);
                                }
                                NodeCommand::RemovePeer { peer_address } => {
                                    tracing::info!("Node {} received request to remove peer: {}", self.id, peer_address);
                                    peers.retain(|p| p != &peer_address);
                                }
                                NodeCommand::Heartbeat { leader_id, term } => {
                                    tracing::info!("Node {} received heartbeat from leader: {}", self.id, leader_id);
                                    if term >= self.term {
                                        self.term = term;
                                        self.state = NodeState::Follower;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error receiving message: {:?}", e);
                        }
                    }
                },
                Some(msg) = rx.recv() => {
                    match msg {
                        NodeQuery::GetState { tx } => {
                            tx.send(self.state.clone()).unwrap();
                        }
                        NodeQuery::SetState { new_state, tx } => {
                            self.state = new_state;
                            tx.send(self.state.clone()).unwrap();
                        }
                        NodeQuery::GetPeers { tx } => {
                            tx.send(peers.clone()).unwrap();
                        }
                    }
                },
                _ = heartbeat_interval.tick() => {
                    match self.state {
                        NodeState::Follower => {
                            tracing::info!("Node: {}, state: {:?}", self.id, self.state);
                            self.become_candidate();
                        }
                        NodeState::Candidate => {
                            tracing::info!("Node {} is candidate, starting election with term: {}", self.id, self.term);
                            self.send_vote_request_to_peers(&peers, &socket).await;
                        }
                        NodeState::Leader => {
                            tracing::info!("Node {} is a leader.", self.id);
                            total_votes_received = 0;
                            nomination_accepted_count = 0;
                            self.send_heartbeat(&peers, &socket).await;
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Node {} shutting down", self.id);
                    break;
                }
            }
        }
    }

    async fn send_heartbeat(&self, peers: &Vec<String>, socket: &UdpSocket) {
        tracing::info!("Node {} sending heartbeat to peers.", self.id);
        let message_to_peers = bincode::serialize(&NodeCommand::Heartbeat {
            leader_id: self.id,
            term: self.term,
        })
        .unwrap();
        for peer in peers {
            let _ = socket.send_to(&message_to_peers, peer).await;
        }
    }

    async fn send_vote_request_to_peers(&mut self, peers: &Vec<String>, socket: &UdpSocket) {
        let vote_request = NodeCommand::RequetForVote {
            candidate_id: self.id,
            term: self.term,
        };
        let message_to_peers = bincode::serialize(&vote_request).unwrap();
        for peer in peers {
            tracing::info!("Sending vote request to peer: {}", peer);
            let _ = socket.send_to(&message_to_peers, peer).await;
        }
    }

    fn become_candidate(&mut self) {
        self.state = NodeState::Candidate;
        self.term += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use tokio::sync::oneshot;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_node_run() {
        let interval_ms: u64 = 500;
        let mut test_node = Node {
            id: Uuid::new_v4(),
            address: "".to_string(),
            state: NodeState::Follower,
            term: 0,
        };

        let node_manager_port = 5055;
        let (node_tx, rx) = mpsc::channel(10);
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.clone();
        let peers = vec!["peer_1".to_string(), "peer_2".to_string()];
        let handle = tokio::spawn(async move {
            test_node
                .run(interval_ms, node_manager_port, peers, rx, ct_clone)
                .await;
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeState>();
        node_tx
            .send(NodeQuery::GetState { tx: oneshot_tx })
            .await
            .unwrap();

        let state = oneshot_rx.await.unwrap();
        assert_eq!(state, NodeState::Follower);

        // Wait a bit to ensure the message has been processed
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeState>();
        node_tx
            .send(NodeQuery::GetState { tx: oneshot_tx })
            .await
            .unwrap();

        let state = oneshot_rx.await.unwrap();
        assert_eq!(state, NodeState::Candidate);

        cancellation_token.cancel();
        handle.abort();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_node_should_become_leader_if_enough_peers_accept_candidature() {
        let interval_ms: u64 = 250;
        let test_node_id = Uuid::new_v4();
        let mut test_node = Node {
            id: test_node_id.clone(),
            address: "0.0.0.0".to_string(),
            state: NodeState::Follower,
            term: 0,
        };

        let node_manager_port = 5056;
        let (tx, rx) = mpsc::channel(3);
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.clone();

        let peers = vec!["0.0.0.0:5057".to_string(), "0.0.0.0:5058".to_string()];
        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5057")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5058")).await.unwrap();

        let handle = tokio::spawn(async move {
            test_node
                .run(interval_ms, node_manager_port, peers, rx, ct_clone)
                .await;
        });

        thread::sleep(Duration::from_millis(100));

        let mut buffer = [0; 100];
        let vote_result = NodeCommand::VoteResponse {
            voter_id: Uuid::new_v4(),
            vote: VoteResult::Accepted,
        };

        let (bytes_count, peer_address) = peer_1_socket.recv_from(&mut buffer).await.unwrap();
        let message: NodeCommand = bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(
            message,
            NodeCommand::RequetForVote {
                candidate_id: test_node_id,
                term: 1
            }
        );

        let serialized_vote_result = bincode::serialize(&vote_result).unwrap();
        tracing::debug!("Sending vote result from peer_1");
        peer_1_socket
            .send_to(&serialized_vote_result, peer_address)
            .await
            .unwrap();

        let (bytes_count, peer_address) = peer_2_socket.recv_from(&mut buffer).await.unwrap();
        let message: NodeCommand = bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(
            message,
            NodeCommand::RequetForVote {
                candidate_id: test_node_id,
                term: 1
            }
        );

        peer_2_socket
            .send_to(&serialized_vote_result, peer_address)
            .await
            .unwrap();

        tracing::debug!("Peers have accepted the candidature");
        tokio::time::sleep(Duration::from_millis(interval_ms * 2)).await;

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeState>();
        let get_state_query = NodeQuery::GetState { tx: oneshot_tx };
        tx.send(get_state_query).await.unwrap();
        let result = oneshot_rx.await.unwrap();
        assert_eq!(result, NodeState::Leader);

        cancellation_token.cancel();
        handle.abort();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_node_should_become_follower_if_majority_peers_do_not_accept_as_a_leader() {
        let interval_ms: u64 = 250;
        let test_node_id = Uuid::new_v4();
        let mut test_node = Node {
            id: test_node_id.clone(),
            address: "0.0.0.0".to_string(),
            state: NodeState::Follower,
            term: 0,
        };

        let node_manager_port = 5059;
        let (tx, rx) = mpsc::channel(3);
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.clone();

        let peers = vec!["0.0.0.0:5060".to_string(), "0.0.0.0:5061".to_string()];
        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5060")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5061")).await.unwrap();

        let handle = tokio::spawn(async move {
            test_node
                .run(interval_ms, node_manager_port, peers, rx, ct_clone)
                .await;
        });

        let mut buffer = [0; 100];
        let reject_leader_command = NodeCommand::VoteResponse {
            voter_id: Uuid::new_v4(),
            vote: VoteResult::Rejected,
        };
        let reject_leader_message = bincode::serialize(&reject_leader_command).unwrap();

        let (bytes_count, peer_address) = peer_1_socket.recv_from(&mut buffer).await.unwrap();
        let message: NodeCommand = bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(
            message,
            NodeCommand::RequetForVote {
                candidate_id: test_node_id,
                term: 1
            }
        );

        peer_1_socket
            .send_to(&reject_leader_message, peer_address)
            .await
            .unwrap();

        let (bytes_count, candidate_address) = peer_2_socket.recv_from(&mut buffer).await.unwrap();
        let message_from_candidate: NodeCommand =
            bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(
            message_from_candidate,
            NodeCommand::RequetForVote {
                candidate_id: test_node_id,
                term: 1
            }
        );

        peer_2_socket
            .send_to(&reject_leader_message, candidate_address)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(interval_ms * 2)).await;

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeState>();
        let get_state_query = NodeQuery::GetState { tx: oneshot_tx };
        tx.send(get_state_query).await.unwrap();
        let result = oneshot_rx.await.unwrap();
        assert_eq!(result, NodeState::Candidate);

        cancellation_token.cancel();
        handle.abort();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_node_should_send_heartbeat_signal_to_peers() {
        let interval_ms: u64 = 250;
        let test_node_id = Uuid::new_v4();
        let mut test_node = Node {
            id: test_node_id.clone(),
            address: "0.0.0.0".to_string(),
            state: NodeState::Leader,
            term: 1,
        };
        let node_manager_port = 5065;
        let (_, rx) = mpsc::channel(1);
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.clone();

        let peers = vec!["0.0.0.0:5066".to_string(), "0.0.0.0:5067".to_string()];
        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5066")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5067")).await.unwrap();

        let handle = tokio::spawn(async move {
            test_node
                .run(interval_ms, node_manager_port, peers, rx, ct_clone)
                .await;
        });

        let expected_heartbeat = NodeCommand::Heartbeat {
            leader_id: test_node_id,
            term: 1,
        };
        let mut buffer = [0; 100];
        let (bytes_count, _) = peer_1_socket.recv_from(&mut buffer).await.unwrap();
        let actual_message: NodeCommand = bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(actual_message, expected_heartbeat);

        let mut buffer = [0; 100];
        let (bytes_count, _) = peer_2_socket.recv_from(&mut buffer).await.unwrap();
        let actual_message: NodeCommand = bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(actual_message, expected_heartbeat);

        cancellation_token.cancel();
        handle.abort();
    }
}
