use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::models::{MainCommands, NodeCommand, NodeResponse, NodeState, Topic, VoteResult};

const MIN_HEARTBEAT_INTERVAL_MS: u64 = 10;
const MAX_HEARTBEAT_INTERVAL_MS: u64 = 10000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Node {
    pub id: Uuid,
    pub address: String,
    pub state: NodeState,
    pub term: u64,
    pub num_total_partitions: u32,
}

// TODO: Use type state pattern to manage the state of the node
// Reference: https://zerotomastery.io/blog/rust-typestate-patterns/
// https://www.youtube.com/watch?v=_ccDqRTx-JU
impl Node {
    pub fn new(address: String) -> Node {
        //TODO: Read the number of partitions from file on disk
        Node {
            id: Uuid::new_v4(),
            address,
            state: NodeState::Follower,
            term: 0,
            num_total_partitions: 0,
        }
    }
    pub async fn run(
        &mut self,
        interval_ms: u64,
        mut peers: Vec<Node>,
        mut main_rx: mpsc::Receiver<MainCommands>,
        cancellation_token: CancellationToken,
    ) {
        let mut num_peers = u32::try_from(peers.len()).unwrap();
        assert_ne!(
            num_peers, 0,
            "{}: At least one peer needed to start node.",
            self.id
        );
        assert_ne!(
            interval_ms, 0,
            "{}: Interval must be greater than 0",
            self.id
        );
        assert!(
            interval_ms <= MAX_HEARTBEAT_INTERVAL_MS,
            "{}: Heartbeat Interval must be less than 10 seconds",
            self.id
        );
        assert!(
            interval_ms >= MIN_HEARTBEAT_INTERVAL_MS,
            "{}: Heartbeat Interval must be greater than 100 millis",
            self.id
        );

        tracing::info!(
            "Starting node {}, heartbeat interval: {} millis, address : {}",
            self.id,
            interval_ms,
            self.address
        );

        let mut heartbeat_interval = interval(Duration::from_millis(interval_ms));
        let node_socket = UdpSocket::bind(&self.address).await.unwrap();

        let mut total_votes_received = 0;
        let mut nomination_accepted_count = 0;
        let mut max_candidate_attempts = 10;
        let mut leader_node: Option<Node> = None;
        let mut topics: HashMap<String, Topic> = HashMap::new();

        let min_votes_needed: u32 = (num_peers / 2) + 1;
        tracing::debug!("Minimum votes needed to be a leader: {}", min_votes_needed);

        loop {
            let mut buffer = [0u8; 48];
            tokio::select! {
                recv_result = node_socket.recv_from(&mut buffer) => {
                    match recv_result {
                        Ok((bytes_read, peer_address)) => {
                            let command: NodeCommand = bincode::deserialize(&buffer[..bytes_read]).unwrap();
                            let peer_ip = peer_address.ip().to_string();
                            match command {
                                NodeCommand::RequetForVote { candidate_id, term } => {
                                    tracing::info!("received vote request from {}", peer_ip);
                                    if term > self.term {
                                        self.term = term;
                                        self.state = NodeState::Follower;
                                    }
                                    let vote_result = if self.state != NodeState::Leader && leader_node.is_none() {
                                        tracing::info!("accepted {} as leader.", peer_ip);

                                        leader_node = Some(Node {
                                            id:candidate_id,
                                            address:peer_ip,
                                            state:NodeState::Leader,
                                            term,
                                            num_total_partitions: 0,
                                        });
                                        VoteResult::Accepted
                                    } else {
                                        tracing::info!("rejected {} as leader.", peer_ip);
                                        VoteResult::Rejected
                                    };
                                    let response = NodeCommand::VoteResponse {
                                        voter_id: self.id,
                                        vote: vote_result,
                                    };
                                    let serialized_vote_result = bincode::serialize(&response).unwrap();
                                    let _ = node_socket.send_to(&serialized_vote_result, peer_address).await;
                                }
                                NodeCommand::VoteResponse { voter_id, vote } => {
                                    tracing::info!("received vote response from {}", voter_id);
                                    if self.state == NodeState::Candidate {
                                        total_votes_received += 1;
                                        if vote == VoteResult::Accepted {
                                            nomination_accepted_count += 1;
                                        }
                                        if nomination_accepted_count >= min_votes_needed {
                                            tracing::info!("This node {} won the election", self.address);
                                            total_votes_received = 0;
                                            nomination_accepted_count = 0;
                                            self.state = NodeState::Leader;
                                        } else if total_votes_received >= num_peers {
                                            tracing::info!("This node {} lost the election", self.address);
                                            total_votes_received = 0;
                                            nomination_accepted_count = 0;
                                            self.state = NodeState::Follower;
                                            sleep(Duration::from_millis(interval_ms)).await;
                                        }
                                    } else {
                                        tracing::warn!("received vote response from {} but no election in progress", voter_id);
                                    }
                                }
                                NodeCommand::AddPeer { peer } => {
                                    tracing::info!("Adding {} to known peers.", &peer.address);
                                    peers.push(peer);
                                    num_peers += 1;
                                }
                                NodeCommand::RemovePeer { peer_id } => {
                                    peers.retain(|p| p.id != peer_id && p.address != peer_address.ip().to_string());
                                    num_peers -= 1;
                                    tracing::info!("Peer removed from known peers: {}", peer_address);
                                }
                                NodeCommand::Heartbeat { leader_id, term } => {
                                    tracing::info!("received heartbeat from leader: {}", leader_id);
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
                Some(msg) = main_rx.recv() => {
                    match msg {
                        MainCommands::GetState { tx } => {
                            tx.send(self.state.clone()).unwrap();
                        }
                        MainCommands::SetState { new_state, tx } => {
                            self.state = new_state;
                            tx.send(self.state.clone()).unwrap();
                        }
                        MainCommands::GetPeers { tx } => {
                            tx.send(peers.clone()).unwrap();
                        }
                        MainCommands::CreateTopic { topic, tx } => {
                            if topics.contains_key(&topic.name) {
                                tx.send(NodeResponse::TopicAlreadyExists).unwrap();
                            } else {
                                topics.insert(topic.name.clone(), topic);
                                //TODO: Find a leader node for the topic
                                // Try to distribute partitions equally among nodes
                                // Create lead partition
                                // Create follower partitions
                                // Send topic created response to client
                                tx.send(NodeResponse::TopicCreated { leader_address: self.address.clone() }).unwrap();
                            }
                        }
                    }
                },
                _ = heartbeat_interval.tick() => {
                    match self.state {
                        NodeState::Follower => if leader_node.is_none() {
                            tracing::info!("Node {} is {:?}", self.id, self.state);
                            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                            self.become_candidate();
                        }
                        NodeState::Candidate => if leader_node.is_none() {
                            tracing::info!("Node {} is candidate, term: {}", self.id, self.term);
                            if max_candidate_attempts == 0 {
                                tracing::warn!("Node {} did not receive votes. Becoming follower.", self.id);
                                self.state = NodeState::Follower;
                                max_candidate_attempts = 20;
                            } else {
                                self.send_vote_request_to_peers(&peers, &node_socket).await;
                                max_candidate_attempts -= 1;
                            }
                        }
                        NodeState::Leader => {
                            tracing::info!("Node {} is a leader.", self.id);
                            total_votes_received = 0;
                            nomination_accepted_count = 0;
                            self.send_heartbeat(&peers, &node_socket).await;
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

    async fn send_heartbeat(&self, peers: &Vec<Node>, socket: &UdpSocket) {
        tracing::info!("sending heartbeat to peers.");
        let message_to_peers = bincode::serialize(&NodeCommand::Heartbeat {
            leader_id: self.id,
            term: self.term,
        })
        .unwrap();
        for peer in peers {
            let _ = socket.send_to(&message_to_peers, &peer.address).await;
        }
    }

    async fn send_vote_request_to_peers(&mut self, peers: &Vec<Node>, socket: &UdpSocket) {
        let vote_request = NodeCommand::RequetForVote {
            candidate_id: self.id,
            term: self.term,
        };
        let message_to_peers = bincode::serialize(&vote_request).unwrap();
        for peer in peers {
            let _ = socket.send_to(&message_to_peers, &peer.address).await;
            tracing::info!("Vote request sent to peer: {}", peer.address);
        }
        tracing::info!("Vote requests sent to all peers.");
    }

    fn become_candidate(&mut self) {
        self.state = NodeState::Candidate;
        self.term += 1;
    }
}

#[cfg(test)]
mod should {
    use std::thread;

    use crate::models::{NodeResponse, Topic};

    use super::*;
    use tokio::sync::oneshot;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn create_a_new_topic() {
        let mut test_node = Node::new("0.0.0.0:5075".to_string());
        let (main_tx, main_rx) = mpsc::channel(1);
        let cancellation_token = CancellationToken::new();
        let node_ct = cancellation_token.child_token();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("peer_1".to_string()),
                Node::new("peer_2".to_string()),
            ];
            test_node.run(10, peers, main_rx, node_ct).await;
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeResponse>();
        let create_topic_command: MainCommands = MainCommands::CreateTopic {
            topic: Topic::new("test_topic".to_string()),
            tx: oneshot_tx,
        };
        main_tx.send(create_topic_command).await.unwrap();
        match oneshot_rx.await.unwrap() {
            NodeResponse::TopicCreated { leader_address } => {
                assert_eq!(leader_address, "0.0.0.0:5075".to_string());
            }
            _ => panic!("Invalid response for create topic command"),
        }
        cancellation_token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_node_run() {
        let interval_ms: u64 = 10;
        let mut test_node = Node::new("0.0.0.0:5055".to_string());

        let (node_tx, rx) = mpsc::channel(10);
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.child_token();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("peer_1".to_string()),
                Node::new("peer_2".to_string()),
            ];
            test_node.run(interval_ms, peers, rx, ct_clone).await;
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeState>();
        node_tx
            .send(MainCommands::GetState { tx: oneshot_tx })
            .await
            .unwrap();

        let state = oneshot_rx.await.unwrap();
        assert_eq!(state, NodeState::Follower);

        // Wait a bit to ensure the message has been processed
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeState>();
        node_tx
            .send(MainCommands::GetState { tx: oneshot_tx })
            .await
            .unwrap();

        let state = oneshot_rx.await.unwrap();
        assert_eq!(state, NodeState::Candidate);

        cancellation_token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn become_leader_if_enough_peers_accept_candidature() {
        let interval_ms: u64 = 20;
        let mut test_node = Node::new("0.0.0.0:5056".to_string());
        let test_node_id = test_node.id;

        let (tx, rx) = mpsc::channel(3);
        let cancellation_token = CancellationToken::new();
        let node_ct = cancellation_token.child_token();

        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5057")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5058")).await.unwrap();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("0.0.0.0:5057".to_string()),
                Node::new("0.0.0.0:5058".to_string()),
            ];
            test_node.run(interval_ms, peers, rx, node_ct).await;
        });

        thread::sleep(Duration::from_millis(50));

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
        let get_state_query = MainCommands::GetState { tx: oneshot_tx };
        tx.send(get_state_query).await.unwrap();
        let result = oneshot_rx.await.unwrap();
        assert_eq!(result, NodeState::Leader);

        cancellation_token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn become_follower_if_majority_peers_do_not_accept_as_a_leader() {
        let interval_ms: u64 = 20;
        let mut test_node = Node::new("0.0.0.0:5059".to_string());
        let node_id = test_node.id;

        let (tx, rx) = mpsc::channel(3);
        let cancellation_token = CancellationToken::new();
        let node_ct = cancellation_token.child_token();

        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5060")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5061")).await.unwrap();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("0.0.0.0:5060".to_string()),
                Node::new("0.0.0.0:5061".to_string()),
            ];
            test_node.run(interval_ms, peers, rx, node_ct).await;
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
                candidate_id: node_id,
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
                candidate_id: node_id,
                term: 1
            }
        );

        peer_2_socket
            .send_to(&reject_leader_message, candidate_address)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(interval_ms * 2)).await;

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeState>();
        let get_state_query = MainCommands::GetState { tx: oneshot_tx };
        tx.send(get_state_query).await.unwrap();
        let result = oneshot_rx.await.unwrap();
        assert_eq!(result, NodeState::Candidate);

        cancellation_token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn send_heartbeat_signal_to_peers() {
        let interval_ms: u64 = 20;
        let test_node_id = Uuid::new_v4();
        let mut test_node = Node {
            id: test_node_id,
            address: "0.0.0.0:5065".to_string(),
            state: NodeState::Leader,
            term: 1,
            num_total_partitions: 0,
        };
        let (_, rx) = mpsc::channel(1);
        let cancellation_token = CancellationToken::new();
        let node_ct = cancellation_token.child_token();

        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5066")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5067")).await.unwrap();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("0.0.0.0:5066".to_string()),
                Node::new("0.0.0.0:5067".to_string()),
            ];
            test_node.run(interval_ms, peers, rx, node_ct).await;
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
        handle.await.unwrap();
    }
}
