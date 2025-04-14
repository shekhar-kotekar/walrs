use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;

use crate::models::{
    MainCommands, NodeCommand, NodeResponse, NodeState, Topic, VoteRejectionReason, VoteResult,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Node {
    pub address: String,
    pub state: NodeState,
    pub term: u64,
    pub num_total_partitions: u32,
    sleep_interval: u64,
    total_votes_received: u32,
    nomination_accepted_count: u32,
    max_candidate_attempts: u32,
}

// TODO: Use type state pattern to manage the state of the node
// Reference: https://zerotomastery.io/blog/rust-typestate-patterns/
// https://www.youtube.com/watch?v=_ccDqRTx-JU
impl Node {
    pub fn new(address: String, sleep_interval: u64) -> Node {
        //TODO: Read the number of partitions from file on disk
        Node {
            address,
            state: NodeState::Follower,
            term: 0,
            num_total_partitions: 0,
            sleep_interval: sleep_interval,
            total_votes_received: 0,
            nomination_accepted_count: 0,
            max_candidate_attempts: 20,
        }
    }

    pub fn new_leader(address: String, sleep_interval: u64) -> Node {
        //TODO: Read the number of partitions from file on disk
        Node {
            address,
            state: NodeState::Leader,
            term: 0,
            num_total_partitions: 0,
            sleep_interval: sleep_interval,
            total_votes_received: 0,
            nomination_accepted_count: 0,
            max_candidate_attempts: 20,
        }
    }

    pub async fn run(
        &mut self,
        mut peers: Vec<Node>,
        mut main_rx: mpsc::Receiver<MainCommands>,
        cancellation_token: CancellationToken,
    ) {
        let mut num_peers = u32::try_from(peers.len()).unwrap();
        assert_ne!(
            num_peers, 0,
            "{}: At least one peer needed to run the node.",
            self.address
        );

        tracing::info!("Started running {:?}", self);
        let peer_addresses: String = peers
            .iter()
            .map(|p| p.address.clone())
            .collect::<Vec<String>>()
            .join(", ");
        tracing::info!("peers: {:?}", peer_addresses);

        let mut heartbeat_interval = interval(Duration::from_millis(self.sleep_interval));
        let node_socket = UdpSocket::bind(&self.address).await.unwrap();

        let mut leader_node: Option<Node> = None;
        let mut topics: HashMap<String, Topic> = HashMap::new();
        let mut last_heartbeat_from_leader = tokio::time::Instant::now();

        let min_votes_needed: u32 = (num_peers / 2) + 1;
        tracing::debug!("Minimum votes needed to be a leader: {}", min_votes_needed);

        loop {
            let mut buffer = [0u8; 150];
            tokio::select! {
                received_data = node_socket.recv_from(&mut buffer) => {
                    match received_data {
                        Ok((bytes_read, peer_address)) => {
                            let command: NodeCommand = bincode::deserialize(&buffer[..bytes_read]).unwrap();
                            match command {
                                NodeCommand::RequetForVote { candidate_address, heartbeat_interval, term } => {
                                    tracing::info!(
                                        "received vote request. candidate address: {}, term: {}",
                                        candidate_address,
                                        term
                                    );
                                    let vote_result: VoteResult = if self.state == NodeState::Leader {
                                        tracing::info!(
                                            "I am already a leader. Rejecting vote request from {}",
                                            candidate_address
                                        );
                                        VoteResult::Rejected {
                                            reason: VoteRejectionReason::LeaderAlreadyExists {
                                                leader_address: self.address.clone(),
                                                leader_heartbeat_interval: self.sleep_interval,
                                            },
                                        }
                                    } else if let Some(leader) = leader_node.as_ref() {
                                        tracing::info!("Cluster already has a leader. Rejecting vote request.");
                                        VoteResult::Rejected {
                                            reason: VoteRejectionReason::LeaderAlreadyExists {
                                                leader_address: leader.address.clone(),
                                                leader_heartbeat_interval: leader.sleep_interval,
                                            },
                                        }
                                    } else {
                                        if term >= self.term {
                                            tracing::info!("** NEW LEADER ACCEPTED **: {} is a new leader with the term: {}", candidate_address, term);
                                            leader_node = Some(Node::new_leader(candidate_address.clone(), heartbeat_interval));
                                            self.state = NodeState::Follower;
                                            VoteResult::Accepted
                                        } else {
                                            tracing::info!(
                                                "Candidate {} has a lower term {} than my term {}. Rejecting its vote request.",
                                                candidate_address,
                                                term,
                                                self.term
                                            );
                                            VoteResult::Rejected {
                                                reason: VoteRejectionReason::LowerTerm,
                                            }
                                        }
                                    };

                                    let voter_response = NodeCommand::VoteResponse {
                                        voter_address: self.address.clone(),
                                        vote_result,
                                    };
                                    let serialized_vote_result = bincode::serialize(&voter_response).unwrap();
                                    let _ = node_socket
                                        .send_to(&serialized_vote_result, peer_address)
                                        .await;
                                }
                                NodeCommand::VoteResponse { voter_address, vote_result } => {
                                    self.total_votes_received += 1;
                                    match vote_result {
                                        VoteResult::Accepted => {
                                            tracing::info!("Vote accepted from {}", voter_address);
                                            self.nomination_accepted_count += 1;
                                        }
                                        VoteResult::Rejected { reason } => {
                                            tracing::info!("Vote rejected from {}. Reason: {:?}", voter_address, reason);
                                            match reason {
                                                VoteRejectionReason::LeaderAlreadyExists { leader_address, leader_heartbeat_interval } => {
                                                    tracing::info!("Cluster already has a leader: {}", leader_address);
                                                    leader_node = Some(Node::new_leader(leader_address, leader_heartbeat_interval));
                                                }
                                                VoteRejectionReason::LowerTerm => {
                                                    self.term = self.term + 1;
                                                }
                                            }
                                        }
                                    }
                                }
                                NodeCommand::AddPeer { peer } => {
                                    tracing::info!("Adding {} to known peers.", &peer.address);
                                    peers.push(peer);
                                    num_peers += 1;
                                }
                                NodeCommand::RemovePeer { peer_address } => {
                                    peers.retain(|p| p.address != peer_address);
                                    num_peers -= 1;
                                    tracing::info!("Peer removed from known peers: {}", peer_address);
                                }
                                NodeCommand::Heartbeat { leader_address } => {
                                    tracing::info!("received heartbeat from leader: {}", leader_address);
                                    last_heartbeat_from_leader= tokio::time::Instant::now();
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
                        NodeState::Follower => {
                            tokio::time::sleep(Duration::from_millis(self.sleep_interval)).await;
                            if leader_node.is_none() {
                                tracing::info!("This node :{}, state: {:?} and no leader. Will become candidate", self.address, self.state);
                                self.become_candidate();
                            } else {
                                let leader_details = leader_node.as_ref().unwrap();
                                tracing::info!("This node {} is a follower and has a leader: {}", self.address, leader_details.address);
                                tracing::debug!("last heartbeat from leader: {:?}, elapsed time since last heartbeat from leader: {:?}, leader sleep interval: {}",
                                    last_heartbeat_from_leader,
                                    last_heartbeat_from_leader.elapsed(),
                                    leader_details.sleep_interval);

                                if last_heartbeat_from_leader.elapsed() > Duration::from_millis(leader_details.sleep_interval * 2) {
                                    tracing::info!("Leader {} hasn't sent heartbeat in a while. I am becoming a candidate.", leader_details.address);
                                    leader_node = None;
                                    self.become_candidate();
                                }
                            }
                        }
                        NodeState::Candidate => {
                            tracing::info!("I am still a candidate. Address: {}, term: {}", self.address, self.term);
                            if self.nomination_accepted_count >= min_votes_needed {
                                self.state = NodeState::Leader;
                                tracing::info!("Got majority votes: {}. I am a leader now. Address: {}", self.nomination_accepted_count, self.address);
                                leader_node = Some(Node::new_leader(self.address.clone(), self.sleep_interval));
                            } else {

                                if self.max_candidate_attempts == 0 {
                                    tracing::warn!("I {} did not receive votes. Becoming follower.", self.address);
                                    self.state = NodeState::Follower;
                                    self.max_candidate_attempts = 20;
                                } else {
                                    self.send_vote_request_to_peers(&peers, &node_socket).await;
                                    self.max_candidate_attempts -= 1;
                                }
                            }
                        }
                        NodeState::Leader => {
                            self.total_votes_received = 0;
                            self.nomination_accepted_count = 0;
                            self.send_heartbeat(&peers, &node_socket).await;
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Node {} shutting down", self.address);
                    break;
                }
            }
        }
    }

    async fn send_heartbeat(&self, peers: &Vec<Node>, socket: &UdpSocket) {
        tracing::info!(
            "I am a leader, term: {}. Sending heartbeat to peers.",
            self.term
        );
        let message_to_peers = bincode::serialize(&NodeCommand::Heartbeat {
            leader_address: self.address.clone(),
        })
        .unwrap();
        for peer in peers {
            let _ = socket.send_to(&message_to_peers, &peer.address).await;
        }
    }

    async fn send_vote_request_to_peers(&mut self, peers: &Vec<Node>, socket: &UdpSocket) {
        let vote_request = NodeCommand::RequetForVote {
            candidate_address: self.address.clone(),
            heartbeat_interval: self.sleep_interval,
            term: self.term,
        };
        let message_to_peers = bincode::serialize(&vote_request).unwrap();
        for peer in peers {
            let _ = socket.send_to(&message_to_peers, &peer.address).await;
            tracing::info!("Vote request sent to peer: {}", peer.address);
        }
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
        let sleep_interval: u64 = 20;
        let mut test_node = Node::new("0.0.0.0:5075".to_string(), sleep_interval);
        let (main_tx, main_rx) = mpsc::channel(1);
        let cancellation_token = CancellationToken::new();
        let node_ct = cancellation_token.child_token();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("peer_1".to_string(), sleep_interval),
                Node::new("peer_2".to_string(), sleep_interval),
            ];
            test_node.run(peers, main_rx, node_ct).await;
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
        let sleep_interval: u64 = 20;
        let mut test_node = Node::new("0.0.0.0:5055".to_string(), sleep_interval);

        let (node_tx, rx) = mpsc::channel(10);
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.child_token();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("peer_1".to_string(), sleep_interval),
                Node::new("peer_2".to_string(), sleep_interval),
            ];
            test_node.run(peers, rx, ct_clone).await;
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
        let sleep_interval: u64 = 20;
        let mut test_node = Node::new("0.0.0.0:5056".to_string(), sleep_interval);

        let (tx, rx) = mpsc::channel(3);
        let cancellation_token = CancellationToken::new();
        let node_ct = cancellation_token.child_token();

        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5057")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5058")).await.unwrap();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("0.0.0.0:5057".to_string(), sleep_interval),
                Node::new("0.0.0.0:5058".to_string(), sleep_interval),
            ];
            test_node.run(peers, rx, node_ct).await;
        });

        thread::sleep(Duration::from_millis(50));

        let mut buffer = [0; 100];
        let vote_result = NodeCommand::VoteResponse {
            voter_address: "0.0.0.0:5057".to_string(),
            vote_result: VoteResult::Accepted,
        };

        let (bytes_count, peer_address) = peer_1_socket.recv_from(&mut buffer).await.unwrap();
        let message: NodeCommand = bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(
            message,
            NodeCommand::RequetForVote {
                candidate_address: "0.0.0.0:5056".to_string(),
                heartbeat_interval: sleep_interval,
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
                candidate_address: "0.0.0.0:5056".to_string(),
                heartbeat_interval: sleep_interval,
                term: 1
            }
        );

        peer_2_socket
            .send_to(&serialized_vote_result, peer_address)
            .await
            .unwrap();

        tracing::debug!("Peers have accepted the candidature");
        tokio::time::sleep(Duration::from_millis(sleep_interval * 2)).await;

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
        let sleep_interval: u64 = 20;
        let test_node_address = "0.0.0.0:5059".to_string();
        let mut test_node = Node::new(test_node_address.clone(), sleep_interval);

        let (tx, rx) = mpsc::channel(3);
        let cancellation_token = CancellationToken::new();
        let node_ct = cancellation_token.child_token();

        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5060")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5061")).await.unwrap();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("0.0.0.0:5060".to_string(), sleep_interval),
                Node::new("0.0.0.0:5061".to_string(), sleep_interval),
            ];
            test_node.run(peers, rx, node_ct).await;
        });

        let mut buffer = [0; 100];
        let reject_leader_command = NodeCommand::VoteResponse {
            voter_address: "0.0.0.0:5060".to_string(),
            vote_result: VoteResult::Rejected {
                reason: VoteRejectionReason::LowerTerm,
            },
        };
        let reject_leader_message = bincode::serialize(&reject_leader_command).unwrap();

        let (bytes_count, peer_address) = peer_1_socket.recv_from(&mut buffer).await.unwrap();
        let message: NodeCommand = bincode::deserialize(&buffer[..bytes_count]).unwrap();
        let expected_message_from_candidate = NodeCommand::RequetForVote {
            candidate_address: test_node_address.clone(),
            heartbeat_interval: sleep_interval,
            term: 1,
        };
        assert_eq!(message, expected_message_from_candidate);

        peer_1_socket
            .send_to(&reject_leader_message, peer_address)
            .await
            .unwrap();

        let (bytes_count, candidate_address) = peer_2_socket.recv_from(&mut buffer).await.unwrap();
        let message_from_candidate: NodeCommand =
            bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(message_from_candidate, expected_message_from_candidate);
        let reject_leader_command = NodeCommand::VoteResponse {
            voter_address: "0.0.0.0:5061".to_string(),
            vote_result: VoteResult::Rejected {
                reason: VoteRejectionReason::LowerTerm,
            },
        };
        let reject_leader_message = bincode::serialize(&reject_leader_command).unwrap();
        peer_2_socket
            .send_to(&reject_leader_message, candidate_address)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100 * 2)).await;

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
        let sleep_interval: u64 = 20;
        let mut leader_node = Node::new_leader("0.0.0.0:5065".to_string(), sleep_interval);
        let (_, rx) = mpsc::channel(1);
        let cancellation_token = CancellationToken::new();
        let node_ct = cancellation_token.child_token();

        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5066")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5067")).await.unwrap();

        let handle = tokio::spawn(async move {
            let peers = vec![
                Node::new("0.0.0.0:5066".to_string(), sleep_interval),
                Node::new("0.0.0.0:5067".to_string(), sleep_interval),
            ];
            leader_node.run(peers, rx, node_ct).await;
        });

        let expected_heartbeat = NodeCommand::Heartbeat {
            leader_address: "0.0.0.0:5065".to_string(),
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
