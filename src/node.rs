use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub address: String,
    pub state: NodeState,
    pub term: u64,
    pub election_timeout: u64,
}

impl Node {
    pub async fn run(
        &mut self,
        interval_ms: u64,
        node_manager_port: u16,
        peers: Vec<&str>,
        mut rx: mpsc::Receiver<NodeQuery>,
        cancellation_token: CancellationToken,
    ) {
        assert_ne!(peers.len(), 0, "At least one peer is required.");
        assert_ne!(interval_ms, 0, "Interval must be greater than 0");

        let mut heartbeat_interval = interval(Duration::from_millis(interval_ms));
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", node_manager_port))
            .await
            .unwrap();
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    match msg {
                        NodeQuery::GetState { tx } => {
                            tx.send(self.state.clone()).unwrap();
                        }
                        NodeQuery::SetState { new_state, tx } => {
                            self.state = new_state;
                            tx.send(self.state.clone()).unwrap();
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
                            tracing::info!("Node {} is candidate, starting election with term number: {}", self.id, self.term);
                            self.send_vote_request_to_peers(peers.clone(), &socket).await;

                            match self.start_election(peers.clone(), &socket).await {
                                ElectionResult::Won => {
                                    tracing::info!("Node {} won the election", self.id);
                                    self.state = NodeState::Leader;
                                }
                                ElectionResult::Lost => {
                                    tracing::info!("Node {} lost the election", self.id);
                                    self.state = NodeState::Follower;
                                }
                                ElectionResult::TimedOut => {
                                    tracing::info!("Node {} election timed out", self.id);
                                    self.state = NodeState::Follower;
                                }
                            }
                        }
                        NodeState::Leader => {
                            tracing::info!("Node {} is in leader state.", self.id);
                            self.send_heartbeat();
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Node {} is shutting down", self.id);
                    break;
                }
            }
        }
    }

    fn send_heartbeat(&self) {
        tracing::info!("Node {} is sending heartbeat.", self.id);
    }

    async fn start_election(&mut self, peers: Vec<&str>, socket: &UdpSocket) -> ElectionResult {
        let num_peers = peers.len();
        let minimum_votes_needed = num_peers / 2 + 1;
        let mut nomination_accepted_count = 0;
        let mut total_answers_received = 0;
        let mut timeout = interval(Duration::from_millis(self.election_timeout));
        loop {
            let mut buffer = [0; 200];
            tokio::select! {
                _ = timeout.tick() => {
                    tracing::info!("Total votes: {}, total attendance: {}", num_peers, total_answers_received);
                    tracing::info!("Accepted votes: {}", nomination_accepted_count);
                    if total_answers_received >= num_peers {
                        if nomination_accepted_count >= minimum_votes_needed {
                            return ElectionResult::Won;
                        } else {
                            return ElectionResult::Lost;
                        }
                    } else {
                        return ElectionResult::TimedOut;
                    }
                }
                received = socket.recv_from(&mut buffer) => {
                    match received {
                        Ok((_, peer)) => {
                            total_answers_received += 1;
                            let message: VoteResult = bincode::deserialize(&buffer).unwrap();
                            match message {
                                VoteResult::Accepted => {
                                    nomination_accepted_count += 1;
                                }
                                VoteResult::Rejected => {
                                    tracing::warn!("Node {} rejected nomination", peer.ip());
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error receiving message: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    async fn send_vote_request_to_peers(&mut self, peers: Vec<&str>, socket: &UdpSocket) {
        let vote_request = ElectionCommand::RequetForVote {
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

pub enum ElectionResult {
    Won,
    Lost,
    TimedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VoteResult {
    Accepted,
    Rejected,
}

pub enum NodeQuery {
    GetState {
        tx: oneshot::Sender<NodeState>,
    },
    SetState {
        new_state: NodeState,
        tx: oneshot::Sender<NodeState>,
    },
}

pub enum NodeResponse {
    State(NodeState),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ElectionCommand {
    RequetForVote { candidate_id: Uuid, term: u64 },
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
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
            election_timeout: interval_ms * 2,
        };

        let node_manager_port = 5055;
        let (node_tx, rx) = mpsc::channel(10);
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.clone();
        let peers = vec!["peer_1", "peer_2"];
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

        // Now, let's assume we want to stop the test after receiving our message and a few ticks
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
            election_timeout: interval_ms * 2,
        };

        let node_manager_port = 5056;
        let (tx, rx) = mpsc::channel(3);
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.clone();

        let peers = vec!["0.0.0.0:5057", "0.0.0.0:5058"];
        let peer_1_socket = UdpSocket::bind(format!("0.0.0.0:5057")).await.unwrap();
        let peer_2_socket = UdpSocket::bind(format!("0.0.0.0:5058")).await.unwrap();

        let handle = tokio::spawn(async move {
            test_node
                .run(interval_ms, node_manager_port, peers, rx, ct_clone)
                .await;
        });

        thread::sleep(Duration::from_millis(100));

        let mut buffer = [0; 200];
        let vote_result = VoteResult::Accepted;

        let (bytes_count, peer_address) = peer_1_socket.recv_from(&mut buffer).await.unwrap();
        let message: ElectionCommand = bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(
            message,
            ElectionCommand::RequetForVote {
                candidate_id: test_node_id,
                term: 1
            }
        );

        let serialized_vote_result = bincode::serialize(&vote_result).unwrap();
        peer_1_socket
            .send_to(&serialized_vote_result, peer_address)
            .await
            .unwrap();

        let (bytes_count, peer_address) = peer_2_socket.recv_from(&mut buffer).await.unwrap();
        let message: ElectionCommand = bincode::deserialize(&buffer[..bytes_count]).unwrap();
        assert_eq!(
            message,
            ElectionCommand::RequetForVote {
                candidate_id: test_node_id,
                term: 1
            }
        );

        let serialized_vote_result = bincode::serialize(&vote_result).unwrap();
        peer_2_socket
            .send_to(&serialized_vote_result, peer_address)
            .await
            .unwrap();

        tracing::info!("Peers have accepted the candidature");
        tokio::time::sleep(Duration::from_millis(interval_ms * 2)).await;

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<NodeState>();
        let get_state_query = NodeQuery::GetState { tx: oneshot_tx };
        tx.send(get_state_query).await.unwrap();
        let result = oneshot_rx.await.unwrap();
        assert_eq!(result, NodeState::Leader);

        cancellation_token.cancel();
        handle.abort();
    }
}
