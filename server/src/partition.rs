use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::models::{MainCommands, NodeResponse, PartitionState};

pub struct Partition {
    address: String,
    state: PartitionState,
    term: u64,
    total_votes_received: u32,
    nomination_accepted_count: u32,
    max_candidate_attempts: u32,
}

impl Partition {
    pub fn new_follower(address: String) -> Self {
        Partition {
            address,
            state: PartitionState::Follower,
            term: 0,
            total_votes_received: 0,
            nomination_accepted_count: 0,
            max_candidate_attempts: 5,
        }
    }

    pub async fn run(
        &self,
        mut main_rx: mpsc::Receiver<MainCommands>,
        sleep_interval_ms: u64,
        cancellation_token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(sleep_interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tracing::debug!("{}: Node is running", self.address);
        let mut peers: Vec<String> = vec![];
        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        MainCommands::AddPeer { peer_address, tx } => {
                            tracing::debug!("Received new peer: {:?}", peer_address);
                            peers.push(peer_address);
                            let _ = tx.send(NodeResponse::PeerAdded);
                        }
                    }
                }
                _ = interval.tick() => {
                    tracing::debug!("{}: Node is alive", self.address);
                    match self.state {
                        PartitionState::Follower => {
                            self.manage_follower_state().await;
                        }
                        PartitionState::Candidate => {
                            self.manage_candidate_state().await;
                        }
                        PartitionState::Leader => {
                            self.manage_leader_state().await;
                        }

                    }
                }
                _ = cancellation_token.cancelled() => {
                    tracing::debug!("{}: Node is shutting down", self.address);
                    break;
                }
            }
        }
    }

    async fn manage_follower_state(&self) {
        tracing::debug!("{}: Node is a Follower", self.address);
    }

    async fn manage_leader_state(&self) {
        tracing::debug!("{}: Node is a Leader", self.address);
    }

    async fn manage_candidate_state(&self) {
        tracing::debug!("{}: Node is a Candidate", self.address);
    }
}

#[cfg(test)]
mod should {
    use tracing_test::traced_test;

    use super::*;

    #[tokio::test]
    #[traced_test]
    async fn be_able_to_create_follower_node() {
        let follower_node = Partition::new_follower("0.0.0.0:5056".to_string());
        assert!(follower_node.state == PartitionState::Follower);
        assert!(follower_node.term == 0);
        assert!(follower_node.total_votes_received == 0);
        assert!(follower_node.nomination_accepted_count == 0);
        assert!(follower_node.max_candidate_attempts == 5);
    }
}
