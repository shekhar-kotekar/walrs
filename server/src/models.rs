use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VoteResult {
    Accepted,
    Rejected { reason: VoteRejectionReason },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VoteRejectionReason {
    LeaderAlreadyExists {
        leader_address: String,
        leader_heartbeat_interval: u64,
    },
    LowerTerm,
}

pub enum MainCommands {
    AddPeer {
        peer_address: String,
        tx: oneshot::Sender<NodeResponse>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartitionState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeCommand {
    RequetForVote {
        candidate_address: String,
        heartbeat_interval: u64,
        term: u64,
    },
    VoteResponse {
        voter_address: String,
        vote_result: VoteResult,
    },
    Heartbeat {
        leader_address: String,
    },
    RemovePeer {
        peer_address: String,
    },
}

#[derive(Debug, PartialEq)]
pub enum NodeResponse {
    PeerAdded,
}
