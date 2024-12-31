use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use uuid::Uuid;

pub enum ClientCommand {
    CreateTopic {
        topic_name: String,
        num_partitions: u8,
        retention_period: u16,
    },
    DeleteTopic {
        topic_name: String,
    },
}

pub enum ClusterResponse {
    TopicCreated {
        topic_id: Uuid,
        leader_address: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
    GetPeers {
        tx: oneshot::Sender<Vec<String>>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeCommand {
    RequetForVote { candidate_id: Uuid, term: u64 },
    VoteResponse { voter_id: Uuid, vote: VoteResult },
    Heartbeat { leader_id: Uuid, term: u64 },
    AddPeer { peer_address: String },
    RemovePeer { peer_address: String },
}
