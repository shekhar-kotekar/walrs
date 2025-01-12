use chrono::Duration;
use common::models::MessageBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use uuid::Uuid;

pub enum PartitionCommand {
    Write {
        batch: MessageBatch,
        response_tx: oneshot::Sender<PartitionResponse>,
    },
}

#[derive(Debug, PartialEq)]
pub enum PartitionResponse {
    LeaderAcknowledged,
    MajorityAcknowledged,
    AllAcknowledged,
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VoteResult {
    Accepted,
    Rejected,
}

pub enum MainCommands {
    GetState {
        tx: oneshot::Sender<NodeState>,
    },
    SetState {
        new_state: NodeState,
        tx: oneshot::Sender<NodeState>,
    },
    GetPeers {
        tx: oneshot::Sender<Vec<Node>>,
    },
    CreateTopic {
        topic: Topic,
        tx: oneshot::Sender<NodeResponse>,
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
    AddPeer { peer: Node },
    RemovePeer { peer_id: Uuid },
}

#[derive(Debug)]
pub enum NodeResponse {
    TopicCreated { leader_address: String },
    TopicAlreadyExists,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub address: String,
    pub state: NodeState,
    pub term: u64,
    pub num_total_partitions: u32,
}

const TOPIC_DEFAULT_NUM_PARTITIONS: u8 = 3;
const TOPIC_DEFAULT_REPLICATION_FACTOR: u8 = 2;
const TOPIC_DEFAULT_RETENTION_PERIOD_HOURS: Duration = Duration::hours(24 * 7);
pub struct Topic {
    pub name: String,
    id: Uuid,
    num_partitions: u8,
    replication_factor: u8,
    retention_period_hours: Duration,
    leader_address: Option<String>,
}

impl Topic {
    pub fn new(name: String) -> Self {
        Topic {
            name,
            id: Uuid::new_v4(),
            num_partitions: TOPIC_DEFAULT_NUM_PARTITIONS,
            replication_factor: TOPIC_DEFAULT_REPLICATION_FACTOR,
            retention_period_hours: TOPIC_DEFAULT_RETENTION_PERIOD_HOURS,
            leader_address: None,
        }
    }
}
