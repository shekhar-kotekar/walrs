use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage {
    VoteRequest(Node),
    VoteResponse { node: Node, answer: Answer },
    LeaderElected(Node),
    Heartbeat(Node),
}

impl From<Vec<u8>> for ClusterMessage {
    fn from(bytes: Vec<u8>) -> Self {
        bincode::deserialize(&bytes).unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Answer {
    LeaderAccepted,
    LeaderRejected,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    Leader,
    Follower,
    Candidate,
    InterimLeader,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Cluster {
    pub nodes: Vec<Node>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: Option<Uuid>,
    pub ip_address: String,
    pub state: NodeState,
    pub term: u64,
    pub is_local: bool,
}

impl Node {
    pub fn new(ip_address: Option<String>) -> Self {
        Node {
            id: None,
            state: NodeState::Follower,
            ip_address: ip_address.unwrap_or_default(),
            term: 0,
            is_local: false,
        }
    }
}

#[derive(Debug)]
pub enum ClusterStateQuery {
    GetClusterState {
        tx: oneshot::Sender<Cluster>,
    },
    GetLeader {
        tx: oneshot::Sender<Option<Node>>,
    },
    GetLocalNode {
        tx: oneshot::Sender<Node>,
    },
    GetOtherNodes {
        tx: oneshot::Sender<Vec<Node>>,
    },
    UpdateNode {
        node_details: Node,
        tx: oneshot::Sender<bool>,
    },
    NominateLocalNodeAsLeader {
        tx: oneshot::Sender<Option<Node>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_new() {
        let node = Node::new(None);
        assert_eq!(node.state, NodeState::Follower);
        assert_eq!(node.ip_address, "");
        assert_eq!(node.term, 0);
    }
}
