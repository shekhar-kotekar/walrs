use serde::{Deserialize, Serialize};
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
    pub current_node: Node,
    pub other_nodes: Vec<Node>,
    pub lealder: Option<Node>,
}

impl Cluster {
    pub fn new() -> Self {
        Cluster {
            current_node: Node::new(None),
            other_nodes: vec![],
            lealder: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub ip_address: String,
    pub state: NodeState,
    pub term: u64,
}

impl Node {
    pub fn new(ip_address: Option<String>) -> Self {
        Node {
            id: Uuid::new_v4(),
            state: NodeState::Follower,
            ip_address: ip_address.unwrap_or_else(|| "".to_string()),
            term: 0,
        }
    }
    pub fn next_candidate(&mut self) {
        self.state = NodeState::Candidate;
        self.term += 1;
    }
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

    #[test]
    fn test_cluster_new() {
        let cluster = Cluster {
            current_node: Node::new(None),
            other_nodes: vec![],
            lealder: None,
        };
        assert_eq!(cluster.other_nodes.len(), 0);
        assert_eq!(cluster.lealder, None);
    }
}
