#[derive(Debug, PartialEq)]
pub enum PartitionRole {
    Leader,
    Follower,
}

pub enum ParittionState {
    InSync,
    OutOfSync,
}

pub struct Partition {
    id: u8,
    role: PartitionRole,
    topic: String,
    peers: Vec<Partition>,
    state: ParittionState,
    retention_period: u16,
}

impl Partition {
    pub fn new(id: u8, topic: String, retention_period: u16) -> Partition {
        Partition {
            id,
            topic,
            retention_period,

            role: PartitionRole::Follower,
            peers: Vec::new(),
            state: ParittionState::OutOfSync,
        }
    }

    pub fn add_peer(&mut self, peer: Partition) {
        self.peers.push(peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_partition() {
        let partition = Partition::new(1, "test_topic".to_string(), 7);
        assert_eq!(partition.id, 1);
        assert_eq!(partition.role, PartitionRole::Follower);
        assert_eq!(partition.topic, "test_topic");
        assert_eq!(partition.peers.len(), 0);
    }

    #[test]
    fn test_add_peer() {
        let mut partition = Partition::new(1, "test_topic".to_string(), 7);
        let peer = Partition::new(2, "test_topic".to_string(), 7);
        partition.add_peer(peer);
        assert_eq!(partition.peers.len(), 1);
    }
}
