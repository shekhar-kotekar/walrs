use common::enable_tracing;
use node::{Node, NodeState};
use uuid::Uuid;

mod common;
mod node;

#[tokio::main]
async fn main() {
    enable_tracing();
    let local_node = Node {
        id: Uuid::new_v4(),
        state: NodeState::Follower,
        term: 0,
        election_timeout: 1000,
        address: "".to_string(),
    };
    tracing::info!("Starting Node {}", local_node.id);
}
