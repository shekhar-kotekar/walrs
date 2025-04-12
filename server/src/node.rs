use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::models::{MainCommands, NodeResponse, NodeState, Topic};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub address: String,
    pub state: NodeState,
    pub term: u64,
    pub num_partitions_on_node: u32,
}

impl Node {
    pub fn new(address: String) -> Self {
        Node {
            id: Uuid::new_v4(),
            address,
            state: NodeState::Follower,
            term: 0,
            num_partitions_on_node: 0,
        }
    }

    pub async fn run(
        &mut self,
        interval_ms: u64,
        peers: Vec<Node>,
        mut main_rx: mpsc::Receiver<MainCommands>,
        cancellation_token: CancellationToken,
    ) {
        tracing::info!("Started running {} node.", self.address);
        let existing_topics: Vec<String> = vec![];

        loop {
            tokio::select! {
                Some(command) = main_rx.recv() => {
                    match command {
                        MainCommands::GetState { tx } => {
                            let _ = tx.send(self.state.clone());
                        },
                        MainCommands::SetState { new_state, tx } => {
                            self.state = new_state;
                            let _ = tx.send(self.state.clone());
                        },
                        MainCommands::GetPeers { tx } => {
                            let _ = tx.send(peers.clone());
                        },
                        MainCommands::CreateTopic { topic, tx } => {
                            tracing::info!("Creating topic {} on node {}.", topic.name, self.address);
                            let response = self.create_topic(&topic, &existing_topics).await;
                            let _ = tx.send(response);
                        },
                    }
                },
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(interval_ms)) => {
                    // Do nothing
                },
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Node {} cancelled.", self.address);
                    break;
                }
            }
        }
    }

    async fn create_topic(&self, topic: &Topic, existing_topics: &Vec<String>) -> NodeResponse {
        // TODO: Check if the node is the leader before creating the topic
        if existing_topics.contains(&topic.name) {
            NodeResponse::TopicAlreadyExists
        } else {
            NodeResponse::TopicCreated {
                leader_address: self.address.clone(),
            }
        }
    }
}

#[cfg(test)]
mod should {
    use std::thread;

    use tracing_test::traced_test;

    use crate::models::{NodeResponse, Topic};

    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    #[traced_test]
    async fn create_a_new_topic_if_not_exists_already() {
        let mut test_node = Node::new("0.0.0.0:5075".to_string());
        let cancellation_token = CancellationToken::new();
        let node_ct = cancellation_token.child_token();
        let (main_tx, main_rx) = mpsc::channel::<MainCommands>(5);
        let run_interval = 1000;
        let node_handle = tokio::spawn(async move {
            test_node.run(run_interval, vec![], main_rx, node_ct).await;
        });

        let topic_to_create = Topic::new("test-topic".to_string());
        let (node_tx, node_rx) = oneshot::channel::<NodeResponse>();
        let create_topic_command = MainCommands::CreateTopic {
            topic: topic_to_create,
            tx: node_tx,
        };
        main_tx.send(create_topic_command).await.unwrap();

        let node_response = node_rx.await.unwrap();

        thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(
            node_response,
            NodeResponse::TopicCreated {
                leader_address: "0.0.0.0:5075".to_string()
            }
        );
        cancellation_token.cancel();
        node_handle.await.unwrap();
    }
}
