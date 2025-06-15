use commons::models::{AdminCommand, AdminResponse};
use tokio::sync::{mpsc, oneshot};

use crate::node_manager::{NodeManagerCommand, NodeManagerResponse};

pub async fn handle_admin_request(
    command: AdminCommand,
    node_manager_tx: mpsc::Sender<NodeManagerCommand>,
) -> AdminResponse {
    tracing::info!("command received: {:?}", command);
    match command {
        AdminCommand::CreateTopic { topic } => {
            node_manager_tx
                .send(NodeManagerCommand::CreateTopic { topic })
                .await
                .unwrap_or_else(|err| {
                    tracing::error!(
                        "Failed to send CreateTopic command to node manager: {}",
                        err
                    );
                });
            AdminResponse::RequestAccepted
        }
        AdminCommand::GetTopicInfo { topic_name } => {
            let (tx, rx) = oneshot::channel::<NodeManagerResponse>();
            let command = NodeManagerCommand::GetTopicInfo { topic_name, tx };
            node_manager_tx.send(command).await.unwrap_or_else(|err| {
                tracing::error!(
                    "Failed to send GetTopicInfo command to node manager: {}",
                    err
                );
            });
            match rx.await {
                Ok(response) => match response {
                    NodeManagerResponse::TopicInfo { topic } => AdminResponse::TopicInfo { topic },
                    other => AdminResponse::Error(format!("Unexpected response type: {:?}", other)),
                },
                Err(err) => {
                    AdminResponse::Error(format!("Failed to receive topic info response: {}", err))
                }
            }
        }
    }
}

#[cfg(test)]
mod should {
    use tracing_test::traced_test;

    #[tokio::test]
    // #[ignore]
    #[traced_test]
    async fn handle_create_topic_command() {}
}
