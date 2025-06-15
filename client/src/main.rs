use std::thread::sleep;

use commons::{
    admin::ClusterAdmin,
    models::{AckLevel, AdminCommand, AdminResponse, Topic},
};

#[tokio::main]
async fn main() {
    commons::init_tracing(None);

    let topic: Topic = Topic::new(
        "test_topic".into(),
        None,
        None,
        Some(60),
        Some(AckLevel::Leader),
    )
    .unwrap();

    let admin_command = AdminCommand::CreateTopic {
        topic: topic.clone(),
    };

    let cluster_admin = ClusterAdmin {
        brokers: vec![
            String::from("127.0.0.1:5075"),
            String::from("127.0.0.1:5076"),
            String::from("127.0.0.1:5077"),
        ],
    };

    let response = cluster_admin
        .send_command_and_get_response(&admin_command)
        .await;
    match response {
        AdminResponse::RequestAccepted => {
            tracing::info!(
                "Cluster admin command executed successfully: {:?}",
                admin_command
            );
            sleep(std::time::Duration::from_millis(500));
            let get_topic_info_command = AdminCommand::GetTopicInfo {
                topic_names: vec![topic.name.clone()],
            };

            match cluster_admin
                .send_command_and_get_response(&get_topic_info_command)
                .await
            {
                AdminResponse::TopicInfo { topics } => {
                    tracing::info!("Topic info retrieved successfully: {:?}", topics);
                }
                AdminResponse::Error(err) => {
                    tracing::error!("Failed to retrieve topic info: {}", err);
                }
                _ => {
                    tracing::error!("Unexpected response type: {:?}", response);
                }
            }
        }
        AdminResponse::Error(err) => {
            tracing::error!("Failed to execute cluster admin command: {}", err);
        }
        _ => {
            tracing::error!("Unexpected response type: {:?}", response);
        }
    }
    tracing::info!("Cluster admin command executed successfully.");
}
