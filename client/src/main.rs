use std::{collections::HashMap, thread::sleep};

use commons::{
    admin::ClusterAdmin,
    models::{AckLevel, AdminCommand, AdminResponse, Message, ProducerResponse, Topic},
    producer::Producer,
};

#[tokio::main]
async fn main() {
    commons::init_tracing(None);

    let topic: Topic = Topic::new(
        "client_topic_1".into(),
        None,
        None,
        Some(60),
        Some(AckLevel::Leader),
    )
    .unwrap();

    let admin_command = AdminCommand::CreateTopic {
        topic: topic.clone(),
    };

    let brokers = vec![
        String::from("127.0.0.1:5075"),
        String::from("127.0.0.1:5076"),
        String::from("127.0.0.1:5077"),
    ];

    let mut producer: Producer = Producer::new(brokers.clone());
    let cluster_admin = ClusterAdmin { brokers };

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
                    let messages = vec![
                        Message {
                            key: Some("key1".to_string()),
                            payload: "this is first message".as_bytes().to_vec(),
                            headers: HashMap::from([(
                                "first_msg_header".to_string(),
                                "value1".to_string(),
                            )]),
                        },
                        Message {
                            key: Some("key2".to_string()),
                            payload: "this is second message".as_bytes().to_vec(),
                            headers: HashMap::from([("name".to_string(), "Shekhar".to_string())]),
                        },
                        Message {
                            key: None,
                            payload: "this is third message".as_bytes().to_vec(),
                            headers: HashMap::from([
                                ("name".to_string(), "foo bar".to_string()),
                                ("age".to_string(), "23".to_string()),
                            ]),
                        },
                    ];

                    producer.send_batch(topic.name.clone(), messages);
                    match producer.flush().await {
                        Ok(response) => match response {
                            ProducerResponse::MessagesPersisted { count } => {
                                tracing::info!("Messages persisted successfully: {}", count)
                            }
                            ProducerResponse::Error { message } => {
                                tracing::error!("Failed to persist messages: {}", message);
                            }
                            _ => {
                                tracing::error!("Unexpected response type: {:?}", response);
                            }
                        },
                        Err(e) => {
                            tracing::error!("Failed to send messages: {}", e);
                        }
                    }
                }
                AdminResponse::Error(err) => tracing::error!(err),

                _ => {
                    tracing::error!("Unexpected response type: {:?}", response);
                }
            }
        }
        AdminResponse::Error(err) => {
            tracing::error!(err);
        }
        _ => {
            tracing::error!("Unexpected response type: {:?}", response);
        }
    }
    tracing::info!("Cluster admin command executed successfully.");
}
