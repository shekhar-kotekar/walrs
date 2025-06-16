use std::{collections::HashMap, thread::sleep};

use commons::{
    admin::ClusterAdmin,
    consumer::Consumer,
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

    let cluster_admin = ClusterAdmin {
        brokers: brokers.clone(),
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
                    send_messages(brokers.clone(), &topic.name).await;
                    sleep(std::time::Duration::from_millis(2500));
                    read_messages(brokers, &topic.name).await;
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

async fn read_messages(brokers: Vec<String>, topic: &str) {
    let consumer = Consumer::new(brokers.clone());
    match consumer.fetch_messages(topic).await {
        Ok(messages) => {
            tracing::info!("{} messages fetched.", messages.len());
            messages.iter().for_each(|message| {
                tracing::info!(
                    "Received message: key: {:?}, payload: {}, headers: {:?}",
                    message.key,
                    String::from_utf8_lossy(&message.payload),
                    message.headers
                );
            });
        }
        Err(e) => {
            tracing::error!("Failed to fetch messages: {}", e);
        }
    }
}

async fn send_messages(brokers: Vec<String>, topic: &str) {
    let mut producer: Producer = Producer::new(brokers.clone());
    let messages = vec![
        Message {
            key: Some("key1".to_string()),
            payload: "one".as_bytes().to_vec(),
            headers: HashMap::from([("first_msg_header".to_string(), "value1".to_string())]),
        },
        Message {
            key: Some("key2".to_string()),
            payload: "two".as_bytes().to_vec(),
            headers: HashMap::from([("name".to_string(), "Shekhar".to_string())]),
        },
        Message {
            key: None,
            payload: "three".as_bytes().to_vec(),
            headers: HashMap::from([("age".to_string(), "23".to_string())]),
        },
    ];

    producer.send_batch(topic.to_owned(), messages);
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
