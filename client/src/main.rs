use common::{
    admin::ClusterAdmin,
    consumer::Consumer,
    models::{AckLevel, ClientCommand, ClusterResponse, Message, Topic},
    producer::Producer,
};

#[tokio::main]
async fn main() {
    common::init_tracing(None);
    let admin = ClusterAdmin {
        brokers: vec!["127.0.0.1:5056".into(), "127.0.0.1:5058".into()],
    };
    let topic_name: &str = "my_new_topic";

    let replication_factor: u8 = 2;
    let topic_to_create = Topic::new(
        topic_name.to_string(),
        Some(2), // number of partitions
        Some(replication_factor),
        Some(10),               // retention time in minutes
        Some(AckLevel::Leader), // ack level
    )
    .unwrap();
    let command = ClientCommand::CreateTopic {
        topic_details: topic_to_create,
    };
    let first_message = Message {
        key: Some("key1".to_string()),
        payload: "first_message".as_bytes().to_vec(),
    };
    let second_message = Message {
        key: None,
        payload: "second_message".as_bytes().to_vec(),
    };
    let sent_messages = vec![first_message, second_message];
    let received_messages: Vec<Message> = match admin.create_topic(command) {
        ClusterResponse::TopicCreated { topic_metadata } => {
            tracing::info!("Topic created successfully. Metadata: {:?}", topic_metadata);
            send_messages(topic_name, sent_messages.clone());
            // read_messages(topic_name, admin.brokers.clone()).await
            Vec::new() // Temporarily returning an empty vector to avoid compilation error
        }
        ClusterResponse::TopicAlreadyExists => {
            tracing::info!("{} topic already exists.", topic_name);
            send_messages(topic_name, sent_messages.clone());
            // read_messages(topic_name, admin.brokers.clone()).await
            Vec::new() // Temporarily returning an empty vector to avoid compilation error
        }
        e => {
            tracing::error!("Failed to create topic because: {:?}", e);
            Vec::new()
        }
    };
    // check if sent messages and received messages are same
    tracing::debug!("sent messages");
    for message in &sent_messages {
        tracing::debug!("  {:?}", message);
    }
    tracing::debug!("received messages");
    for message in &received_messages {
        tracing::debug!("  {:?}", message);
    }
}

fn send_messages(topic_name: &str, messages: Vec<Message>) {
    let mut producer = Producer::new(vec!["127.0.0.1:5056".into()]);
    for message in messages {
        producer.send(topic_name.to_owned(), &message);
    }
    let cluster_response: ClusterResponse = producer.flush();
    match cluster_response {
        ClusterResponse::MessagesPersisted { count } => {
            tracing::info!("{} Messages successfully persisted.", count);
        }
        e => {
            tracing::error!("Failed to persist messages because: {:?}", e);
        }
    }
}

async fn read_messages(topic_name: &str, brokers: Vec<String>) -> Vec<Message> {
    let mut consumer = Consumer::new(topic_name.to_owned(), brokers);
    match consumer.next_message().await {
        Some(message_batch) => {
            tracing::info!("Received message count: {:?}", message_batch.messages.len());
            message_batch.messages.iter().for_each(|m| {
                tracing::info!("Received message: {:?}", m);
            });
            message_batch.messages
        }
        None => {
            tracing::error!("Failed to receive message batch.");
            Vec::new()
        }
    }
}
