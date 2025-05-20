use common::{
    admin::ClusterAdmin,
    consumer::Consumer,
    models::{ClientCommand, ClusterResponse, Message},
    producer::Producer,
};

#[tokio::main]
async fn main() {
    common::enable_tracing();
    let admin = ClusterAdmin {
        brokers: vec!["127.0.0.1:5056".into(), "broker2:9092".into()],
    };
    let topic_name: &str = "my_new_topic";
    let command = ClientCommand::CreateTopic {
        topic_name: topic_name.into(),
        num_partitions: 3,
        retention_period_hours: 48,
    };
    match admin.create_topic(command) {
        ClusterResponse::TopicCreated { leader_address } => {
            tracing::info!("Topic created successfully. Leader address: {}", leader_address);
            send_messages(topic_name);
            read_messages(topic_name, admin.brokers.clone()).await;
        }
        ClusterResponse::TopicAlreadyExists => {
            tracing::info!("{} topic already exists.", topic_name);
            send_messages(topic_name);
            read_messages(topic_name, admin.brokers.clone()).await;
        }
        e => {
            tracing::error!("Failed to create topic because: {:?}", e);
        }
    }
}

fn send_messages(topic_name: &str) {
    let first_message = Message {
        payload: "first_message".as_bytes().to_vec(),
    };
    let mut producer = Producer::new(vec!["127.0.0.1:5056".into()]);
    producer.send(topic_name.to_owned(), first_message);
    producer.send(
        topic_name.to_owned(),
        Message {
            payload: "second_message".as_bytes().to_vec(),
        },
    );
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

async fn read_messages(topic_name: &str, brokers: Vec<String>) {
    let mut consumer = Consumer::new(topic_name.to_owned(), brokers);
    match consumer.next_message().await {
        Some(message_batch) => {
            tracing::info!("Received message count: {:?}", message_batch.messages.len());
            message_batch.messages.iter().for_each(|m| {
                tracing::info!("Received message: {:?}", m);
            });
        }
        None => {
            tracing::error!("Failed to receive message batch.");
        }
    }
}
