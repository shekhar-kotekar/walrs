use common::{
    admin::ClusterAdmin,
    models::{ClientCommand, ClusterResponse, Message},
    producer::Producer,
};

fn main() {
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
    let cluster_response: ClusterResponse = producer.flush();
    match cluster_response {
        ClusterResponse::MessagesPersisted => {
            tracing::info!("Messages successfully persisted.");
        }
        e => {
            tracing::error!("Failed to persist messages because: {:?}", e);
        }
    }
}
