use std::env;

use crate::node::Node;

mod node;

#[tokio::main]
async fn main() {
    commons::init_tracing(Some(tracing::Level::INFO));
    let pod_ip: String = env::var("POD_IP").expect("POD_IP environment variable not set.");
    let broker_address: String = format!("{}:8085", pod_ip);

    let node = Node {
        address: broker_address,
    };
    node.start().await;

    tracing::info!("Main Exited.");
}
