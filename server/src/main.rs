use std::env;

use tokio_util::sync::CancellationToken;

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
    let cancellation_token = CancellationToken::new();
    node.start(cancellation_token.child_token()).await;

    cancellation_token.cancel();
    tracing::info!("Main Exited.");
}
