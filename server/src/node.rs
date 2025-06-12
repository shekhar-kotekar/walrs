use tokio::{
    net::TcpListener,
    signal::{
        self,
        unix::{Signal, SignalKind, signal},
    },
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub struct Node {
    pub address: String,
}

impl Node {
    pub async fn start(&self, cancellation_token: CancellationToken) {
        let task_tracker = TaskTracker::new();
        let main_tcp_listener = TcpListener::bind(&self.address).await.unwrap();
        tracing::debug!("Listening on: {}", main_tcp_listener.local_addr().unwrap());

        let mut sigterm: Signal =
            signal(SignalKind::terminate()).expect("Failed to create signal handler");

        tokio::select! {
            _ = async {
                loop {
                    let (socket, _) = main_tcp_listener.accept().await.unwrap();
                    tracing::info!("connection accepted from: {:?}", socket.peer_addr());
                }
            } => {
                tracing::info!("Main listener closed.");
            }
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM signal. Cancelling all tasks.");
                cancellation_token.cancel();
                task_tracker.close();
                task_tracker.wait().await;
                tracing::info!("All tasks cancelled.");
            }
            _ = signal::ctrl_c() => {
                tracing::info!("Received Ctrl-C signal. Cancelling all tasks.");

                cancellation_token.cancel();
                tracing::info!("Cancellation token cancelled.");

                task_tracker.close();
                tracing::info!("Task tracker closed.");

                task_tracker.wait().await;
                tracing::info!("Task tracker wait is over. All tasks cancelled.");
            }
        }
    }
}

#[cfg(test)]
mod should {
    use commons::models::{AdminCommand, WalrsClient};
    use tokio::{io::AsyncWriteExt, net::TcpStream};
    use tracing_test::traced_test;

    use super::*;

    #[tokio::test]
    // #[ignore]
    #[traced_test]
    async fn return_accepted_response_when_a_command_is_sent() {
        let address: String = "127.0.0.1:8080".into();
        let node: Node = Node {
            address: address.clone(),
        };
        let cancellation_token: CancellationToken = CancellationToken::new();
        let node_cancellation_token = cancellation_token.child_token();
        tokio::spawn(async move {
            node.start(node_cancellation_token).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut sender_stream = TcpStream::connect(address).await.unwrap();
        let create_topic_command = WalrsClient::Admin(AdminCommand::CreateTopic {
            name: "test_topic".into(),
            num_partitions: 3,
            replication_factor: 2,
            retention_period_ms: Some(60000),
        });
        let serialized_command =
            bincode::encode_to_vec(&create_topic_command, bincode::config::standard()).unwrap();
        sender_stream.write_all(&serialized_command).await.unwrap();

        cancellation_token.cancel();
    }
}
