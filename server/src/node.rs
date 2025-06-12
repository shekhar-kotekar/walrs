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
    pub async fn start(&self) {
        let task_tracker = TaskTracker::new();
        let cancellation_token = CancellationToken::new();
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
    use tracing_test::traced_test;

    // use super::*;

    #[tokio::test]
    // #[ignore]
    #[traced_test]
    async fn test_foo() {}
}
