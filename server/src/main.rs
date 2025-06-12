use std::env;

use tokio::{
    net::TcpListener,
    signal::{
        self,
        unix::{Signal, SignalKind, signal},
    },
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[tokio::main]
async fn main() {
    commons::init_tracing(Some(tracing::Level::INFO));
    let pod_ip: String = env::var("POD_IP").expect("POD_IP environment variable not set.");
    let broker_address: String = format!("{}:8085", pod_ip);

    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    let main_tcp_listener = TcpListener::bind(broker_address).await.unwrap();
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
    tracing::info!("Main Exited.");
}
