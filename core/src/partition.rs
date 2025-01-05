use tokio::{io::AsyncReadExt, net::TcpListener};
use tokio_util::sync::CancellationToken;

use crate::models::Partition;

impl Partition {
    pub async fn run(&self, address: &str, cancellation_token: CancellationToken) {
        tracing::info!(
            "Partition {} for topic {} is running.",
            self.number,
            self.topic_name
        );
        let listener = TcpListener::bind(address).await.unwrap();
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Partition {} for topic {} is cancelled.", self.number, self.topic_name);
                    drop(listener);
                    break;
                }
                Ok((mut stream, _)) = listener.accept() => {
                    let mut buffer = [0; 1024];
                    match stream.read(&mut buffer).await {
                        Ok(n) => {
                            let message = String::from_utf8_lossy(&buffer[..n]);
                            tracing::info!("Partition {} for topic {} received message: {}", self.number, self.topic_name, message);
                        }
                        Err(e) => {
                            tracing::error!("Error reading from stream: {:?}", e);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::{io::AsyncWriteExt, net::TcpStream};
    use tracing_test::traced_test;

    use super::*;

    const SLEEP_DURATION_MILLIS: u64 = 5;

    #[tokio::test]
    async fn create_partition_and_run() {
        let partition = Partition {
            number: 1,
            topic_name: "test_topic".to_string(),
        };
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.clone();
        let partition_address = "0.0.0.0:3000";
        let handle = tokio::spawn(async move {
            partition.run(partition_address, ct_clone).await;
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(SLEEP_DURATION_MILLIS)).await;
        cancellation_token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn write_message_to_partition() {
        let partition = Partition {
            number: 1,
            topic_name: "test_topic".to_string(),
        };
        let cancellation_token = CancellationToken::new();
        let ct_clone = cancellation_token.clone();
        let partition_address = "0.0.0.0:3001";
        let handle = tokio::spawn(async move {
            partition.run(partition_address, ct_clone).await;
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(SLEEP_DURATION_MILLIS)).await;
        let message = "Hello, World!".as_bytes();
        let mut stream = TcpStream::connect(partition_address).await.unwrap();
        stream.write(message).await.unwrap();

        cancellation_token.cancel();
        handle.await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(SLEEP_DURATION_MILLIS)).await;
        panic!("Test not completed yet");
    }

    #[ignore = "not implemented"]
    #[tokio::test]
    #[traced_test]
    async fn read_message_from_partition() {}
}
