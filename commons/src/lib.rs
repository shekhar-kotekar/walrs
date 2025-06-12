use bincode::Decode;
use bytes::BytesMut;
use tokio::{io::AsyncReadExt, net::TcpStream};
use tracing_subscriber::fmt::format::FmtSpan;

pub mod models;

pub fn init_tracing(log_level: Option<tracing::Level>) {
    // let file_appender = tracing_appender::rolling::daily("/tmp/kraft-rs/logs/", "kraft-rs.log");
    // let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(log_level.unwrap_or(tracing::Level::DEBUG))
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .with_thread_ids(true)
        // .with_writer(non_blocking)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    tracing::info!("Tracing enabled!");
}

pub async fn deserialize_from_socket<T: Decode<()>>(
    socket: &mut TcpStream,
) -> Result<T, std::io::Error> {
    let mut buffer = BytesMut::with_capacity(1024);
    let bytes_read = socket.read_buf(&mut buffer).await.ok();
    tracing::debug!("Received {:?} bytes from socket", bytes_read);
    if bytes_read.is_none() || bytes_read.unwrap() == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Socket closed",
        ));
    }
    tracing::debug!("Buffer length: {}", buffer.len());
    let (decoded, decoded_length): (T, usize) =
        bincode::decode_from_slice(&buffer, bincode::config::standard()).unwrap();

    tracing::debug!("Decoded length: {}", decoded_length);
    if decoded_length != buffer.len() {
        tracing::warn!(
            "Decoded length {} does not match buffer length {}",
            decoded_length,
            buffer.len()
        );
    }
    Ok(decoded)
}

#[cfg(test)]
mod lib {

    use crate::models::{AdminCommand, Client};

    use super::*;
    use tokio::{io::AsyncWriteExt, net::TcpListener};

    #[tokio::test]
    async fn test_deserialize_command_from_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let mut sender_stream = TcpStream::connect(address).await.unwrap();
            let create_topic_command = Client::Admin(AdminCommand::CreateTopic {
                name: "test_topic".into(),
                num_partitions: 3,
                replication_factor: 2,
                retention_period_ms: Some(60000),
            });
            let serialized_command =
                bincode::encode_to_vec(&create_topic_command, bincode::config::standard()).unwrap();
            sender_stream.write_all(&serialized_command).await.unwrap();
            tracing::debug!("Sent command: {:?}", create_topic_command);
            tracing::debug!(
                "Sent serialized command length: {}",
                serialized_command.len()
            );
        });

        let (mut receiver_stream, _) = listener.accept().await.unwrap();

        let deserialized_command: Client = deserialize_from_socket(&mut receiver_stream)
            .await
            .expect("Failed to deserialize command");

        tracing::debug!("Deserialized command: {:?}", deserialized_command);
        if let Client::Admin(AdminCommand::CreateTopic {
            name,
            num_partitions,
            replication_factor,
            retention_period_ms,
        }) = deserialized_command
        {
            assert_eq!(name, "test_topic");
            assert_eq!(num_partitions, 3);
            assert_eq!(replication_factor, 2);
            assert_eq!(retention_period_ms, Some(60000));
        } else {
            panic!("Deserialized command is not of type AdminCommand::CreateTopic");
        }
    }
}
