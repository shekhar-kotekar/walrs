use bincode::{Decode, Encode, error::DecodeError};
use bytes::BytesMut;
use std::{
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing_subscriber::fmt::format::FmtSpan;

use crate::models::{PeerCommand, PeerResponse, WalrsCommand, WalrsResponse};

pub mod admin;
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

pub fn hash_code<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

pub fn to_bytes<T: Encode + Debug>(value: &T) -> Vec<u8> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .unwrap_or_else(|_| format!("Failed to serialize: {:?}", value).into_bytes())
}

pub fn from_bytes<T: Decode<()>>(bytes: &[u8]) -> Result<T, DecodeError> {
    // tracing::debug!(
    //     "deserializing {} bytes to type: {}",
    //     bytes.len(),
    //     std::any::type_name::<T>()
    // );
    bincode::decode_from_slice(bytes, bincode::config::standard()).map(|(value, _)| value)
}

pub async fn read_from_socket<T: Decode<()>>(socket: &mut TcpStream) -> Result<T, std::io::Error> {
    let mut buffer = BytesMut::with_capacity(512);
    let num_bytes_read = socket.read_buf(&mut buffer).await?;
    // tracing::debug!(
    //     "Buffer length: {}, number of bytes read from socket: {}",
    //     buffer.len(),
    //     num_bytes_read,
    // );

    if num_bytes_read == 0 {
        Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Socket closed",
        ))
    } else {
        from_bytes::<T>(&buffer).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to deserialize data",
            )
        })
    }
}

pub async fn send_message<T: Encode + Debug, ResponseType: Decode<()>>(
    data: &T,
    peer_address: &String,
) -> Result<ResponseType, std::io::Error> {
    match TcpStream::connect(peer_address).await {
        Ok(mut stream) => {
            tracing::debug!("Connected to peer at {}", peer_address);
            write_to_socket(data, &mut stream).await?;
            read_from_socket::<ResponseType>(&mut stream).await
        }
        Err(e) => {
            tracing::error!("Failed to connect to peer at {}: {}", peer_address, e);
            Err(e)
        }
    }
}

pub async fn send_and_receive_peer_command(
    command: PeerCommand,
    peer_address: &String,
) -> PeerResponse {
    let walrs_command = WalrsCommand::Peer(command);
    match send_message::<WalrsCommand, WalrsResponse>(&walrs_command, peer_address).await {
        Ok(response) => match response {
            WalrsResponse::Peer(peer_response) => peer_response,
            _ => PeerResponse::Error {
                message: "Unexpected response type".to_string(),
            },
        },
        Err(e) => PeerResponse::Error {
            message: format!("Failed to send command: {}", e),
        },
    }
}

pub async fn send_serialized_message<ResponseType: Decode<()>>(
    serialized_data: &[u8],
    peer_address: &String,
) -> Result<ResponseType, std::io::Error> {
    match TcpStream::connect(peer_address).await {
        Ok(mut stream) => {
            tracing::debug!(
                "Connected. peer address: {}, local address: {}",
                peer_address,
                stream.local_addr().unwrap()
            );
            stream.write_all(serialized_data).await.unwrap();
            tracing::debug!(
                "Sent serialized data of length {} to: {}",
                serialized_data.len(),
                peer_address
            );
            read_from_socket::<ResponseType>(&mut stream).await
        }
        Err(e) => {
            tracing::error!("Failed to connect to peer at {}: {}", peer_address, e);
            Err(e)
        }
    }
}

pub async fn write_to_socket<T: Encode + Debug>(
    data: &T,
    socket: &mut TcpStream,
) -> Result<(), std::io::Error> {
    let serialized = bincode::encode_to_vec(data, bincode::config::standard()).unwrap();
    socket.write_all(&serialized).await?;
    Ok(())
}

#[cfg(test)]
mod lib {

    use crate::models::{AdminCommand, Topic, WalrsCommand};

    use super::*;
    use tokio::{io::AsyncWriteExt, net::TcpListener};
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_write_to_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let mut sender_stream = TcpStream::connect(address).await.unwrap();
            let topic: Topic = Topic::new(
                "test_topic".into(),
                None,
                None,
                Some(60),
                Some(models::AckLevel::Leader),
            )
            .unwrap();
            let command_to_write = WalrsCommand::Admin(AdminCommand::CreateTopic {
                topic: topic.clone(),
            });
            write_to_socket::<WalrsCommand>(&command_to_write, &mut sender_stream)
                .await
                .unwrap();
        });

        let (mut receiver_stream, _) = listener.accept().await.unwrap();

        let deserialized_command: WalrsCommand = read_from_socket(&mut receiver_stream)
            .await
            .expect("Failed to deserialize command");

        tracing::debug!("Deserialized command: {:?}", deserialized_command);
        if let WalrsCommand::Admin(AdminCommand::CreateTopic { topic }) = deserialized_command {
            assert_eq!(topic.name, "test_topic");
            assert_eq!(topic.num_partitions, 3);
            assert_eq!(topic.replication_factor, 3);
            assert_eq!(topic.retention_period_minutes, 60);
        } else {
            panic!("Deserialized command is not of type AdminCommand::CreateTopic");
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test_read_command_from_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let topic: Topic = Topic::new(
            "test_topic".into(),
            None,
            None,
            Some(60),
            Some(models::AckLevel::Leader),
        )
        .unwrap();

        let topic_clone = topic.clone();

        tokio::spawn(async move {
            let mut sender_stream = TcpStream::connect(address).await.unwrap();
            let create_topic_command =
                WalrsCommand::Admin(AdminCommand::CreateTopic { topic: topic_clone });
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

        let deserialized_command: WalrsCommand = read_from_socket(&mut receiver_stream)
            .await
            .expect("Failed to deserialize command");

        tracing::debug!("Deserialized command: {:?}", deserialized_command);
        if let WalrsCommand::Admin(AdminCommand::CreateTopic { topic }) = deserialized_command {
            assert_eq!(topic.name, "test_topic");
            assert_eq!(topic.num_partitions, 3);
            assert_eq!(topic.replication_factor, 3);
            assert_eq!(topic.retention_period_minutes, 60);
        } else {
            panic!("Deserialized command is not of type AdminCommand::CreateTopic");
        }
    }
}
