use bincode::{Decode, Encode, error::DecodeError};
use bytes::BytesMut;
use std::{
    fmt::Debug,
    io::{Error, ErrorKind},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing_subscriber::fmt::format::FmtSpan;

use crate::models::{
    AdminCommand, AdminResponse, ConsumerCommand, ConsumerResponse, PeerCommand, PeerResponse,
    ProducerCommand, ProducerResponse, WalrsCommand, WalrsResponse,
};

pub mod models;
pub mod producer;

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

pub async fn write_to_socket<T: Encode + Debug>(
    data: &T,
    socket: &mut TcpStream,
) -> Result<(), std::io::Error> {
    let serialized = bincode::encode_to_vec(data, bincode::config::standard()).unwrap();
    socket.write_all(&serialized).await?;
    Ok(())
}

pub async fn send_and_receive<MessageType: Encode + Debug, ResponseType: Decode<()>>(
    data: &MessageType,
    peer_address: &str,
) -> Result<ResponseType, std::io::Error> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        match TcpStream::connect(peer_address).await {
            Ok(mut stream) => {
                write_to_socket(data, &mut stream).await?;
                read_from_socket::<ResponseType>(&mut stream).await
            }
            Err(e) => Err(e),
        }
    })
    .await
    .map_err(|e| Error::new(ErrorKind::TimedOut, format!("Operation timed out: {}", e)))?
}

pub async fn send_and_receive_consumer_command(
    command: ConsumerCommand,
    peer_address: &str,
) -> ConsumerResponse {
    let walrs_command = WalrsCommand::Consumer(command);
    match send_and_receive::<WalrsCommand, WalrsResponse>(&walrs_command, peer_address).await {
        Ok(response) => match response {
            WalrsResponse::Consumer(consumer_response) => consumer_response,
            response => {
                ConsumerResponse::Error(format!("Unexpected response type: {:?}", response))
            }
        },
        Err(e) => ConsumerResponse::Error(format!("Failed to send command: {}", e)),
    }
}

pub async fn send_and_receive_producer_command(
    command: ProducerCommand,
    peer_address: &str,
) -> ProducerResponse {
    let walrs_command = WalrsCommand::Producer(command);
    match send_and_receive::<WalrsCommand, WalrsResponse>(&walrs_command, peer_address).await {
        Ok(response) => match response {
            WalrsResponse::Producer(producer_response) => producer_response,
            response => ProducerResponse::Error {
                message: format!("Unexpected response type: {:?}", response),
            },
        },
        Err(e) => ProducerResponse::Error {
            message: format!("Failed to send command: {}", e),
        },
    }
}

pub async fn send_and_receive_peer_command(
    command: PeerCommand,
    peer_address: &str,
) -> PeerResponse {
    tracing::debug!("Sending: {:?} to: {}", command, peer_address);
    let walrs_command = WalrsCommand::Peer(command);
    match send_and_receive::<WalrsCommand, WalrsResponse>(&walrs_command, peer_address).await {
        Ok(response) => match response {
            WalrsResponse::Peer(peer_response) => peer_response,
            response => PeerResponse::Error {
                message: format!("Unexpected response from peer: {:?}", response),
            },
        },
        Err(e) => PeerResponse::Error {
            message: format!("Failed to send command to peer: {}", e),
        },
    }
}

pub async fn send_and_receive_admin_command(
    command: AdminCommand,
    peer_address: &str,
) -> AdminResponse {
    let walrs_command = WalrsCommand::Admin(command);
    match send_and_receive::<WalrsCommand, WalrsResponse>(&walrs_command, peer_address).await {
        Ok(response) => match response {
            WalrsResponse::Admin(admin_response) => admin_response,
            response => AdminResponse::Error(format!("Unexpected response type: {:?}", response)),
        },
        Err(e) => AdminResponse::Error(format!("Failed to send command: {}", e)),
    }
}

pub async fn read_from_socket<T: Decode<()>>(socket: &mut TcpStream) -> Result<T, std::io::Error> {
    let mut buffer = BytesMut::with_capacity(1024);
    let num_bytes_read = socket.read_buf(&mut buffer).await?;
    if num_bytes_read == 0 {
        Err(Error::new(ErrorKind::UnexpectedEof, "Socket closed"))
    } else {
        from_bytes::<T>(&buffer)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Failed to deserialize data"))
    }
}

pub fn to_bytes<T: Encode + Debug>(value: &T) -> Vec<u8> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .unwrap_or_else(|_| format!("Failed to serialize: {:?}", value).into_bytes())
}

pub fn from_bytes<T: Decode<()>>(bytes: &[u8]) -> Result<T, DecodeError> {
    bincode::decode_from_slice(bytes, bincode::config::standard()).map(|(value, _)| value)
}
