use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing_subscriber::fmt::format::FmtSpan;
pub mod admin;
mod authenticator;
pub mod broker_response;
pub mod consumer;
pub mod message_batch;
pub mod models;
pub mod producer;
use tokio::{io::AsyncReadExt, net::TcpStream};
use tokio_util::bytes::BytesMut;

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

pub fn to_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serialize(value).expect("Failed to serialize value")
}

pub fn from_bytes<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> T {
    bincode::deserialize(bytes).expect("Failed to deserialize value")
}

pub async fn read_command_from_socket<T: for<'de> Deserialize<'de>>(socket: &mut TcpStream) -> Option<T> {
    let mut buffer = BytesMut::with_capacity(1024);
    let bytes_read = socket.read_buf(&mut buffer).await.ok()?;
    tracing::debug!("Received {} bytes from socket", bytes_read);
    Some(from_bytes::<T>(&buffer))
}
