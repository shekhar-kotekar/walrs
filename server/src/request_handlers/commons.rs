use common::models::ClientCommand;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

pub async fn read_client_command(socket: &mut TcpStream) -> Option<ClientCommand> {
    let mut buffer = [0u8; 1024];
    let bytes_read = socket.read(&mut buffer).await.ok()?;
    let client_command: ClientCommand = bincode::deserialize(&buffer[..bytes_read]).ok()?;
    Some(client_command)
}
