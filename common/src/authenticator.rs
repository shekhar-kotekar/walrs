use std::{io::Write, net::TcpStream};

use crate::models::{ClientCommand, ClientType, ClusterResponse};

pub fn authenticate(stream: &mut TcpStream, client_type: ClientType) -> Option<ClusterResponse> {
    // Simulate authentication logic
    tracing::debug!("Authenticating request for {:?}", client_type);
    let auth_command = ClientCommand::RequestToConnect { client_type };
    let _ = stream.write_all(&bincode::serialize(&auth_command).unwrap());
    stream.flush().unwrap();

    ClusterResponse::deserialize(stream)
}
