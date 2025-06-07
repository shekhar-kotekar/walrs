use std::{io::Write, net::TcpStream};

use crate::{
    authenticator,
    models::{ClientCommand, ClusterResponse},
};

pub struct ClusterAdmin {
    pub brokers: Vec<String>,
}

impl ClusterAdmin {
    pub fn create_topic(&self, command: ClientCommand) -> ClusterResponse {
        // Simulate sending a request to the broker to create a topic
        tracing::debug!("Sending create topic request: {:?}", command);
        let mut stream = TcpStream::connect(&self.brokers[0]).unwrap();

        match authenticator::authenticate(&mut stream, crate::models::ClientType::Admin) {
            Some(ClusterResponse::ConnectionAccepted) => {
                tracing::info!(
                    "Authentication successful. Sending request to {}",
                    &self.brokers[0]
                );

                let serialized_command = bincode::serialize(&command).unwrap();
                stream.write_all(&serialized_command).unwrap();
                stream.flush().unwrap();

                tracing::debug!("request to create topic sent to broker. waiting for reply.");

                let broker_response = ClusterResponse::deserialize(&mut stream);
                if let Some(response) = broker_response {
                    response
                } else {
                    ClusterResponse::InternalError {
                        message: "Failed to deserialize broker response".to_string(),
                    }
                }
            }
            _ => {
                tracing::error!("Failed to authenticate admin request");
                ClusterResponse::InternalError {
                    message: "Failed to authenticate admin request".to_string(),
                }
            }
        }
    }
}
