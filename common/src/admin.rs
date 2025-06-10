use std::{io::Write, net::TcpStream};

use crate::{
    authenticator,
    models::{ClientCommand, ClusterResponse},
};

pub struct ClusterAdmin {
    pub brokers: Vec<String>,
}

impl ClusterAdmin {
    pub fn get_request_status(&self, topic_name: String) -> ClusterResponse {
        tracing::debug!("Sending request status query for topic: {}", topic_name);
        self.send_command_and_get_response(&ClientCommand::GetStatus { topic_name })
    }

    pub fn create_topic(&self, command: ClientCommand) -> ClusterResponse {
        // Simulate sending a request to the broker to create a topic
        tracing::debug!("Sending create topic request: {:?}", command);
        self.send_command_and_get_response(&command)
    }

    fn send_command_and_get_response(&self, command: &ClientCommand) -> ClusterResponse {
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
                tracing::debug!("Request sent, waiting for response.");
                let response = ClusterResponse::deserialize(&mut stream);
                response.unwrap_or_else(|| {
                    tracing::error!("Failed to deserialize response");
                    ClusterResponse::Error {
                        message: "Failed to deserialize response".to_string(),
                    }
                })
            }
            _ => {
                tracing::error!("Failed to authenticate admin request");
                ClusterResponse::Error {
                    message: "Failed to authenticate admin request".to_string(),
                }
            }
        }
    }
}
