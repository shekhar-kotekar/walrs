use crate::{
    models::{AdminCommand, AdminResponse, WalrsCommand, WalrsResponse},
    send_message,
};

pub struct ClusterAdmin {
    pub brokers: Vec<String>,
}

impl ClusterAdmin {
    pub async fn send_command_and_get_response(&self, command: &AdminCommand) -> AdminResponse {
        let walrs_command = WalrsCommand::Admin(command.clone());
        match send_message::<WalrsCommand, WalrsResponse>(
            &walrs_command,
            self.brokers.first().unwrap(),
        )
        .await
        {
            Ok(response) => match response {
                WalrsResponse::Admin(admin_response) => admin_response,
                _ => AdminResponse::Error("Unexpected response type".to_string()),
            },
            Err(e) => AdminResponse::Error(format!("Failed to send command: {}", e)),
        }
    }
}
