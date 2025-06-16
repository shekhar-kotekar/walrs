use std::io::{Error, ErrorKind};

use crate::{
    models::{ConsumerCommand, ConsumerResponse, Message},
    send_and_receive_consumer_command,
};

pub struct Consumer {
    brokers: Vec<String>,
}
impl Consumer {
    pub fn new(brokers: Vec<String>) -> Self {
        Consumer { brokers }
    }

    pub async fn fetch_messages(&self, topic: &str) -> Result<Vec<Message>, Error> {
        tracing::info!("Fetching messages from: {}", &self.brokers[0]);
        let command = ConsumerCommand::FetchMessages {
            topic: topic.to_string(),
            offset: None,
        };
        let response = send_and_receive_consumer_command(command, &self.brokers[0]).await;
        match response {
            Ok(response) => match response {
                ConsumerResponse::MessagesFetched { messages } => Ok(messages),
                ConsumerResponse::Error(err) => Err(Error::new(ErrorKind::Other, err)),
            },
            Err(e) => Err(Error::new(ErrorKind::Other, e.to_string())),
        }
    }
}
