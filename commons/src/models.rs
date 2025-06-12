use bincode::{Decode, Encode};

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub struct Message {
    pub payload: Vec<u8>,
    pub key: Option<String>,
    pub headers: Vec<(String, String)>,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum AdminCommand {
    CreateTopic {
        name: String,
        num_partitions: u8,
        replication_factor: u8,
        retention_period_ms: Option<u64>,
    },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum ProducerCommand {
    SendMessages {
        topic: String,
        messages: Vec<Message>,
    },
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub enum Client {
    Admin(AdminCommand),
    Producer(ProducerCommand),
}

pub enum AdminResponse {
    TopicCreated,
    Error(String),
}
pub enum ProducerResponse {
    MessagesSent,
    Error(String),
}

pub enum Response {
    Admin(AdminResponse),
    Producer(ProducerResponse),
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_message_serialization() {
        let message = Message {
            payload: vec![1, 2, 3],
            key: Some("key".into()),
            headers: vec![("header".into(), "value".into())],
        };
        let serialized_message =
            bincode::encode_to_vec(&message, bincode::config::standard()).unwrap();

        let (deserialized_message, decoded_length): (Message, usize) =
            bincode::decode_from_slice(&serialized_message, bincode::config::standard()).unwrap();

        assert_eq!(message, deserialized_message);
        assert_eq!(serialized_message.len(), decoded_length);
        assert_eq!(decoded_length, serialized_message.len());
    }

    #[test]
    fn test_command_serialization() {
        let command = Client::Admin(AdminCommand::CreateTopic {
            name: "test_topic".into(),
            num_partitions: 3,
            replication_factor: 2,
            retention_period_ms: Some(60000),
        });

        let serialized_command =
            bincode::encode_to_vec(&command, bincode::config::standard()).unwrap();

        let (deserialized_command, decoded_length): (Client, usize) =
            bincode::decode_from_slice(&serialized_command, bincode::config::standard()).unwrap();

        assert_eq!(command, deserialized_command);
        assert_eq!(serialized_command.len(), decoded_length);
        assert_eq!(decoded_length, serialized_command.len());

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
