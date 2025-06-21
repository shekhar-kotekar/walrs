use std::{collections::HashMap, io::ErrorKind};

use commons::models::{PartitionInfo, PartitionRole, PeerCommand, PeerResponse};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{models::PartitionCommand, partition_managers::partition_writer::PartitionWriter};

pub async fn create_local_partition(
    topic_name: &String,
    partition_number: u8,
    self_address: &str,
    partition_role: PartitionRole,
    local_data_dir_path: &String,
    cancellation_token: CancellationToken,
) -> Option<mpsc::Sender<PartitionCommand>> {
    tracing::info!(
        "Creating partition {}, topic: {}, role: {:?}",
        partition_number,
        topic_name,
        partition_role
    );
    let (partition_writer_tx, partition_writer_rx) = mpsc::channel::<PartitionCommand>(10);

    let mut partition_writer =
        PartitionWriter::new(topic_name, partition_number, local_data_dir_path);

    tokio::spawn(async move {
        partition_writer
            .start(partition_writer_rx, cancellation_token)
            .await;
    });

    match partition_role {
        PartitionRole::Leader { followers } => {
            tracing::info!(
                "Leader partition created. Topic: {}, partition: {}, partition followers: {:?}",
                topic_name,
                partition_number,
                followers
            );
            let peers = followers.keys().cloned().collect::<Vec<String>>();
            let all_peers_created_followers =
                create_partition_followers(topic_name, partition_number, peers, self_address).await;
            if all_peers_created_followers {
                tracing::info!("Partition {} created for: {}", partition_number, topic_name);
                Some(partition_writer_tx)
            } else {
                tracing::error!(
                    "Failed to request peers to create follower partitions. topic: {}, partition: {}",
                    topic_name,
                    partition_number
                );
                None
            }
        }
        PartitionRole::Follower { leader_address } => {
            tracing::info!(
                "Created follower partition {} for topic: {}, leader address: {}",
                partition_number,
                topic_name,
                leader_address
            );
            // let partition_info = PartitionInfo {
            //     number: partition_number,
            //     leader_address: self_address.to_string(),
            //     follower_addresses: vec![self_address.to_string()],
            // };
            Some(partition_writer_tx)
        }
    }
}

async fn create_partition_followers(
    topic_name: &String,
    partition_number: u8,
    peers: Vec<String>,
    self_address: &str,
) -> bool {
    for peer in peers {
        let peer_command = PeerCommand::CreatePartitionWriter {
            topic_name: topic_name.clone(),
            partition_number,
            role: PartitionRole::Follower {
                leader_address: self_address.to_string(),
            },
        };
        match commons::send_and_receive_peer_command(peer_command, &peer).await {
            PeerResponse::PartitionWriterCreated => {
                tracing::info!(
                    "Peer {} created follower partition for topic: {}, partition: {}",
                    peer,
                    topic_name,
                    partition_number
                );
            }
            other_response => {
                tracing::error!(
                    "Peer {} responded with unexpected response: {:?} for topic: {}, partition: {}",
                    peer,
                    other_response,
                    topic_name,
                    partition_number
                );
                return false;
            }
        }
    }
    true
}

pub async fn create_remote_lead_partitions(
    topic_name: &String,
    self_address: &str,
    peers: HashMap<String, usize>,
) -> Result<Vec<PartitionInfo>, std::io::Error> {
    let mut partitions_info = Vec::new();
    for (partition_leader_peer_address, partition_number) in peers.iter() {
        let mut followers: HashMap<String, usize> = peers
            .iter()
            .filter(|(address, _)| *address != partition_leader_peer_address)
            .map(|(address, index)| (address.clone(), *index))
            .collect();

        followers.insert(self_address.to_owned(), 0); // add self as one of the followers
        let command: PeerCommand = PeerCommand::CreatePartitionWriter {
            topic_name: topic_name.clone(),
            partition_number: *partition_number as u8,
            role: PartitionRole::Leader {
                followers: followers.clone(),
            },
        };
        tracing::info!(
            "Requesting peer {} to create lead partition. Topic: {}, partition: {}",
            partition_leader_peer_address,
            topic_name,
            partition_number
        );
        match commons::send_and_receive_peer_command(command, partition_leader_peer_address).await {
            PeerResponse::PartitionWriterCreated => {
                tracing::info!(
                    "Peer {} successfully created lead partition. Topic: {}, partition: {}",
                    partition_leader_peer_address,
                    topic_name,
                    partition_number
                );
                let partition_info = PartitionInfo {
                    number: *partition_number as u8,
                    leader_address: partition_leader_peer_address.clone(),
                    follower_addresses: followers.keys().cloned().collect(),
                };
                partitions_info.push(partition_info);
            }
            other_response => {
                tracing::error!(
                    "Peer {} responded with unexpected response: {:?}. topic: {}, partition: {}",
                    partition_leader_peer_address,
                    other_response,
                    topic_name,
                    partition_number
                );
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "Unexpected response from peer {}. topic: {}, partition: {}",
                        partition_leader_peer_address, topic_name, partition_number
                    ),
                ));
            }
        }
    }
    Ok(partitions_info)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use commons::models::Message;
    use tokio::sync::oneshot;

    use crate::models::PartitionWriterResponse;

    use super::*;

    #[tokio::test]
    #[ignore]
    #[tracing_test::traced_test]
    async fn test_lead_partition_should_forward_messages_to_followers_when_topic_ack_level_is_majority()
     {
        let topic_name = String::from("topic_test_message_forwarding");
        let leader_address = String::from("127.0.0.1:5056");
        let follower_address: String = String::from("127.0.0.1:5057");
        let local_data_dir_path = String::from("/tmp/walrs/test_data/lead_partition");
        let cancellation_token = CancellationToken::new();
        let leader_tx = create_local_partition(
            &topic_name,
            0,
            &leader_address,
            PartitionRole::Leader {
                followers: HashMap::from([(follower_address.clone(), 1)]),
            },
            &local_data_dir_path,
            cancellation_token.child_token(),
        )
        .await
        .unwrap();

        let follower_data_dir_path = String::from("/tmp/walrs/test_data/follower_partition");
        let _ = create_local_partition(
            &topic_name,
            1,
            &follower_address,
            PartitionRole::Follower {
                leader_address: leader_address.clone(),
            },
            &follower_data_dir_path,
            cancellation_token.child_token(),
        )
        .await
        .unwrap();

        let message = Message {
            payload: "first test message".as_bytes().to_vec(),
            key: Some("message_key".into()),
            headers: HashMap::new(),
        };

        let (leader_oneshot_tx, leader_oneshot_rx) = oneshot::channel::<PartitionWriterResponse>();
        let partition_writer_command: PartitionCommand = PartitionCommand::WriteMessages {
            messages: vec![message],
            tx: leader_oneshot_tx,
        };
        leader_tx.send(partition_writer_command).await.unwrap();
        let leader_response = leader_oneshot_rx.await.unwrap();
        match leader_response {
            PartitionWriterResponse::MessagesPersisted { count } => {
                assert_eq!(count, 1);
            }
            PartitionWriterResponse::Error { message } => {
                panic!("Leader partition writer returned an error: {}", message);
            }
        }

        cancellation_token.cancel();
    }
}
