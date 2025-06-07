use std::{collections::HashMap, io::ErrorKind};

use common::models::{PartitionInfo, Topic, TopicMetadata};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{ClusterInfo, CommandToPeer, PartitionCommand, PartitionRole, PeerResponse},
    partition_managers::partition_writer::PartitionWriter,
    MPSC_MAX_Q_SIZE,
};

pub async fn create_topic(
    topic: Topic,
    self_address: String,
    local_data_dir_path: String,
    cluster_info: ClusterInfo,
    cancellation_token: CancellationToken,
) -> Option<(mpsc::Sender<PartitionCommand>, TopicMetadata)> {
    tokio::select! {
        _ = cancellation_token.cancelled() => {
            tracing::info!("Cancellation token cancelled. Topic not created.");
            None
        }
        result = async {
            tracing::info!("Creating new topic: {:?}", topic);
            if cluster_info.brokers.len() < topic.num_partitions as usize {
                tracing::warn!(
                    "Not enough peers to create {} partitions.
                        Topic will be created but will be under replicated. 
                        Once more peers join, walrs will move partitions to them.",
                    topic.num_partitions
                );
            }
            // step 1: for p partitions and r replication factor, we need (p * r) different partition writers
            // example: if p = 3 and r = 2, we need 6 partition writers
            let potential_peers_for_topic: HashMap<String, usize> = find_peers_for_topic(&self_address, topic.num_partitions, &cluster_info);
            // step 2: create local partition writer for partition 0
            let local_partition_writer_cancellation_token = cancellation_token.child_token();
            let partition_zero = 0;

            let partition_zero_tx: Option<mpsc::Sender<PartitionCommand>> = create_partition(&topic.name,
                partition_zero,
                &self_address,
                PartitionRole::Leader {followers: potential_peers_for_topic.clone()},
                &local_data_dir_path,
                local_partition_writer_cancellation_token).await;

            match partition_zero_tx {
                Some(partition_zero_tx) => {
                    tracing::info!("Partition 0 created for topic: {}", topic.name);

                    let partition_zero_info = PartitionInfo {
                        number: partition_zero,
                        leader_address: self_address.clone(),
                        follower_addresses: potential_peers_for_topic.keys().cloned().collect(),
                    };
                    let mut partitions: Vec<PartitionInfo> = vec![partition_zero_info];

                    match create_remote_lead_partitions(&topic.name, &self_address, potential_peers_for_topic).await {
                        Ok(remote_partitions) => {
                            partitions.extend(remote_partitions);
                            let topic_metadata = TopicMetadata {
                                name: topic.name.clone(),
                                replication_factor: topic.replication_factor,
                                retention_period_minutes: topic.retention_period_minutes,
                                ack_level: topic.ack_level,
                                partitions,
                            };
                            Some((partition_zero_tx, topic_metadata))
                        }
                        Err(e) => {
                            tracing::error!("Failed to create remote lead partitions: {}", e);
                            None
                        }
                    }
                }
                None => {
                    tracing::error!("Failed to create local partition writer for partition 0 of topic: {}", topic.name);
                    None
                }
            }

        } => result
    }
}

async fn send_request_to_peer(peer_address: &String, command: CommandToPeer) -> Option<PeerResponse> {
    let bytes = common::to_bytes(&command);
    match tokio::net::TcpStream::connect(peer_address).await {
        Ok(mut stream) => {
            if let Err(e) = stream.write_all(&bytes).await {
                tracing::error!("Failed to send command to peer {}: {}", peer_address, e);
                return None;
            }
            tracing::info!("Command sent to peer {}, waiting for response", peer_address);
            common::read_command_from_socket::<PeerResponse>(&mut stream).await
        }
        Err(e) => {
            tracing::error!("Failed to connect to peer {}: {}", peer_address, e);
            None
        }
    }
}

async fn create_remote_lead_partitions(
    topic_name: &String,
    self_address: &String,
    potential_peers: HashMap<String, usize>,
) -> Result<Vec<PartitionInfo>, std::io::Error> {
    let mut partition_infos = Vec::new();
    for (partition_leader_peer_address, partition_number) in potential_peers.iter() {
        let mut followers: HashMap<String, usize> = potential_peers
            .iter()
            .filter(|(address, _)| *address != partition_leader_peer_address)
            .map(|(address, index)| (address.clone(), *index))
            .collect();
        followers.insert(self_address.clone(), 0); // add self as a follower with index 0
        let command = CommandToPeer::CreatePartitionWriter {
            topic_name: topic_name.clone(),
            partition_number: *partition_number as u8,
            role: PartitionRole::Leader {
                followers: followers.clone(),
            },
        };
        tracing::info!(
            "Requesting peer {} to create lead partition for topic: {}, partition: {}",
            partition_leader_peer_address,
            topic_name,
            partition_number + 1
        );
        match send_request_to_peer(partition_leader_peer_address, command).await {
            Some(PeerResponse::PartitionWriterCreated) => {
                tracing::info!(
                    "Peer {} successfully created lead partition for topic: {}, partition: {}",
                    partition_leader_peer_address,
                    topic_name,
                    partition_number + 1
                );
                let partition_info = PartitionInfo {
                    number: *partition_number as u8,
                    leader_address: partition_leader_peer_address.clone(),
                    follower_addresses: followers.keys().cloned().collect(),
                };
                partition_infos.push(partition_info);
            }
            _ => {
                tracing::error!(
                    "Failed to request peer {} to create lead partition for topic: {}, partition: {}",
                    partition_leader_peer_address,
                    topic_name,
                    partition_number + 1
                );
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "Failed to create remote lead partition on peer {} for topic: {}, partition: {}",
                        partition_leader_peer_address,
                        topic_name,
                        partition_number + 1
                    ),
                ));
            }
        }
    }
    Ok(partition_infos)
}

fn find_peers_for_topic(
    self_address: &String,
    num_partitions: u8,
    cluster_info: &ClusterInfo,
) -> HashMap<String, usize> {
    // sort peers in descending order of number of partitions they have
    // and return the top N peers where N is the number of partitions
    // we will use this to distribute partitions across peers
    tracing::info!("Finding peers for topic with {} partitions", num_partitions);

    // sort peers by number of registered topics in descending order
    let mut peers_sorted_by_number_of_topics: Vec<(usize, String)> = cluster_info
        .brokers
        .iter()
        .map(|(peer_address, peer_info)| (peer_info.registed_topics.len(), peer_address.clone()))
        .collect();
    peers_sorted_by_number_of_topics.sort_by_key(|&(topic_count, _)| topic_count);

    tracing::info!(
        "Peers sorted by number of topics: {:?}",
        peers_sorted_by_number_of_topics
    );

    let potential_peers: HashMap<String, usize> = peers_sorted_by_number_of_topics
        .iter()
        .filter(|&(_, peer_address)| peer_address != self_address)
        .take(num_partitions as usize)
        .enumerate()
        .map(|(index, (_, peer_address))| (peer_address.clone(), index + 1))
        .collect();
    tracing::info!("Potential peers for topic: {:?}", potential_peers);
    potential_peers
}

pub async fn create_partition(
    topic_name: &String,
    partition_number: u8,
    self_address: &String,
    partition_role: PartitionRole,
    local_data_dir_path: &String,
    cancellation_token: CancellationToken,
) -> Option<mpsc::Sender<PartitionCommand>> {
    let (partition_writer_tx, partition_writer_rx) = mpsc::channel::<PartitionCommand>(MPSC_MAX_Q_SIZE);
    let mut partition_writer = PartitionWriter::new(topic_name, partition_number, local_data_dir_path);
    tracing::info!(
        "Starting {} partition {} for topic {}",
        partition_role,
        partition_number,
        topic_name
    );
    tokio::spawn(async move {
        partition_writer
            .start(partition_writer_rx, cancellation_token)
            .await;
    });
    match partition_role {
        PartitionRole::Leader { followers } => {
            tracing::info!(
                "Created leader partition {} for topic: {}, followers: {:?}",
                partition_number,
                topic_name,
                followers
            );
            let all_peers_created_followers =
                create_partition_followers(&followers, &topic_name, &self_address).await;
            if all_peers_created_followers {
                Some(partition_writer_tx)
            } else {
                tracing::error!(
                    "Failed to request peers to create follower partitions for topic: {}",
                    topic_name
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
            Some(partition_writer_tx)
        }
    }
}

async fn create_partition_followers(
    peers: &HashMap<String, usize>,
    topic_name: &String,
    self_address: &str,
) -> bool {
    for (peer_address, partition_number) in peers {
        let request_result = request_peer_to_create_follower_partitions(
            peer_address,
            topic_name,
            (partition_number + 1) as u8, // partition numbers start from 0, so we add 1 to match the partition number
            self_address.to_string(),
        )
        .await;
        match request_result {
            Ok(_) => {
                tracing::info!(
                    "Successfully requested peer {} to create partition writer for topic {} with partition number {}",
                    peer_address,
                    topic_name,
                    partition_number + 1
                );
                continue;
            }
            Err(e) => {
                tracing::error!(
                    "Failed to request peer {} to create partition writer for topic {} with partition number {}: {}",
                    peer_address,
                    topic_name,
                    partition_number + 1,
                    e
                );
                return false; // if any request fails, we return false
            }
        }
    }
    true
}

async fn request_peer_to_create_follower_partitions(
    peer_address: &String,
    topic_name: &String,
    partition_number: u8,
    self_address: String,
) -> Result<(), std::io::Error> {
    let peer_command = CommandToPeer::CreatePartitionWriter {
        topic_name: topic_name.clone(),
        partition_number,
        role: PartitionRole::Follower {
            leader_address: self_address,
        },
    };
    tracing::info!(
        "Requesting peer {} to create partition writer for topic {} with partition number {}",
        peer_address,
        topic_name,
        partition_number
    );
    common::write_command_to_socket(peer_address, &peer_command).await
        .map_err(|e| {
            tracing::error!(
                "Failed to send command to peer {}: {}",
                peer_address,
                e
            );
            e
        })
        .map(|_| {
            tracing::info!(
                "Successfully sent command to peer {} to create partition writer for topic {} with partition number {}",
                peer_address,
                topic_name,
                partition_number
            );
        })
}

#[cfg(test)]
mod tests {
    use tokio::task::JoinHandle;
    use tracing_test::traced_test;

    use crate::models::BrokerInfo;

    use super::*;

    #[tokio::test]
    // #[ignore]
    #[traced_test]
    async fn test_should_be_able_to_create_follower_partition() {
        let num_partitions = 3;
        let replication_factor = 2;
        let topic_to_create = Topic::new(
            String::from("test-topic"),
            Some(num_partitions),
            Some(replication_factor),
            None,
            None,
        )
        .unwrap();

        let self_address = String::from("localhost:8080");
        let peer_1_address = String::from("localhost:8081");
        let peer_2_address = String::from("localhost:8082");

        let mut cluster_info = ClusterInfo::new();
        cluster_info.add_broker(self_address.clone(), BrokerInfo::new());
        cluster_info.add_broker(peer_1_address.clone(), BrokerInfo::new());
        cluster_info.add_broker(peer_2_address.clone(), BrokerInfo::new());

        let cancellation_token = CancellationToken::new();

        let topic_cancellation_token = cancellation_token.clone();
        let join_handle: JoinHandle<Option<(mpsc::Sender<PartitionCommand>, TopicMetadata)>> =
            tokio::spawn(async move {
                create_topic(
                    topic_to_create,
                    self_address,
                    String::from("/tmp/walrs/test_data/"),
                    cluster_info,
                    topic_cancellation_token,
                )
                .await
            });
        match join_handle.await {
            Ok(result) => {
                tracing::info!("Join handle completed with result: {:?}", result);
                // assert!(result.is_some(), "Expected topic creation to succeed");
                // let (_, topic_metadata) = result.unwrap();
                // assert_eq!(
                //     topic_metadata.partitions.len(),
                //     0,
                //     "Expected no peers to be requested for follower partition"
                // );
            }
            Err(e) => panic!("Failed to join handle: {:?}", e),
        }
        cancellation_token.cancel();
    }
}
