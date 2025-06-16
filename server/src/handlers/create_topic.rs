use std::{collections::HashMap, io::ErrorKind};

use commons::models::{
    PartitionInfo, PartitionRole, PeerCommand, PeerResponse, Topic, TopicStatus,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    models::{ClusterInfo, PartitionCommand},
    partition_managers::partition_writer::create_partition,
};

#[derive(Debug)]
pub enum TopicManagerResponse {
    TopicCreated {
        topic: Topic,
        partition_zero_tx: mpsc::Sender<PartitionCommand>,
    },
    TopicCreationFailed {
        topic_name: String,
        error: String,
    },
}

pub async fn create_topic(
    topic: Topic,
    self_address: String,
    local_data_dir_path: String,
    cluster_info: ClusterInfo,
    broker_tx: mpsc::Sender<TopicManagerResponse>,
    cancellation_token: CancellationToken,
) {
    let mut topic_clone = topic.clone();
    let topic_name = topic_clone.name.clone();
    tokio::select! {
        _ = cancellation_token.cancelled() => {
            tracing::info!("Cancellation token cancelled. Topic not created.");
            broker_tx
                .send(TopicManagerResponse::TopicCreationFailed {
                    topic_name,
                    error: "Topic creation was cancelled.".to_string(),
                })
                .await
                .expect("Failed to send topic creation failed command to broker");
        }
        _ = async {
            tracing::info!("Creating {:?}", topic_clone);
            if cluster_info.nodes.len() < topic_clone.num_partitions as usize {
                tracing::warn!(
                    "Not enough peers to create {} partitions.
                    Topic will be created but will be under replicated. 
                    Once more peers join, walrs will move partitions to them.",
                    topic_clone.num_partitions
                );
            }
            // step 1: for p partitions and r replication factor, we need (p * r) different partition writers
            // example: if p = 3 and r = 2, we need 6 partition writers
            let selected_nodes_for_topic: HashMap<String, usize> =
                find_nodes_for_topic(&self_address, topic_clone.num_partitions, &cluster_info);
            // step 2: create local partition writer for partition 0
            let local_partition_writer_cancellation_token = cancellation_token.child_token();
            let partition_zero = 0;

            let partition_zero_tx: Option<mpsc::Sender<PartitionCommand>> = create_partition(&topic_clone.name,
                partition_zero,
                &self_address,
                PartitionRole::Leader {followers: selected_nodes_for_topic.clone()},
                &local_data_dir_path,
                local_partition_writer_cancellation_token).await;

            match partition_zero_tx {
                Some(partition_zero_tx) => {
                    tracing::info!("Partition 0 created for topic: {}", topic_clone.name);

                    let partition_zero_info = PartitionInfo {
                        number: partition_zero,
                        leader_address: self_address.clone(),
                        follower_addresses: selected_nodes_for_topic.keys().cloned().collect(),
                    };
                    let mut partitions: Vec<PartitionInfo> = vec![partition_zero_info];

                    match create_remote_lead_partitions(&topic_clone.name, &self_address, selected_nodes_for_topic).await {
                        Ok(remote_partitions) => {
                            partitions.extend(remote_partitions);
                            topic_clone.add_partitions(partitions);
                            topic_clone.status = TopicStatus::ReadyToServe;
                            tracing::debug!(
                                "Topic created successfully: {:?}",
                                topic_clone.name
                            );
                            broker_tx
                                .send(TopicManagerResponse::TopicCreated {
                                    topic: topic_clone,
                                    partition_zero_tx: partition_zero_tx.clone(),
                                })
                                .await
                                .expect("Failed to send topic created command to broker");
                        }
                        Err(e) => {
                            tracing::error!("Failed to create remote lead partitions: {}", e);
                            broker_tx
                                .send(TopicManagerResponse::TopicCreationFailed {
                                    topic_name: topic_clone.name.clone(),
                                    error: e.to_string(),
                                })
                                .await
                                .expect("Failed to send topic creation failed command to broker");
                        }
                    }
                }
                None => {
                    tracing::error!("Failed to create local partition writer for partition 0 of topic: {}", topic.name);
                    broker_tx
                        .send(TopicManagerResponse::TopicCreationFailed {
                            topic_name: topic.name.clone(),
                            error: "Failed to create local partition writer for partition 0".to_string(),
                        })
                        .await
                        .expect("Failed to send topic creation failed command to broker");
                }
            }
        } => (),
    }
}

async fn create_remote_lead_partitions(
    topic_name: &String,
    self_address: &str,
    potential_peers: HashMap<String, usize>,
) -> Result<Vec<PartitionInfo>, std::io::Error> {
    let mut partition_infos = Vec::new();
    for (partition_leader_peer_address, partition_number) in potential_peers.iter() {
        let mut followers: HashMap<String, usize> = potential_peers
            .iter()
            .filter(|(address, _)| *address != partition_leader_peer_address)
            .map(|(address, index)| (address.clone(), *index))
            .collect();
        followers.insert(self_address.to_owned(), 0); // add self as a follower with index 0
        let command = PeerCommand::CreatePartitionWriter {
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
                partition_infos.push(partition_info);
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

    Ok(partition_infos)
}

fn find_nodes_for_topic(
    self_address: &String,
    num_partitions: u8,
    cluster_info: &ClusterInfo,
) -> HashMap<String, usize> {
    // sort peers in descending order of number of partitions they have
    // and return the top N peers where N is the number of partitions
    // we will use this to distribute partitions across peers

    // sort peers by number of registered topics in descending order
    let mut peers_sorted_by_number_of_topics: Vec<(usize, String)> = cluster_info
        .nodes
        .iter()
        .map(|(peer_address, peer_info)| (peer_info.registed_topics.len(), peer_address.clone()))
        .filter(|(_, peer_address)| peer_address != self_address)
        .collect();
    peers_sorted_by_number_of_topics.sort_by_key(|&(topic_count, _)| topic_count);

    let potential_peers: HashMap<String, usize> = peers_sorted_by_number_of_topics
        .iter()
        .take(num_partitions as usize)
        .enumerate()
        .map(|(index, (_, peer_address))| (peer_address.clone(), index + 1))
        .collect();
    tracing::info!(
        "Potential peers except this node, for topic: {:?}",
        potential_peers
    );
    potential_peers
}

#[cfg(test)]
mod tests {
    use commons::models::NodeInfo;
    use tracing_test::traced_test;

    use super::*;

    #[tokio::test]
    // #[ignore]
    #[traced_test]
    async fn test_should_be_able_to_create_follower_partition() {
        // let num_partitions = 3;
        // let replication_factor = 2;
        // let topic_to_create = Topic::new(
        //     String::from("test-topic"),
        //     Some(num_partitions),
        //     Some(replication_factor),
        //     None,
        //     None,
        // )
        // .unwrap();

        let self_address = String::from("localhost:8080");
        let peer_1_address = String::from("localhost:8081");
        let peer_2_address = String::from("localhost:8082");

        let mut cluster_info = ClusterInfo::new();
        cluster_info.add_node(NodeInfo::new(self_address.clone()));
        cluster_info.add_node(NodeInfo::new(peer_1_address.clone()));
        cluster_info.add_node(NodeInfo::new(peer_2_address.clone()));

        let cancellation_token = CancellationToken::new();

        // let topic_cancellation_token = cancellation_token.clone();

        // tokio::spawn(async move {
        //     create_topic(
        //         topic_to_create,
        //         self_address,
        //         String::from("/tmp/walrs/test_data/"),
        //         cluster_info,
        //         topic_cancellation_token,
        //     )
        //     .await
        // });
        // tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        cancellation_token.cancel();
    }
}
