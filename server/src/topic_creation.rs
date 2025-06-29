use std::io::{Error, ErrorKind};

use commons::models::{PartitionInfo, PeerResponse, Topic, TopicStatus};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    models::{ClusterInfo, PartitionCommand},
    partition_managers::partition_writer::{self},
    TASK_TIMEOUT_SECONDS,
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

pub fn find_nodes_for_topic(num_partitions: u8, cluster_info: &ClusterInfo) -> Vec<String> {
    // sort peers in descending order of number of partitions they have
    // and return the top N peers where N is the number of partitions
    // we will use this to distribute partitions across peers

    // sort peers by number of registered topics in descending order
    // node on which topic is being created will be excluded from the list because it will be the leader for partition 0
    let mut peers_sorted_by_number_of_topics: Vec<(usize, String)> = cluster_info
        .peers
        .iter()
        .map(|(peer_address, peer_info)| (peer_info.registered_topics.len(), peer_address.clone()))
        .collect();

    tracing::debug!(
        "Peers sorted by number of topics: {:?}",
        peers_sorted_by_number_of_topics
    );

    peers_sorted_by_number_of_topics.sort_by_key(|&(topic_count, _)| topic_count);

    let peers: Vec<String> = peers_sorted_by_number_of_topics
        .iter()
        .take(num_partitions as usize)
        .map(|(_, peer_address)| {
            tracing::debug!("Peer address: {}", peer_address);
            peer_address.clone()
        })
        .collect();
    tracing::debug!("Selected Peers: {:?}", peers);
    peers
}

pub fn create_partition_info(num_partitions: u8, cluster_info: &ClusterInfo) -> Vec<PartitionInfo> {
    // we want num_partitions - 1 because partition 0 is created locally
    let peers_for_partitions: Vec<String> = find_nodes_for_topic(num_partitions - 1, cluster_info);
    tracing::info!("Peers for partitions: {:?}", peers_for_partitions);
    assert!(
        peers_for_partitions.len() >= num_partitions as usize,
        "Not enough peers to create partitions"
    );
    let partition_zero = PartitionInfo {
        number: 0,
        leader_address: cluster_info.self_info.address.clone(),
        followers: peers_for_partitions.clone(),
    };
    let mut partitions = peers_for_partitions
        .iter()
        .enumerate()
        .map(|(index, peer_address)| {
            let mut followers = peers_for_partitions
                .iter()
                .filter(|&follower| follower != peer_address)
                .cloned()
                .collect::<Vec<String>>();
            followers.push(cluster_info.self_info.address.clone());
            PartitionInfo {
                number: (index as u8) + 1,
                leader_address: peer_address.clone(),
                followers,
            }
        })
        .collect::<Vec<PartitionInfo>>();
    partitions.insert(0, partition_zero);
    partitions
}

pub async fn create_topic(
    topic: Topic,
    data_dir_path: &str,
    cancellation_token: CancellationToken,
) -> Result<(TopicStatus, mpsc::Sender<PartitionCommand>), std::io::Error> {
    tracing::info!("Creating: {:?}", topic);
    assert_ne!(
        topic.partitions.len(),
        0,
        "Topic must have at least one partition"
    );
    let mut task_join_set: JoinSet<PeerResponse> = JoinSet::new();

    let partition_zero_sender: mpsc::Sender<PartitionCommand> =
        partition_writer::create_local_leader_partition(
            topic.name.clone(),
            topic.partitions[0].clone(),
            data_dir_path.to_string(),
            cancellation_token.clone(),
        )
        .await?;

    topic.partitions.iter().skip(1).for_each(|partition| {
        let partition_to_create = partition.clone();
        let topic_name = topic.name.clone();
        task_join_set.spawn(async move {
            partition_writer::create_remote_leader_partition(topic_name, partition_to_create).await
        });
    });
    tracing::debug!("tasks spawned to create partitions.");
    let timeout_duration = std::time::Duration::from_secs(TASK_TIMEOUT_SECONDS);

    let _ = tokio::time::timeout(timeout_duration, async {
        let task_result = task_join_set.join_all().await;
        tracing::debug!("All partition creation tasks completed.");
        for result in task_result.iter() {
            match result {
                PeerResponse::PartitionCreated {
                    topic_name,
                    partition_number,
                } => {
                    tracing::info!(
                        "Partition {} for topic {} created successfully.",
                        partition_number,
                        topic_name
                    );
                }
                PeerResponse::Error { message } => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("Failed to create partition: {}", message),
                    ));
                }
                other => {
                    tracing::warn!("Unexpected response type: {:?}", other);
                }
            }
        }
        Ok(())
    })
    .await;
    Ok((TopicStatus::ReadyToServe, partition_zero_sender))
}

#[cfg(test)]
mod tests {
    use super::*;

    use commons::models::{
        PartitionInfo, PeerCommand, PeerResponse, Topic, WalrsCommand, WalrsResponse,
    };
    use std::time::Duration;
    use tokio::{net::TcpListener, time::timeout};
    use tokio_util::task::TaskTracker;
    use tracing_test::traced_test;

    async fn start_mock_peer(peer: &str) {
        let listener = TcpListener::bind(peer).await.unwrap();
        tracing::debug!("Listening for connections on {}", peer);

        loop {
            match timeout(Duration::from_millis(200), listener.accept()).await {
                Ok(Ok((mut socket, _))) => {
                    let peer_command: WalrsCommand =
                        commons::read_from_socket::<WalrsCommand>(&mut socket)
                            .await
                            .unwrap();

                    let response = match peer_command {
                        WalrsCommand::Peer(PeerCommand::CreatePartition {
                            topic_name,
                            partition_number,
                            role,
                        }) => {
                            tracing::debug!(
                                "{}: received request. topic: {}, partition: {}, role: {:?}",
                                peer,
                                topic_name,
                                partition_number,
                                role
                            );
                            PeerResponse::PartitionCreated {
                                topic_name,
                                partition_number,
                            }
                        }
                        other => PeerResponse::Error {
                            message: format!("{}: received unexpected command: {:?}", peer, other),
                        },
                    };
                    let walrs_response = WalrsResponse::Peer(response);
                    commons::write_to_socket(&walrs_response, &mut socket)
                        .await
                        .unwrap();
                }
                Ok(Err(e)) => {
                    panic!("Failed to accept connection on {}: {}", peer, e);
                }
                Err(_) => {
                    panic!("Failed to accept connection on {}", peer);
                }
            };
        }
    }

    #[tokio::test]
    // #[ignore]
    #[traced_test]
    async fn test_create_topic() {
        /*
        While creating a topic; the following steps are taken:
        1. Find suitable broker(s) to handle the partitions. Node Manager should do this.
        2. For each partition spawn a Tokio task to create the partition.
        3. Wait for all partition creation tasks to complete.
         */
        let task_tracker: TaskTracker = TaskTracker::new();
        task_tracker.spawn(async move { start_mock_peer("127.0.0.1:8081").await });
        task_tracker.spawn(async move { start_mock_peer("127.0.0.1:8082").await });

        let partition_0: PartitionInfo = PartitionInfo {
            number: 0,
            leader_address: "127.0.0.1:8080".to_string(),
            followers: vec![
                String::from("127.0.0.1:8081"),
                String::from("127.0.0.1:8082"),
            ],
        };

        let partition_1: PartitionInfo = PartitionInfo {
            number: 1,
            leader_address: "127.0.0.1:8081".to_string(),
            followers: vec![
                String::from("127.0.0.1:8080"),
                String::from("127.0.0.1:8082"),
            ],
        };

        let partition_2: PartitionInfo = PartitionInfo {
            number: 2,
            leader_address: "127.0.0.1:8082".to_string(),
            followers: vec![
                String::from("127.0.0.1:8080"),
                String::from("127.0.0.1:8081"),
            ],
        };

        let mut topic_to_create = Topic::new(
            "test-topic".to_string(),
            Some(3), // num_partitions
            None,    // replication_factor
            None,    // retention_period_minutes
            None,
        )
        .unwrap();
        topic_to_create.partitions = vec![partition_0, partition_1, partition_2];
        let cancellation_token = CancellationToken::new();
        match create_topic(topic_to_create, "/tmp/walrs/test_data/", cancellation_token).await {
            Ok((status, _)) => {
                assert_eq!(status, TopicStatus::ReadyToServe);
            }
            Err(e) => panic!("Failed to create topic: {}", e),
        }
        task_tracker.close();
        task_tracker.wait().await;
    }
}
