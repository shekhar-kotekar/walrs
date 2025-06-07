use std::collections::HashMap;

use common::models::Topic;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    models::{BrokerRole, ClusterInfo, CommandToPeer, PartitionCommand},
    partition_managers::partition_writer::PartitionWriter,
};

pub async fn create_topic(
    topic: Topic,
    self_address: String,
    local_data_dir_path: String,
    role: BrokerRole,
    cluster_info: ClusterInfo,
    cancellation_token: CancellationToken,
) -> bool {
    tokio::select! {
        _ = cancellation_token.cancelled() => {
            tracing::info!("Cancellation token cancelled. Stopping topic creation for: {}", topic.name);
            return false;
        }
        _ = async {
            tracing::info!("Creating new topic: {:?}", topic);
            if cluster_info.brokers.len() < topic.num_partitions as usize {
                tracing::warn!(
                    "Not enough peers to create {} partitions. Topic will be created but will be under replicated. Once more peers join, walrs will move partitions to them.",
                    topic.num_partitions
                );
            }
            // step 2: create local partition writer for partition 0
            let local_partition_writer_cancellation_token = cancellation_token.child_token();
            let partition_zero = 0;
            let partition_zero_sender: mpsc::Sender<PartitionCommand> = create_local_partition_writer(&topic.name, partition_zero, &local_data_dir_path, local_partition_writer_cancellation_token).await;

            // step 3: request peers to create follower partitions for partition 0
            if role == BrokerRole::Leader {
                // step 1: for p partitions and r replication factor, we need (p * r) different partition writers
                // example: if p = 3 and r = 2, we need 6 partition writers
                let potential_peers_for_topic: HashMap<String, usize> = find_peers_for_topic(&self_address, topic.num_partitions, &cluster_info);
                request_peers_to_create_follower_partitions(&potential_peers_for_topic, &topic.name, &self_address).await;
            }
        } => {}
    }
    tracing::info!("Topic created: {}", topic.name);
    panic!("not implemented yet");
}

fn find_peers_for_topic(
    self_address: &String,
    num_partitions: u8,
    cluster_info: &ClusterInfo,
) -> HashMap<String, usize> {
    // sort peers in descending order of number of partitions they have
    // and return the top N peers where N is the number of partitions
    // we will use this to distribute partitions across peers
    tracing::debug!("Finding peers for topic with {} partitions", num_partitions);

    // sort peers by number of registered topics in descending order
    let mut peers_sorted_by_number_of_topics: Vec<(usize, String)> = cluster_info
        .brokers
        .iter()
        .map(|(peer_address, peer_info)| (peer_info.registed_topics.len(), peer_address.clone()))
        .collect();
    peers_sorted_by_number_of_topics.sort_by_key(|&(topic_count, _)| topic_count);

    tracing::debug!(
        "Peers sorted by number of topics: {:?}",
        peers_sorted_by_number_of_topics
    );

    let potential_peers: HashMap<String, usize> = peers_sorted_by_number_of_topics
        .iter()
        .filter(|&(_, peer_address)| peer_address != self_address)
        .take(num_partitions as usize)
        .enumerate()
        .map(|(index, (_, peer_address))| (peer_address.clone(), index))
        .collect();
    tracing::debug!("Potential peers for topic: {:?}", potential_peers);
    potential_peers
}

async fn create_local_partition_writer(
    topic_name: &String,
    partition_number: u8,
    local_data_dir_path: &String,
    cancellation_token: CancellationToken,
) -> mpsc::Sender<PartitionCommand> {
    let (partition_writer_tx, partition_writer_rx) = mpsc::channel::<PartitionCommand>(2);
    let mut partition_writer = PartitionWriter::new(topic_name, partition_number, local_data_dir_path);
    partition_writer
        .start(partition_writer_rx, cancellation_token)
        .await;
    partition_writer_tx
}

async fn request_peers_to_create_follower_partitions(
    peers: &HashMap<String, usize>,
    topic_name: &String,
    self_address: &String,
) -> Result<(), std::io::Error> {
    for (peer_address, partition_number) in peers {
        request_peer_to_create_follower_partitions(
            peer_address,
            topic_name,
            (partition_number + 1) as u8, // partition numbers start from 0, so we add 1 to match the partition number
            self_address.clone(),
        )
        .await;
    }
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
        role: BrokerRole::Follower {
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
        let join_handle: JoinHandle<bool> = tokio::spawn(async move {
            create_topic(
                topic_to_create,
                self_address,
                String::from("/tmp/walrs/test_data/"),
                BrokerRole::Follower {
                    leader_address: String::from("localhost:8080"),
                },
                cluster_info,
                topic_cancellation_token,
            )
            .await
        });
        assert_eq!(join_handle.await.unwrap(), true);
        cancellation_token.cancel();
    }
}
