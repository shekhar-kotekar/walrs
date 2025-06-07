# WAL-rs &nbsp;&nbsp;&nbsp;<img src="walrus.svg" alt="Agni" width="32" height="32">

Highly opinionated attempt to implement Apache Kafka in Rust.

## Why are we building this?

Apache Kafka is great but we can do better using Rust. Kafka consumes lot of resources, there are alternatives like RedPanda which can outperform. We are attempting to build blend of Kafka and RedPanda using Rust to take advantage of Rust's awesome features like memory safety, being faster than Java while consuming less resources.

In this process we are aiming to learn distributed systems, some algorithms, performance monitoring and optimization techniques and many more things.

## Expected features:

- [x] Cloud Native - We believe in standing on the shoulders of the giants and want to leverage Kubernetes heavily which means that
      we will make certain assumptions about the system like nodes will always run as a Pod in Kubernetes and not as a virtual machine.
- [x] Kafka like message replication
- [x] Raft based leader election
- [x] WAL based message persistence
- [x] Low resource consumption compared to Kafka
- [x] High throughput than Kafka

## Technologies Used:

- Rust
- Tokio
- Docker
- Kubernetes

## Getting Started

Execute `make dev-setup` target which will install necessary tools like tokio-console, etc.

### Run in local

In one console execute below to run one walrs server in local

```
export POD_IP=127.0.0.1
export BROKER_CONFIG_FILE=./server/configs/<broker_number_conf>.yml
make run_server
```

Open another console and execute `cargo run -p client` to run test client which does following:

1. Connect to locally running walrs server
2. Try to create a new topic
3. Send 2 dummy messages
4. Receive first batch of messages from the walrs server

(Optional) If you want to view Tokio task details then in another console execute `tokio-console` command.

### Deployment in local k8s

Execute `make deploy` to deploy server in local kind cluster.

Execute `make deploy FAST=true` to deploy server in local kind cluster without building Docker image.

Execute `kubectl exec -it ubuntu-debug-pod --namespace walrs -- /bin/bash` to connect to debug pod.

Execute below commands inside debug pod to check if dig command is working or not.

```
apt-get update
apt-get install dnsutils
dig +short +search walrs-headless-service.walrs.svc.cluster.local
```

## Unit testing

```
cargo test <MODULE NAME> -- --nocapture
Example:
cargo test node_manager -- --nocapture
```

## Topic creation process

A client can send a topic creation request to any broker in the cluster, all the brokers are equal. Whenever broker receives a request to create a new topic, it will first check if topic already exists in the clsuter or not. Each broker sends it's own metadata to other brokers as a `heartbeat` signal and receives the same information from other brokers. Topic names are case sensitve so MyTopic is NOT same as Mytopic and both can co-exist in the cluster.

Each topic in the cluster has one `lead broker`. After checking existance of a topic, broker will check its cluster metadata to find a broker with minimum number of topics it owns.
After selecting the lead broker (lead broker can be the same broker which received the request in the first place), broker will do the following:

- Find potential follower brokers by checking how many partitions that broker is serving
- Create directory and other metadata for the topic in its local system and spawn `lead partition writer` Tokio task
- Send request to `follower brokers` to create `follower partition writer` task
- `follower brokers` can respond with either `follower created` status or `Error` status with some message string
- If all the `follower brokers` send `follower created` status then `lead broker` will send `TopicCreated` message to client
- In any of the `follower brokers` responds with `Error` or due to any other reason topic creation fails then leader will respond with `Error` status to the client.

## Producers

## Consumers

When a consumer asks for next message batch, broker first performs following validations:

1. consumer is authenticated and authorized to read from given topic
2. topic exists on the broker

Each broker in the cluster maintains a map of topic name and Tokio task MPSC sender. After validation broker checks if there is any existing Tokio reader task which can serve the request, broker will ask the topic to read next set of messages. If there is no such Tokio task then broker will spawn the new task and then send the request to this Tokio reader task.

Tokio reader task will read next set of messages from the disk, send it to the broker and continue waiting for next request from the broker until it times out. During time out it will notify broker so that broker will remove this task from its internal map. Broker will simply relay these messages back to consumer.

## Next Milestones:

- Milestone 1:

  - [x] Write crude code to respond to Leader election request. Always accept the leader for time being
  - [x] Run 3 pods with above code
  - Check if all the three pods
    - [x] can communicate with each other over UDP
    - [x] accept one of the pod as a leader
    - [x] change their state as follower

- Milestone 2: Collision cases

  - [x] When should a node be considered a leader?
    - [x] after all the nodes accept?
    - [x] or when majority of nodes accept?
  - [x] How many times leader election should happen? infinitely?

- Milestone 3: Client integration
  - Understand how ISR work in Kafka and implement the functionality
    - Implement ring buffer
  - Implement Client SDK (if possible write Python wrapper using Pyo3)
  - Deploy in k8s and test the results
- Milestone 4:
  - Implement WAL
  - Make WAL persisting to underlying storage
- Milestone 5:
  - Performance testing
    - Understand and learn how Kafka and RedPanda does stress and performance testing
    - Create same testing setup
    - Generate results
- Milestone 6:
  - TBD

### References:

Kafka official docs:https://kafka.apache.org/24/documentation.html

Confluent docs: https://docs.confluent.io/kafka/design/index.html

Raft: https://thesecretlivesofdata.com/raft/
