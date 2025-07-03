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
- Make
- Grafana
- Prometheus

## Getting Started

Execute `make dev-setup` target which will install necessary tools like tokio-console, etc.

## Run in kind

1. Execute `make deploy` command which will build Docker image for server and deploy a stateful set in local kind cluster.
2. Exeecute `kubectl port-forward walrs-srvr-0 8080:5056 -n walrs` so that we can send a curl request to it. Keep this terminal open.
3. In another terminal execute `cargo run -p cli`. This will start a CLI application to which we can send various commands.
4. To create a topic, give following commands in cli

```
use -b localhost:8080

create-topic -t first-topic -p 3

get-topic-info -t first-topic

send-message -m first-message-for-topic-1 -t first-topic

send-message -m second-message-for-topic-1 -t first-topic

consume -t first-topic -p 0
```

## Unit testing

```
cargo test <MODULE NAME> -- --nocapture
Example:
cargo test server -- --nocapture
```

## Monitoring

We have created a Grafana dashboard to monitor application deployed in k8s. Dashboard is exported and its JSON is saved in `server/monitoring` directory. As of now we monitor only CPU, Memory and network utilization metrics but in future we can add custom metrics like number of messages being processed by each topic, etc.

### Steps to view dashboard

1. Assumption is that Grafana and Prometheus is installed
2. In case you are running Grafana in local k8s (kind, etc.) then we need to do k8s port-forward to reach the dashboard from outside the cluster.
3. Make sure you are in correct k8s context and execute `kubectl port-forward service/grafana 3000:3000 -n monitoring`.
4. Open http://localhost:3000 in browser to access grafana.

## Topic creation process

Client sends a topic creation request to any broker in the cluster - all the brokers are equal. Whenever broker receives a request to create a new topic, it will first check if topic already exists in the cluster or not. Each broker sends it's own metadata to other brokers as a `heartbeat` signal and receives the same information from other brokers. This metadata will be used while creating the topic.

Topic names are case sensitve so MyTopic is NOT same as Mytopic and both can co-exist in the cluster.

Cluster will create N partitions and each partition will have M replications. Values for N & M are provided by client. Each partition of the topic has one `lead broker`. After checking existance of a topic, broker will check its cluster metadata to find a broker with minimum number of topics it owns.
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

- Milestone 3:

  - Make WAL persisting to underlying storage

- Milestone 4: Client integration

  - Implement Client SDK (if possible write Python wrapper using Pyo3)
  - Deploy in k8s and test the results

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
