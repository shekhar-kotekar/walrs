# WAL-rs

Highly opinionated attempt to implement Kafka in Rust.

## Why are we building this?

Apache Kafka is great but we can do better using Rust. Kafka consumes lot of resources, there are alternatives like RedPanda which can outperform. We are attempting to build Kafka using Rust to take advantage of Rust's awesome features like memory safety, being faster than Java and consuming less resources.

In this process we are aiming to learn underlying algorithms, techniques and many more things.

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

Execute `make deploy` to deploy server in local kind cluster.
Execute `make deploy FAST=true` to deploy server in local kind cluster without building Docker image.

## Unit testing

```
cargo test <MODULE NAME> -- --nocapture
Example:
cargo test node_manager -- --nocapture
```

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
