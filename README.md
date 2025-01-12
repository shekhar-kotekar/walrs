# WAL-rs
A bold attempt to implement Kafka in Rust.

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

- Milestone 3:
    - Find out if Kafka has
        - leader per Topic?
        - leader per partition?
- Milestone 4:
    - Implement WAL
    - Make WAL persisting to underlying storage

### References:
Kafka official docs:https://kafka.apache.org/24/documentation.html
Confluent docs: https://docs.confluent.io/kafka/design/index.html
Raft: https://thesecretlivesofdata.com/raft/
