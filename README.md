Raft reference: https://thesecretlivesofdata.com/raft/

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
