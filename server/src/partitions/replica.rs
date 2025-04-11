use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicaRequest {
    FetchRequest { current_offset: u64 },
}
