use bincode::{Decode, Encode};

pub const INDEX_ENTRY_SIZE: usize = std::mem::size_of::<RecordIndex>();

#[derive(Encode, Decode)]
pub struct RecordIndex {
    // 24 bytes total
    pub timestamp: i64, // 8 bytes
    pub offset: u64,    // 8 bytes
    pub length: u32,    // 4 bytes
}
