use bincode::{Decode, Encode};

pub const INDEX_ENTRY_SIZE: usize = std::mem::size_of::<RecordIndex>();

#[derive(Debug, Encode, Decode)]
pub struct RecordIndex {
    // total size is all the fields combined + 4 bytes extra
    // pub timestamp: i64, // 8 bytes
    pub offset: u64, // 8 bytes
    pub length: u32, // 4 bytes
}
