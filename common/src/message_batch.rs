use std::io::{Error, ErrorKind};

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::models::Message;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MessageBatch {
    pub topic_name: String,
    pub messages: Vec<Message>,
}

pub struct MessageBatchCodec;

impl Encoder<MessageBatch> for MessageBatchCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: MessageBatch, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_batch = bincode::serialize(&item).map_err(|e| Error::new(ErrorKind::Other, e))?;
        buf.put_u32(encoded_batch.len() as u32);
        buf.extend_from_slice(&encoded_batch);
        Ok(())
    }
}

impl Decoder for MessageBatchCodec {
    type Item = MessageBatch;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let len = buf.get_u32() as usize;
        if buf.len() < len {
            return Ok(None);
        }
        let encoded_batch = buf.split_to(len);
        let message_batch: MessageBatch =
            bincode::deserialize(&encoded_batch).map_err(|e| Error::new(ErrorKind::Other, e))?;
        Ok(Some(message_batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    #[test]
    fn test_encode_decode_message_batch() {
        let messages = vec![
            Message {
                payload: b"hello".to_vec(),
            },
            Message {
                payload: b"world".to_vec(),
            },
        ];
        let batch = MessageBatch {
            topic_name: "test_topic".to_string(),
            messages,
        };

        let mut codec = MessageBatchCodec;
        let mut buf = BytesMut::new();

        // Encode the batch
        codec.encode(batch.clone(), &mut buf).expect("Encoding failed");

        // Decode the batch
        let decoded = codec.decode(&mut buf).expect("Decoding failed");
        assert_eq!(decoded, Some(batch));
    }

    #[test]
    fn test_partial_decode_returns_none() {
        let messages = vec![Message {
            payload: b"partial".to_vec(),
        }];
        let batch = MessageBatch {
            topic_name: "partial_topic".to_string(),
            messages,
        };

        let mut codec = MessageBatchCodec;
        let mut buf = BytesMut::new();
        codec.encode(batch, &mut buf).expect("Encoding failed");

        // Provide only part of the buffer (less than 4 bytes)
        let mut partial_buf = BytesMut::from(&buf[..2]);
        assert!(codec.decode(&mut partial_buf).unwrap().is_none());

        // Provide only the length prefix but not the full message
        let mut partial_buf = BytesMut::from(&buf[..4]);
        assert!(codec.decode(&mut partial_buf).unwrap().is_none());

        // Provide less than the full message
        let partial_len = buf.len() - 2;
        let mut partial_buf = BytesMut::from(&buf[..partial_len]);
        assert!(codec.decode(&mut partial_buf).unwrap().is_none());
    }
}
