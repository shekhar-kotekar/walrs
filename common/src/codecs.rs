use std::io;

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::models::{Message, MessageBatch};

pub struct MessageBatchCodec;

impl Encoder<MessageBatch> for MessageBatchCodec {
    type Error = io::Error;

    fn encode(&mut self, item: MessageBatch, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.topic_name.len() + 1 + item.messages.len() * 4);

        dst.put_u8(item.topic_name.len() as u8);
        dst.put_slice(item.topic_name.as_bytes());

        dst.put_u8(item.ack_level as u8);

        dst.put_u8(item.messages.len() as u8);
        for message in item.messages {
            dst.put_u32(message.payload.len() as u32);
            dst.put_slice(&message.payload);
        }
        Ok(())
    }
}

impl Decoder for MessageBatchCodec {
    type Item = MessageBatch;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 1 {
            return Ok(None);
        }

        let topic_name_len = src.get_u8() as usize;
        let topic_name = src.split_to(topic_name_len).to_vec();

        let ack_level = src.get_u8();

        let num_messages = src.get_u8();
        let mut messages = Vec::with_capacity(num_messages as usize);
        for _ in 0..num_messages {
            let payload_len = src.get_u32() as usize;
            let payload = src.split_to(payload_len).to_vec();
            messages.push(Message { payload });
        }

        Ok(Some(MessageBatch {
            topic_name: String::from_utf8(topic_name).unwrap(),
            ack_level: ack_level.into(),
            messages,
        }))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;

    use crate::models::AckLevel;

    use super::*;

    #[test]
    fn test_message_batch_codec() {
        let mut codec = MessageBatchCodec;
        let mut encoded_message_batch = BytesMut::new();

        let message_batch = MessageBatch {
            topic_name: "dummy_topic".to_string(),
            ack_level: AckLevel::Leader,
            messages: vec![
                Message {
                    payload: "message_1_payload".as_bytes().to_vec(),
                },
                Message {
                    payload: "message_2_payload".as_bytes().to_vec(),
                },
            ],
        };

        codec
            .encode(message_batch.clone(), &mut encoded_message_batch)
            .unwrap();
        let decoded_message_batch = codec.decode(&mut encoded_message_batch).unwrap().unwrap();

        assert_eq!(message_batch, decoded_message_batch);
    }
}
