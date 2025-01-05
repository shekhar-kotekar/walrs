use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::{models::Message, TWO_MB};

pub struct MessageCodec;

impl Encoder<Message> for MessageCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Ensure we have enough space to write the entire message
        dst.reserve(item.data.len() + 4 + item.topic_name.len());

        // Write the length of the data array
        dst.put_u32(item.data.len() as u32);

        // Write the data
        dst.extend_from_slice(&item.data);

        dst.put_u32(item.topic_name.len() as u32);

        // Write the client name
        dst.extend_from_slice(item.topic_name.as_bytes());

        Ok(())
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None); // Not enough data to read data length
        }

        let data_len = src.get_u32() as usize;
        if src.len() < data_len + 4 {
            return Ok(None); // Not enough data for the data itself
        }

        let data = src.split_to(data_len).to_vec();
        let mut data_array = [0u8; TWO_MB];
        data_array[..data_len].copy_from_slice(&data);

        if src.len() < 4 {
            return Ok(None); // Not enough data to read client name length
        }

        let topic_name_len = src.get_u32() as usize;
        if src.len() < topic_name_len {
            return Ok(None); // Not enough data for the client name
        }

        let topic_name = String::from_utf8(src.split_to(topic_name_len).to_vec())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8"))?;

        Ok(Some(Message {
            data: data_array,
            topic_name,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_and_decode_message() {
        let message = Message {
            data: [0; TWO_MB],
            topic_name: "test_topic".to_string(),
        };

        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(message.clone(), &mut buf).unwrap();
        let decoded_message = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(message.data.len(), decoded_message.data.len());
        assert_eq!(message.topic_name, decoded_message.topic_name);
    }
}
