use std::io::{Error, ErrorKind};

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::models::ClusterResponse;

pub struct BrokerResponseCodec;

impl Encoder<ClusterResponse> for BrokerResponseCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: ClusterResponse, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_response =
            bincode::serialize(&item).map_err(|e| Error::new(ErrorKind::Other, e))?;
        buf.put_u32(encoded_response.len() as u32);
        buf.extend_from_slice(&encoded_response);
        Ok(())
    }
}

impl Decoder for BrokerResponseCodec {
    type Item = ClusterResponse;
    type Error = std::io::Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buffer.len() < 4 {
            return Ok(None);
        }

        let len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        if buffer.len() < 4 + len {
            return Ok(None);
        }

        buffer.advance(4);

        let encoded = buffer.split_to(len);
        let response =
            bincode::deserialize(&encoded).map_err(|e| Error::new(ErrorKind::Other, e))?;
        Ok(Some(response))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    #[test]
    fn test_broker_response_codec_encode_decode() {
        let mut codec = BrokerResponseCodec;
        let response = ClusterResponse::TopicCreated {
            topic_metadata: crate::models::TopicMetadata {
                name: "test_topic".to_string(),
                replication_factor: 2,
                retention_period_minutes: 60,
                ack_level: crate::models::AckLevel::Leader,
                partitions: vec![
                    crate::models::PartitionInfo {
                        number: 0,
                        leader_address: "broker1".to_string(),
                        follower_addresses: vec!["broker1".to_string(), "broker2".to_string()],
                    },
                    crate::models::PartitionInfo {
                        number: 1,
                        leader_address: "broker2".to_string(),
                        follower_addresses: vec!["broker2".to_string(), "broker3".to_string()],
                    },
                ],
            },
        };

        // Encode the response
        let mut buf = BytesMut::new();
        codec
            .encode(response.clone(), &mut buf)
            .expect("Encoding failed");

        // Decode the response
        let decoded = codec.decode(&mut buf).expect("Decoding failed");
        // assert_eq!(decoded, Some(response));
    }
}
