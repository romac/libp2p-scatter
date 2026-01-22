use std::fmt;
use std::io::{Error, ErrorKind, Result};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use prometheus_client::encoding::{EncodeLabelSet, LabelSetEncoder};

use crate::length_prefixed::{read_length_prefixed, write_length_prefixed};

const PROTOCOL_INFO: &str = "/ax/broadcast/1.0.0";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Topic {
    len: u8,
    bytes: [u8; 64],
}

impl Topic {
    pub const MAX_TOPIC_LENGTH: usize = 64;

    pub fn new(topic: &[u8]) -> Self {
        let mut bytes = [0u8; 64];
        bytes[..topic.len()].copy_from_slice(topic);
        Self {
            len: topic.len() as _,
            bytes,
        }
    }
}

impl EncodeLabelSet for Topic {
    fn encode(&self, mut encoder: LabelSetEncoder) -> fmt::Result {
        use prometheus_client::encoding::{EncodeLabelKey, EncodeLabelValue};

        let mut label_encoder = encoder.encode_label();
        let mut key_encoder = label_encoder.encode_label_key()?;
        EncodeLabelKey::encode(&"topic", &mut key_encoder)?;
        let mut value_encoder = key_encoder.encode_label_value()?;
        let value = String::from_utf8_lossy(self.as_ref());
        EncodeLabelValue::encode(&value, &mut value_encoder)?;
        value_encoder.finish()
    }
}

impl std::ops::Deref for Topic {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<[u8]> for Topic {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..(self.len as usize)]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    Subscribe(Topic),
    Broadcast(Topic, Bytes),
    Unsubscribe(Topic),
}

impl Message {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(Error::new(ErrorKind::InvalidData, "empty message"));
        }
        let topic_len = (bytes[0] >> 2) as usize;
        if bytes.len() < topic_len + 1 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "topic length out of range",
            ));
        }
        let msg_len = bytes.len() - topic_len - 1;
        let topic = Topic::new(&bytes[1..topic_len + 1]);
        Ok(match bytes[0] & 0b11 {
            0b00 => Message::Subscribe(topic),
            0b10 => Message::Unsubscribe(topic),
            0b01 => {
                let mut msg = Vec::with_capacity(msg_len);
                msg.extend_from_slice(&bytes[(topic_len + 1)..]);
                Message::Broadcast(topic, msg.into())
            }
            _ => return Err(Error::new(ErrorKind::InvalidData, "invalid header")),
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Message::Subscribe(topic) => {
                let mut buf = Vec::with_capacity(topic.len() + 1);
                buf.push((topic.len() as u8) << 2);
                buf.extend_from_slice(topic);
                buf
            }
            Message::Unsubscribe(topic) => {
                let mut buf = Vec::with_capacity(topic.len() + 1);
                buf.push((topic.len() as u8) << 2 | 0b10);
                buf.extend_from_slice(topic);
                buf
            }
            Message::Broadcast(topic, msg) => {
                let mut buf = Vec::with_capacity(topic.len() + msg.len() + 1);
                buf.push((topic.len() as u8) << 2 | 0b01);
                buf.extend_from_slice(topic);
                buf.extend_from_slice(msg);
                buf
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Message::Subscribe(topic) => 1 + topic.len(),
            Message::Unsubscribe(topic) => 1 + topic.len(),
            Message::Broadcast(topic, msg) => 1 + topic.len() + msg.len(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub max_buf_size: usize,
}

impl Config {
    pub fn max_buf_size(mut self, max_buf_size: usize) -> Self {
        self.max_buf_size = max_buf_size;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_buf_size: 1024 * 1024 * 4, // 4 MiB
        }
    }
}

impl UpgradeInfo for Config {
    type Info = &'static str;
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(PROTOCOL_INFO)
    }
}

impl<TSocket> InboundUpgrade<TSocket> for Config
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = Message;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Output>>;

    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let packet = read_length_prefixed(&mut socket, self.max_buf_size).await?;
            socket.close().await?;
            let request = Message::from_bytes(&packet)?;
            Ok(request)
        })
    }
}

impl UpgradeInfo for Message {
    type Info = &'static str;
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(PROTOCOL_INFO)
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for Message
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Output>>;

    fn upgrade_outbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let bytes = self.to_bytes();
            write_length_prefixed(&mut socket, bytes).await?;
            socket.close().await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Topic Tests ====================

    #[test]
    fn test_topic_empty() {
        let topic = Topic::new(b"");
        assert_eq!(topic.len(), 0);
        assert_eq!(topic.as_ref(), b"");
    }

    #[test]
    fn test_topic_max_length() {
        // Note: Wire format uses 6 bits for topic length, so max is 63 bytes
        // Topic::MAX_TOPIC_LENGTH is 64 for storage, but wire max is 63
        let data = [b'x'; 63];
        let topic = Topic::new(&data);
        assert_eq!(topic.len(), 63);
        assert_eq!(topic.as_ref(), &data[..]);
    }

    #[test]
    fn test_topic_various_lengths() {
        for len in [1, 16, 32, 62, 63] {
            let data: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let topic = Topic::new(&data);
            assert_eq!(topic.len(), len);
            assert_eq!(topic.as_ref(), &data[..]);
        }
    }

    #[test]
    fn test_topic_equality() {
        let t1 = Topic::new(b"hello");
        let t2 = Topic::new(b"hello");
        let t3 = Topic::new(b"world");
        assert_eq!(t1, t2);
        assert_ne!(t1, t3);
    }

    #[test]
    fn test_topic_ordering() {
        let t1 = Topic::new(b"aaa");
        let t2 = Topic::new(b"bbb");
        assert!(t1 < t2);
    }

    // ==================== Message Roundtrip Tests ====================

    #[test]
    fn test_roundtrip() {
        let topic = Topic::new(b"topic");
        let msgs = [
            Message::Broadcast(Topic::new(b""), Bytes::from_static(b"")),
            Message::Subscribe(topic),
            Message::Unsubscribe(topic),
            Message::Broadcast(topic, Bytes::from_static(b"content")),
        ];
        for msg in &msgs {
            let msg2 = Message::from_bytes(&msg.to_bytes()).unwrap();
            assert_eq!(msg, &msg2);
        }
    }

    #[test]
    fn test_roundtrip_empty_topic() {
        let topic = Topic::new(b"");
        let msgs = [
            Message::Subscribe(topic),
            Message::Unsubscribe(topic),
            Message::Broadcast(topic, Bytes::from_static(b"payload")),
        ];
        for msg in &msgs {
            let msg2 = Message::from_bytes(&msg.to_bytes()).unwrap();
            assert_eq!(msg, &msg2);
        }
    }

    #[test]
    fn test_roundtrip_max_topic() {
        // Wire format uses 6 bits for topic length, so max is 63 bytes
        let data = [b'T'; 63];
        let topic = Topic::new(&data);
        let msgs = [
            Message::Subscribe(topic),
            Message::Unsubscribe(topic),
            Message::Broadcast(topic, Bytes::from_static(b"data")),
        ];
        for msg in &msgs {
            let msg2 = Message::from_bytes(&msg.to_bytes()).unwrap();
            assert_eq!(msg, &msg2);
        }
    }

    #[test]
    fn test_roundtrip_empty_payload() {
        let topic = Topic::new(b"topic");
        let msg = Message::Broadcast(topic, Bytes::new());
        let msg2 = Message::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(msg, msg2);
    }

    #[test]
    fn test_roundtrip_large_payload() {
        let topic = Topic::new(b"topic");
        let payload = Bytes::from(vec![0xAB; 10000]);
        let msg = Message::Broadcast(topic, payload);
        let msg2 = Message::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(msg, msg2);
    }

    // ==================== Message Length Tests ====================

    #[test]
    fn test_message_len() {
        let topic = Topic::new(b"hello"); // 5 bytes
        assert_eq!(Message::Subscribe(topic).len(), 6); // 1 header + 5 topic
        assert_eq!(Message::Unsubscribe(topic).len(), 6);
        assert_eq!(
            Message::Broadcast(topic, Bytes::from_static(b"world")).len(),
            11
        ); // 1 + 5 + 5
    }

    // ==================== Error Condition Tests ====================

    #[test]
    fn test_empty_message_error() {
        let result = Message::from_bytes(&[]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_truncated_topic_error() {
        // Header says topic is 5 bytes but only 2 bytes follow
        let bytes = [0b0001_0100, b'a', b'b']; // topic_len = 5, actual = 2
        let result = Message::from_bytes(&bytes);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_invalid_message_type_error() {
        // Type bits 0b11 are invalid
        let bytes = [0b0000_0011]; // topic_len = 0, type = 0b11
        let result = Message::from_bytes(&bytes);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    #[should_panic]
    fn test_invalid_message() {
        let out_of_range = [0b0000_0100];
        Message::from_bytes(&out_of_range).unwrap();
    }

    // ==================== Wire Format Tests ====================

    #[test]
    fn test_subscribe_wire_format() {
        let topic = Topic::new(b"test");
        let msg = Message::Subscribe(topic);
        let bytes = msg.to_bytes();

        // Header: topic_len (4) << 2 | type (0b00) = 0b0001_0000 = 16
        assert_eq!(bytes[0], 16);
        assert_eq!(&bytes[1..], b"test");
    }

    #[test]
    fn test_unsubscribe_wire_format() {
        let topic = Topic::new(b"test");
        let msg = Message::Unsubscribe(topic);
        let bytes = msg.to_bytes();

        // Header: topic_len (4) << 2 | type (0b10) = 0b0001_0010 = 18
        assert_eq!(bytes[0], 18);
        assert_eq!(&bytes[1..], b"test");
    }

    #[test]
    fn test_broadcast_wire_format() {
        let topic = Topic::new(b"test");
        let msg = Message::Broadcast(topic, Bytes::from_static(b"data"));
        let bytes = msg.to_bytes();

        // Header: topic_len (4) << 2 | type (0b01) = 0b0001_0001 = 17
        assert_eq!(bytes[0], 17);
        assert_eq!(&bytes[1..5], b"test");
        assert_eq!(&bytes[5..], b"data");
    }

    // ==================== Config Tests ====================

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.max_buf_size, 4 * 1024 * 1024); // 4 MiB
    }

    #[test]
    fn test_config_builder() {
        let config = Config::default().max_buf_size(1024);
        assert_eq!(config.max_buf_size, 1024);
    }
}
