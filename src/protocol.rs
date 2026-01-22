use std::io::{Error, Result};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};

use crate::codec::{Codec, read_length_prefixed, write_length_prefixed};

const PROTOCOL_INFO: &str = "/ax/broadcast/1.0.0";

/// A topic identifier for the broadcast protocol.
///
/// Topics are fixed-size identifiers that can hold up to [`MAX_LENGTH`](Self::MAX_LENGTH)
/// bytes. They are used to group messages so that peers can subscribe to specific topics
/// and only receive broadcasts for topics they are interested in.
///
/// The topic is stored inline with a length prefix, making it cheap to copy and compare.
///
/// # Example
///
/// ```
/// use libp2p_scatter::Topic;
///
/// let topic = Topic::new(b"my-topic");
/// assert_eq!(topic.as_ref(), b"my-topic");
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Topic {
    /// The actual length of the topic data.
    len: u8,
    /// Fixed-size buffer storing the topic bytes.
    bytes: [u8; 64],
}

impl Topic {
    /// Maximum length of a topic in bytes.
    ///
    /// This limit is determined by the wire format, which uses 6 bits to encode
    /// the topic length in the message header.
    pub const MAX_LENGTH: usize = 63;

    /// Creates a new topic from a byte slice.
    ///
    /// # Panics
    ///
    /// Panics if `topic.len()` exceeds [`MAX_LENGTH`](Self::MAX_LENGTH).
    ///
    /// # Example
    ///
    /// ```
    /// use libp2p_scatter::Topic;
    ///
    /// let topic = Topic::new(b"events");
    /// assert_eq!(topic.len(), 6);
    /// ```
    pub fn new(topic: &[u8]) -> Self {
        assert!(
            topic.len() <= Self::MAX_LENGTH,
            "topic length {} exceeds maximum {}",
            topic.len(),
            Self::MAX_LENGTH
        );
        let mut bytes = [0u8; 64];
        bytes[..topic.len()].copy_from_slice(topic);
        Self {
            len: topic.len() as _,
            bytes,
        }
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
    /// Returns the topic associated with this message.
    pub fn topic(&self) -> &Topic {
        match self {
            Self::Subscribe(topic) => topic,
            Self::Broadcast(topic, _) => topic,
            Self::Unsubscribe(topic) => topic,
        }
    }

    /// Returns the encoded size of this message in bytes.
    pub fn len(&self) -> usize {
        Codec::encoded_len(self)
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
            let request = Codec::decode(&packet)?;
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
            let mut buf = Vec::with_capacity(Codec::encoded_len(&self));
            Codec::encode(&self, &mut buf)?;
            write_length_prefixed(&mut socket, buf).await?;
            socket.close().await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;

    /// Helper to encode a message to a Vec for testing.
    fn encode_to_vec(msg: &Message) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Codec::encoded_len(msg));
        Codec::encode(msg, &mut buf).unwrap();
        buf
    }

    // ==================== Topic Tests ====================

    #[test]
    fn test_topic_empty() {
        let topic = Topic::new(b"");
        assert_eq!(topic.len(), 0);
        assert_eq!(topic.as_ref(), b"");
    }

    #[test]
    fn test_topic_max_length() {
        let data = [b'x'; Topic::MAX_LENGTH];
        let topic = Topic::new(&data);
        assert_eq!(topic.len(), Topic::MAX_LENGTH);
        assert_eq!(topic.as_ref(), &data[..]);
    }

    #[test]
    #[should_panic(expected = "exceeds maximum")]
    fn test_topic_exceeds_max_length() {
        let data = [b'x'; Topic::MAX_LENGTH + 1];
        Topic::new(&data);
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

    // ==================== Codec Roundtrip Tests ====================

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
            let msg2 = Codec::decode(&encode_to_vec(msg)).unwrap();
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
            let msg2 = Codec::decode(&encode_to_vec(msg)).unwrap();
            assert_eq!(msg, &msg2);
        }
    }

    #[test]
    fn test_roundtrip_max_topic() {
        let data = [b'T'; Topic::MAX_LENGTH];
        let topic = Topic::new(&data);
        let msgs = [
            Message::Subscribe(topic),
            Message::Unsubscribe(topic),
            Message::Broadcast(topic, Bytes::from_static(b"data")),
        ];
        for msg in &msgs {
            let msg2 = Codec::decode(&encode_to_vec(msg)).unwrap();
            assert_eq!(msg, &msg2);
        }
    }

    #[test]
    fn test_roundtrip_empty_payload() {
        let topic = Topic::new(b"topic");
        let msg = Message::Broadcast(topic, Bytes::new());
        let msg2 = Codec::decode(&encode_to_vec(&msg)).unwrap();
        assert_eq!(msg, msg2);
    }

    #[test]
    fn test_roundtrip_large_payload() {
        let topic = Topic::new(b"topic");
        let payload = Bytes::from(vec![0xAB; 10000]);
        let msg = Message::Broadcast(topic, payload);
        let msg2 = Codec::decode(&encode_to_vec(&msg)).unwrap();
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

    // ==================== Codec Error Condition Tests ====================

    #[test]
    fn test_empty_message_error() {
        let result = Codec::decode(&[]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_truncated_topic_error() {
        // Header says topic is 5 bytes but only 2 bytes follow
        let bytes = [0b0001_0100, b'a', b'b']; // topic_len = 5, actual = 2
        let result = Codec::decode(&bytes);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_invalid_message_type_error() {
        // Type bits 0b11 are invalid
        let bytes = [0b0000_0011]; // topic_len = 0, type = 0b11
        let result = Codec::decode(&bytes);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    #[should_panic]
    fn test_invalid_message() {
        let out_of_range = [0b0000_0100];
        Codec::decode(&out_of_range).unwrap();
    }

    // ==================== Wire Format Tests ====================

    #[test]
    fn test_subscribe_wire_format() {
        let topic = Topic::new(b"test");
        let msg = Message::Subscribe(topic);
        let bytes = encode_to_vec(&msg);

        // Header: topic_len (4) << 2 | type (0b00) = 0b0001_0000 = 16
        assert_eq!(bytes[0], 16);
        assert_eq!(&bytes[1..], b"test");
    }

    #[test]
    fn test_unsubscribe_wire_format() {
        let topic = Topic::new(b"test");
        let msg = Message::Unsubscribe(topic);
        let bytes = encode_to_vec(&msg);

        // Header: topic_len (4) << 2 | type (0b10) = 0b0001_0010 = 18
        assert_eq!(bytes[0], 18);
        assert_eq!(&bytes[1..], b"test");
    }

    #[test]
    fn test_broadcast_wire_format() {
        let topic = Topic::new(b"test");
        let msg = Message::Broadcast(topic, Bytes::from_static(b"data"));
        let bytes = encode_to_vec(&msg);

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
