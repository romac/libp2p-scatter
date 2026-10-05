use core::fmt;
use std::{io, iter};

use bytes::Bytes;

use asynchronous_codec::{FramedRead, FramedWrite};
use futures::future::BoxFuture;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, FutureExt, future};
use libp2p::core::UpgradeInfo;
use libp2p::{InboundUpgrade, OutboundUpgrade, StreamProtocol};

use crate::util::BytesRef;
use crate::{Codec, Config, legacy};

pub struct ProtocolConfig {
    pub protocol_name: StreamProtocol,
    pub max_message_size: usize,
    pub legacy_protocol: bool,
}

impl From<&Config> for ProtocolConfig {
    fn from(config: &Config) -> Self {
        Self {
            protocol_name: config.protocol_name.clone(),
            max_message_size: config.max_message_size,
            legacy_protocol: config.legacy_protocol,
        }
    }
}

/// Protocol names to negotiate, in order of preference.
type ProtocolNames = iter::Chain<iter::Once<StreamProtocol>, std::option::IntoIter<StreamProtocol>>;

impl UpgradeInfo for ProtocolConfig {
    type Info = StreamProtocol;
    type InfoIter = ProtocolNames;

    fn protocol_info(&self) -> Self::InfoIter {
        let legacy = self.legacy_protocol.then_some(legacy::PROTOCOL_NAME);
        iter::once(self.protocol_name.clone()).chain(legacy)
    }
}

/// A framed read half of a substream.
pub(crate) type FramedSubstreamRead<S> = FramedRead<S, Codec>;

/// A framed write half of a substream.
pub(crate) type FramedSubstreamWrite<S> = FramedWrite<S, Codec>;

/// Output of an inbound substream upgrade.
pub enum Inbound<S> {
    /// A long-lived substream for receiving messages.
    Stream(FramedSubstreamRead<S>),
    /// A message received on a legacy substream.
    Legacy(Message),
}

impl<S> InboundUpgrade<S> for ProtocolConfig
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = Inbound<S>;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;
    type Error = io::Error;

    fn upgrade_inbound(self, socket: S, info: Self::Info) -> Self::Future {
        if info == legacy::PROTOCOL_NAME {
            return legacy::read_message(socket, self.max_message_size)
                .map(|message| message.map(Inbound::Legacy))
                .boxed();
        }

        let codec = Codec::new().with_max_size(self.max_message_size);
        future::ok(Inbound::Stream(FramedRead::new(socket, codec))).boxed()
    }
}

/// Upgrade for an outbound substream.
pub enum OutboundProtocol {
    /// Open a long-lived substream for sending messages.
    Stream(ProtocolConfig),
    /// Send a single message, encoded with [`legacy::encode`], on a legacy substream.
    Legacy(Bytes),
}

/// Output of an outbound substream upgrade.
pub enum Outbound<S> {
    /// A long-lived substream for sending messages.
    Stream(FramedSubstreamWrite<S>),
    /// The remote only supports the legacy protocol.
    LegacyOnly,
    /// A message was sent on a legacy substream.
    LegacySent,
}

impl UpgradeInfo for OutboundProtocol {
    type Info = StreamProtocol;
    type InfoIter = ProtocolNames;

    fn protocol_info(&self) -> Self::InfoIter {
        match self {
            Self::Stream(config) => config.protocol_info(),
            Self::Legacy(_) => iter::once(legacy::PROTOCOL_NAME).chain(None),
        }
    }
}

impl<S> OutboundUpgrade<S> for OutboundProtocol
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = Outbound<S>;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;
    type Error = io::Error;

    fn upgrade_outbound(self, mut socket: S, info: Self::Info) -> Self::Future {
        match self {
            // A legacy substream carries a single message, so close this one
            // and send each message on its own substream with `Legacy`.
            Self::Stream(_) if info == legacy::PROTOCOL_NAME => async move {
                socket.close().await?;
                Ok(Outbound::LegacyOnly)
            }
            .boxed(),
            Self::Stream(config) => {
                let codec = Codec::new().with_max_size(config.max_message_size);
                future::ok(Outbound::Stream(FramedWrite::new(socket, codec))).boxed()
            }
            Self::Legacy(message) => async move {
                legacy::write_message(socket, message).await?;
                Ok(Outbound::LegacySent)
            }
            .boxed(),
        }
    }
}

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
#[derive(Clone, Copy)]
pub struct Topic {
    /// The actual length of the topic data.
    len: u8,
    /// Fixed-size buffer storing the topic bytes.
    bytes: [u8; 64],
}

impl PartialEq for Topic {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Topic {}

impl std::hash::Hash for Topic {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl PartialOrd for Topic {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Topic {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl Topic {
    /// Maximum length of a topic in bytes.
    pub const MAX_LENGTH: usize = 64;

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

impl fmt::Debug for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Topic")
            .field("len", &self.len)
            .field("bytes", &BytesRef(self.as_ref()))
            .finish()
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", BytesRef(self.as_ref()))
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

/// Messages used in the scatter protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// Subscribe to a topic.
    Subscribe(Topic),
    /// Broadcast a message to a topic.
    Broadcast(Topic, Bytes),
    /// Unsubscribe from a topic.
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
}
