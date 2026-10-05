//! Wire protocol of libp2p-scatter 0.3.
//!
//! Each message is sent on its own substream, which is closed right after
//! the message is written.
//!
//! # Wire Format
//!
//! Each message is encoded as a varint length prefix, followed by:
//! - 1 byte: header, `(topic_len << 2) | kind`
//! - `topic_len` bytes: the topic
//! - For `Broadcast` messages: the payload bytes, up to the end of the message
//!
//! Message kinds:
//! - `0b00`: Subscribe
//! - `0b01`: Broadcast
//! - `0b10`: Unsubscribe

use std::io;

use asynchronous_codec::FramedRead;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, StreamExt};
use libp2p::StreamProtocol;
use unsigned_varint::codec::UviBytes;

use crate::protocol::{Message, Topic};

/// Protocol name used by libp2p-scatter 0.3.
pub(crate) const PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/ax/broadcast/1.0.0");

/// Maximum topic length, as the header stores it on 6 bits.
const MAX_TOPIC_LENGTH: usize = 0b11_1111;

const KIND_SUBSCRIBE: u8 = 0b00;
const KIND_BROADCAST: u8 = 0b01;
const KIND_UNSUBSCRIBE: u8 = 0b10;

/// Encodes a message, without its length prefix.
///
/// Returns `None` if the topic is longer than the legacy format allows.
pub(crate) fn encode(message: &Message) -> Option<Bytes> {
    let topic = message.topic();
    if topic.len() > MAX_TOPIC_LENGTH {
        return None;
    }

    let (kind, payload) = match message {
        Message::Subscribe(_) => (KIND_SUBSCRIBE, &[][..]),
        Message::Broadcast(_, payload) => (KIND_BROADCAST, payload.as_ref()),
        Message::Unsubscribe(_) => (KIND_UNSUBSCRIBE, &[][..]),
    };

    let mut buf = BytesMut::with_capacity(1 + topic.len() + payload.len());
    buf.put_u8(((topic.len() as u8) << 2) | kind);
    buf.put_slice(topic);
    buf.put_slice(payload);
    Some(buf.freeze())
}

/// Decodes a message, without its length prefix.
fn decode(mut buf: BytesMut) -> io::Result<Message> {
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty message"));
    }

    let header = buf.get_u8();
    let topic_len = (header >> 2) as usize;

    if buf.len() < topic_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "topic length out of range",
        ));
    }

    let topic = Topic::new(&buf.split_to(topic_len));

    match header & 0b11 {
        KIND_SUBSCRIBE => Ok(Message::Subscribe(topic)),
        KIND_BROADCAST => Ok(Message::Broadcast(topic, buf.freeze())),
        KIND_UNSUBSCRIBE => Ok(Message::Unsubscribe(topic)),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "invalid header")),
    }
}

/// Reads a single message from a substream, then closes it.
pub(crate) async fn read_message<S>(socket: S, max_message_size: usize) -> io::Result<Message>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut codec = UviBytes::<Bytes>::default();
    codec.set_max_len(max_message_size);

    let mut framed = FramedRead::new(socket, codec);
    let buf = framed
        .next()
        .await
        .unwrap_or_else(|| Err(io::ErrorKind::UnexpectedEof.into()))?;
    framed.into_inner().close().await?;

    decode(buf)
}

/// Writes a single message encoded with [`encode`] to a substream, then closes it.
pub(crate) async fn write_message<S>(mut socket: S, message: Bytes) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut len = unsigned_varint::encode::usize_buffer();
    socket
        .write_all(unsigned_varint::encode::usize(message.len(), &mut len))
        .await?;
    socket.write_all(&message).await?;
    socket.close().await
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::executor::block_on;
    use futures::io::Cursor;

    fn roundtrip(message: Message) {
        let encoded = encode(&message).unwrap();
        assert_eq!(decode(BytesMut::from(&encoded[..])).unwrap(), message);
    }

    #[test]
    fn test_encode_matches_v0_3_format() {
        let topic = Topic::new(b"topic");

        assert_eq!(
            encode(&Message::Subscribe(topic)).unwrap(),
            &[5 << 2, b't', b'o', b'p', b'i', b'c'][..]
        );
        assert_eq!(
            encode(&Message::Unsubscribe(topic)).unwrap(),
            &[(5 << 2) | 0b10, b't', b'o', b'p', b'i', b'c'][..]
        );
        assert_eq!(
            encode(&Message::Broadcast(topic, Bytes::from_static(b"hi"))).unwrap(),
            &[(5 << 2) | 0b01, b't', b'o', b'p', b'i', b'c', b'h', b'i'][..]
        );
    }

    #[test]
    fn test_roundtrip() {
        let topic = Topic::new(b"topic");
        roundtrip(Message::Subscribe(topic));
        roundtrip(Message::Unsubscribe(topic));
        roundtrip(Message::Broadcast(topic, Bytes::from_static(b"content")));
        roundtrip(Message::Broadcast(Topic::new(b""), Bytes::new()));
    }

    #[test]
    fn test_max_length_topic_roundtrip() {
        let topic = Topic::new(&[b'x'; MAX_TOPIC_LENGTH]);
        roundtrip(Message::Broadcast(topic, Bytes::from_static(b"data")));
    }

    #[test]
    fn test_encode_rejects_topic_too_long() {
        let topic = Topic::new(&[b'x'; MAX_TOPIC_LENGTH + 1]);
        assert!(encode(&Message::Subscribe(topic)).is_none());
    }

    #[test]
    fn test_decode_empty_message() {
        assert!(decode(BytesMut::new()).is_err());
    }

    #[test]
    fn test_decode_truncated_topic() {
        // Subscribe, topic_len=4, but only 2 topic bytes
        assert!(decode(BytesMut::from(&[4 << 2, b'a', b'b'][..])).is_err());
    }

    #[test]
    fn test_decode_invalid_kind() {
        assert!(decode(BytesMut::from(&[0b11][..])).is_err());
    }

    #[test]
    fn test_write_then_read_message() {
        let message = Message::Broadcast(Topic::new(b"topic"), Bytes::from_static(b"payload"));

        let mut socket = Cursor::new(Vec::new());
        block_on(write_message(&mut socket, encode(&message).unwrap())).unwrap();

        socket.set_position(0);
        let read = block_on(read_message(&mut socket, 1024)).unwrap();
        assert_eq!(read, message);
    }

    #[test]
    fn test_read_message_enforces_max_size() {
        let message = Message::Broadcast(Topic::new(b"topic"), Bytes::from(vec![0; 100]));

        let mut socket = Cursor::new(Vec::new());
        block_on(write_message(&mut socket, encode(&message).unwrap())).unwrap();

        socket.set_position(0);
        assert!(block_on(read_message(&mut socket, 10)).is_err());
    }

    #[test]
    fn test_read_message_eof() {
        let mut socket = Cursor::new(Vec::new());
        let err = block_on(read_message(&mut socket, 1024)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
