use std::io;

use ::unsigned_varint::codec::UviBytes;
use asynchronous_codec::{Decoder, Encoder};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::{Message, Topic};

/// Wire format codec for protocol messages.
///
/// This type handles encoding and decoding of [`Message`] values to/from the
/// wire format. The [`encode`](Self::encode) method writes each message
/// directly into the destination buffer, without a temporary buffer.
///
/// # Wire Format
///
/// Each message is encoded as:
/// - 1 byte: message type tag
/// - 1 byte: topic length
/// - `topic_len` bytes: the topic
/// - For `Broadcast` messages: the payload bytes (varint length-prefixed)
///
/// Message type tags:
/// - `0x00`: Subscribe
/// - `0x01`: Broadcast
/// - `0x02`: Unsubscribe
#[derive(Default)]
pub struct Codec {
    unsigned_varint: UviBytes<Bytes>,
}

impl Codec {
    /// Wire type tag for Subscribe messages.
    const TAG_SUBSCRIBE: u8 = 0x00;
    /// Wire type tag for Broadcast messages.
    const TAG_BROADCAST: u8 = 0x01;
    /// Wire type tag for Unsubscribe messages.
    const TAG_UNSUBSCRIBE: u8 = 0x02;

    /// Create a  new codec.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum size for encoded/decoded values.
    pub fn with_max_size(self, max_size: usize) -> Self {
        let mut unsigned_varint = self.unsigned_varint;
        unsigned_varint.set_max_len(max_size);
        Self { unsigned_varint }
    }

    /// Decodes a message from a byte slice.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The input is too short (missing tag or topic length)
    /// - The topic length exceeds the available bytes
    /// - The message type tag is invalid
    pub fn decode_msg(&mut self, buf: &mut BytesMut) -> io::Result<Option<Message>> {
        let mut buf = match self.unsigned_varint.decode(buf)? {
            None => return Ok(None),
            Some(b) => b,
        };

        // Need at least 2 bytes for tag and topic length
        if buf.remaining() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message too short",
            ));
        }

        let tag = buf.get_u8();
        let topic_len = buf.get_u8() as usize;

        if topic_len > Topic::MAX_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "topic length {} exceeds maximum {}",
                    topic_len,
                    Topic::MAX_LENGTH
                ),
            ));
        }

        if buf.remaining() < topic_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "topic length out of range",
            ));
        }

        let topic = Topic::new(&buf.copy_to_bytes(topic_len));

        match tag {
            Self::TAG_SUBSCRIBE => Ok(Some(Message::Subscribe(topic))),
            Self::TAG_UNSUBSCRIBE => Ok(Some(Message::Unsubscribe(topic))),
            Self::TAG_BROADCAST => {
                let payload = self.unsigned_varint.decode(&mut buf)?.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "incomplete broadcast payload")
                })?;
                Ok(Some(Message::Broadcast(topic, payload.freeze())))
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "invalid tag")),
        }
    }

    /// Encodes a message to the destination buffer.
    ///
    /// # Errors
    /// Returns an error if encoding fails.
    pub fn encode_msg(&mut self, message: Message, dst: &mut BytesMut) -> io::Result<()> {
        let (tag, topic, payload) = match message {
            Message::Subscribe(topic) => (Self::TAG_SUBSCRIBE, topic, None),
            Message::Unsubscribe(topic) => (Self::TAG_UNSUBSCRIBE, topic, None),
            Message::Broadcast(topic, payload) => (Self::TAG_BROADCAST, topic, Some(payload)),
        };

        let mut payload_len_buf = unsigned_varint::encode::usize_buffer();
        let payload_len: &[u8] = match &payload {
            Some(payload) => unsigned_varint::encode::usize(payload.len(), &mut payload_len_buf),
            None => &[],
        };
        let payload: &[u8] = payload.as_deref().unwrap_or_default();

        // The frame length is never smaller than the payload length,
        // so this check also bounds the payload.
        let frame_len = 2 + topic.len() + payload_len.len() + payload.len();
        if frame_len > self.unsigned_varint.max_len() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "len > max when encoding",
            ));
        }

        let mut frame_len_buf = unsigned_varint::encode::usize_buffer();
        let frame_len_prefix = unsigned_varint::encode::usize(frame_len, &mut frame_len_buf);

        dst.reserve(frame_len_prefix.len() + frame_len);
        dst.put_slice(frame_len_prefix);
        dst.put_u8(tag);
        dst.put_u8(topic.len() as u8);
        dst.put_slice(topic.as_ref());
        dst.put_slice(payload_len);
        dst.put_slice(payload);
        Ok(())
    }
}

impl Encoder for Codec {
    type Item<'a> = Message;
    type Error = io::Error;

    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Codec::encode_msg(self, msg, dst)
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Codec::decode_msg(self, src)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_multiple_messages_in_sequence() {
        let mut codec = Codec::new();
        let topic = Topic::new(b"test");

        let messages = vec![
            Message::Subscribe(topic),
            Message::Broadcast(topic, Bytes::from_static(b"hello")),
            Message::Broadcast(topic, Bytes::from_static(b"world")),
            Message::Unsubscribe(topic),
        ];

        // Encode all messages into a single buffer
        let mut buf = BytesMut::new();
        for msg in &messages {
            codec.encode(msg.clone(), &mut buf).unwrap();
        }

        // Decode them one by one
        let mut decoded = Vec::new();
        while let Some(msg) = codec.decode(&mut buf).unwrap() {
            decoded.push(msg);
        }

        assert_eq!(decoded, messages);
        assert!(buf.is_empty(), "buffer should be fully consumed");
    }

    #[test]
    fn test_decode_partial_message_returns_none() {
        let mut codec = Codec::new();
        let topic = Topic::new(b"test");
        let msg = Message::Broadcast(topic, Bytes::from_static(b"payload"));

        let mut full_buf = BytesMut::new();
        codec.encode_msg(msg.clone(), &mut full_buf).unwrap();

        // Try decoding with only partial data
        let mut partial = full_buf.split_to(full_buf.len() / 2);
        assert!(codec.decode(&mut partial).unwrap().is_none());

        // Add the rest and decode should succeed
        partial.extend_from_slice(&full_buf);
        let decoded = codec.decode(&mut partial).unwrap();
        assert_eq!(decoded, Some(msg));
    }

    #[test]
    fn test_broadcast_empty_payload() {
        let mut codec = Codec::new();
        let topic = Topic::new(b"test");
        let msg = Message::Broadcast(topic, Bytes::new());

        let mut buf = BytesMut::new();
        codec.encode(msg.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(msg));
    }

    #[test]
    fn test_max_length_topic_roundtrip() {
        let mut codec = Codec::new();
        let topic_bytes = [b'x'; Topic::MAX_LENGTH];
        let topic = Topic::new(&topic_bytes);

        let messages = vec![
            Message::Subscribe(topic),
            Message::Broadcast(topic, Bytes::from_static(b"data")),
            Message::Unsubscribe(topic),
        ];

        let mut buf = BytesMut::new();
        for msg in &messages {
            codec.encode(msg.clone(), &mut buf).unwrap();
        }

        let mut decoded = Vec::new();
        while let Some(msg) = codec.decode(&mut buf).unwrap() {
            decoded.push(msg);
        }

        assert_eq!(decoded, messages);
    }

    #[test]
    fn test_decode_invalid_tag() {
        let mut codec = Codec::new();

        // Encode a raw message with invalid tag (0xFF)
        let mut buf = BytesMut::new();
        // Manually construct: varint length + invalid_tag + topic_len + topic
        let msg_bytes = &[0xFF, 0x04, b't', b'e', b's', b't'];
        // Prepend varint length
        let mut len_buf = unsigned_varint::encode::usize_buffer();
        let len_slice = unsigned_varint::encode::usize(msg_bytes.len(), &mut len_buf);
        buf.extend_from_slice(len_slice);
        buf.extend_from_slice(msg_bytes);

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_decode_truncated_topic() {
        let mut codec = Codec::new();

        // Message claims 10-byte topic but only provides 2 bytes
        let msg_bytes = &[0x00, 0x0A, b'a', b'b']; // Subscribe, topic_len=10, actual=2
        let mut buf = BytesMut::new();
        let mut len_buf = unsigned_varint::encode::usize_buffer();
        let len_slice = unsigned_varint::encode::usize(msg_bytes.len(), &mut len_buf);
        buf.extend_from_slice(len_slice);
        buf.extend_from_slice(msg_bytes);

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_oversized_topic_length() {
        let mut codec = Codec::new();

        // Message with topic_len=100 (exceeds MAX_LENGTH of 64)
        let msg_bytes = &[0x00, 100, b'a', b'b']; // Subscribe, topic_len=100
        let mut buf = BytesMut::new();
        let mut len_buf = unsigned_varint::encode::usize_buffer();
        let len_slice = unsigned_varint::encode::usize(msg_bytes.len(), &mut len_buf);
        buf.extend_from_slice(len_slice);
        buf.extend_from_slice(msg_bytes);

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn test_encode_wire_format() {
        let mut codec = Codec::new();
        let topic = Topic::new(b"ab");

        let mut buf = BytesMut::new();
        codec.encode(Message::Subscribe(topic), &mut buf).unwrap();
        assert_eq!(&buf[..], &[4, 0x00, 2, b'a', b'b']);

        let mut buf = BytesMut::new();
        codec.encode(Message::Unsubscribe(topic), &mut buf).unwrap();
        assert_eq!(&buf[..], &[4, 0x02, 2, b'a', b'b']);

        let mut buf = BytesMut::new();
        let payload = Bytes::from_static(b"xyz");
        codec
            .encode(Message::Broadcast(topic, payload), &mut buf)
            .unwrap();
        assert_eq!(&buf[..], &[8, 0x01, 2, b'a', b'b', 3, b'x', b'y', b'z']);
    }

    #[test]
    fn test_encode_rejects_frame_over_max_size() {
        let topic = Topic::new(b"test");
        // Frame: tag + topic_len + 4 topic bytes + 1 varint byte + 4 payload bytes = 11
        let msg = Message::Broadcast(topic, Bytes::from_static(b"data"));

        let mut buf = BytesMut::from(&b"existing"[..]);
        let err = Codec::new()
            .with_max_size(10)
            .encode(msg.clone(), &mut buf)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(err.to_string(), "len > max when encoding");
        assert_eq!(&buf[..], b"existing");

        Codec::new()
            .with_max_size(11)
            .encode(msg, &mut buf)
            .unwrap();
        assert_eq!(buf.len(), b"existing".len() + 12);
    }

    #[test]
    fn test_encode_varint_boundaries() {
        let mut codec = Codec::new();
        let topic = Topic::new(b"ab");

        // (payload length, frame length prefix, payload length prefix)
        let cases: &[(usize, &[u8], &[u8])] = &[
            (0, &[0x05], &[0x00]),
            (122, &[0x7f], &[0x7a]),
            (123, &[0x80, 0x01], &[0x7b]),
            (127, &[0x84, 0x01], &[0x7f]),
            (128, &[0x86, 0x01], &[0x80, 0x01]),
            (16377, &[0xff, 0x7f], &[0xf9, 0x7f]),
            (16378, &[0x80, 0x80, 0x01], &[0xfa, 0x7f]),
            (16383, &[0x85, 0x80, 0x01], &[0xff, 0x7f]),
            (16384, &[0x87, 0x80, 0x01], &[0x80, 0x80, 0x01]),
        ];

        for &(payload_len, frame_prefix, payload_prefix) in cases {
            let payload = Bytes::from(vec![0xaa; payload_len]);
            let msg = Message::Broadcast(topic, payload.clone());

            let mut expected = Vec::new();
            expected.extend_from_slice(frame_prefix);
            expected.extend_from_slice(&[0x01, 2, b'a', b'b']);
            expected.extend_from_slice(payload_prefix);
            expected.extend_from_slice(&payload);

            let mut buf = BytesMut::new();
            codec.encode(msg.clone(), &mut buf).unwrap();
            assert_eq!(&buf[..], &expected[..], "payload length {payload_len}");
            assert_eq!(codec.decode(&mut buf).unwrap(), Some(msg));
        }
    }

    #[test]
    fn test_max_size_enforcement() {
        let mut codec = Codec::new().with_max_size(10);
        let topic = Topic::new(b"test");

        // Try to decode a message larger than max_size
        let large_payload = Bytes::from(vec![0u8; 100]);
        let msg = Message::Broadcast(topic, large_payload);

        let mut buf = BytesMut::new();
        // Use a codec without size limit to encode
        let mut encoder = Codec::new();
        encoder.encode(msg, &mut buf).unwrap();

        // Decoding with size limit should fail
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }
}
