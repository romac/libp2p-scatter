use std::io::{self, Write};

use bytes::Bytes;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::protocol::{Message, Topic};

/// Reads a length-prefixed message from the given socket.
///
/// The `max_size` parameter is the maximum size in bytes of the message that we accept. This is
/// necessary in order to avoid DoS attacks where the remote sends us a message of several
/// gigabytes.
///
/// > **Note**: Assumes that a variable-length prefix indicates the length of the message. This is
/// >           compatible with what [`write_length_prefixed`] does.
pub async fn read_length_prefixed(
    socket: &mut (impl AsyncRead + Unpin),
    max_size: usize,
) -> io::Result<Vec<u8>> {
    let len = read_varint(socket).await?;
    if len > max_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Received data size ({} bytes) exceeds maximum ({} bytes)",
                len, max_size
            ),
        ));
    }

    let mut buf = vec![0; len];
    socket.read_exact(&mut buf).await?;

    Ok(buf)
}

/// Reads a variable-length integer from the `socket`.
///
/// As a special exception, if the `socket` is empty and EOFs right at the beginning, then we
/// return `Ok(0)`.
///
/// > **Note**: This function reads bytes one by one from the `socket`. It is therefore encouraged
/// >           to use some sort of buffering mechanism.
pub async fn read_varint(socket: &mut (impl AsyncRead + Unpin)) -> Result<usize, io::Error> {
    let mut buffer = unsigned_varint::encode::usize_buffer();
    let mut buffer_len = 0;

    loop {
        match socket.read(&mut buffer[buffer_len..buffer_len + 1]).await? {
            0 => {
                // Reaching EOF before finishing to read the length is an error, unless the EOF is
                // at the very beginning of the substream, in which case we assume that the data is
                // empty.
                if buffer_len == 0 {
                    return Ok(0);
                } else {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
            n => debug_assert_eq!(n, 1),
        }

        buffer_len += 1;

        match unsigned_varint::decode::usize(&buffer[..buffer_len]) {
            Ok((len, _)) => return Ok(len),
            Err(unsigned_varint::decode::Error::Overflow) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "overflow in variable-length integer",
                ));
            }
            // TODO: why do we have a `__Nonexhaustive` variant in the error? I don't know how to process it
            // Err(unsigned_varint::decode::Error::Insufficient) => {}
            Err(_) => {}
        }
    }
}

/// Writes a message to the given socket with a length prefix appended to it. Also flushes the socket.
///
/// > **Note**: Prepends a variable-length prefix indicate the length of the message. This is
/// >           compatible with what [`read_length_prefixed`] expects.
pub async fn write_length_prefixed(
    socket: &mut (impl AsyncWrite + Unpin),
    data: impl AsRef<[u8]>,
) -> Result<(), io::Error> {
    write_varint(socket, data.as_ref().len()).await?;
    socket.write_all(data.as_ref()).await?;
    socket.flush().await?;

    Ok(())
}

/// Writes a variable-length integer to the `socket`.
///
/// > **Note**: Does **NOT** flush the socket.
pub async fn write_varint(
    socket: &mut (impl AsyncWrite + Unpin),
    len: usize,
) -> Result<(), io::Error> {
    let mut len_data = unsigned_varint::encode::usize_buffer();
    let encoded_len = unsigned_varint::encode::usize(len, &mut len_data).len();
    socket.write_all(&len_data[..encoded_len]).await?;

    Ok(())
}

/// Wire format codec for protocol messages.
///
/// This type handles encoding and decoding of [`Message`] values to/from the
/// wire format. The codec is separate from the message type to allow for
/// zero-allocation encoding via the [`encode`](Self::encode) method.
///
/// # Wire Format
///
/// Each message is encoded as:
/// - 1 byte header: `[topic_len (6 bits) | message_type (2 bits)]`
/// - `topic_len` bytes: the topic
/// - For `Broadcast` messages: the payload bytes
///
/// Message type bits:
/// - `0b00`: Subscribe
/// - `0b01`: Broadcast
/// - `0b10`: Unsubscribe
/// - `0b11`: Reserved (invalid)
pub struct Codec;

impl Codec {
    /// Wire type tag for Subscribe messages.
    const TAG_SUBSCRIBE: u8 = 0b00;
    /// Wire type tag for Broadcast messages.
    const TAG_BROADCAST: u8 = 0b01;
    /// Wire type tag for Unsubscribe messages.
    const TAG_UNSUBSCRIBE: u8 = 0b10;
    /// Mask to extract the message type from the header byte.
    const TAG_MASK: u8 = 0b11;

    /// Decodes a message from a byte slice.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The input is empty
    /// - The topic length in the header exceeds the available bytes
    /// - The message type bits are invalid (`0b11`)
    pub fn decode(bytes: &[u8]) -> io::Result<Message> {
        if bytes.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty message"));
        }
        let topic_len = (bytes[0] >> 2) as usize;
        if bytes.len() < topic_len + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "topic length out of range",
            ));
        }
        let topic = Topic::new(&bytes[1..topic_len + 1]);
        match bytes[0] & Self::TAG_MASK {
            Self::TAG_SUBSCRIBE => Ok(Message::Subscribe(topic)),
            Self::TAG_UNSUBSCRIBE => Ok(Message::Unsubscribe(topic)),
            Self::TAG_BROADCAST => {
                let payload = Bytes::copy_from_slice(&bytes[topic_len + 1..]);
                Ok(Message::Broadcast(topic, payload))
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "invalid header")),
        }
    }

    /// Encodes a message to a writer without intermediate allocation.
    ///
    /// This method writes the message directly to the provided writer,
    /// avoiding the need to allocate a temporary buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if writing to the writer fails.
    pub fn encode<W: Write>(message: &Message, writer: &mut W) -> io::Result<()> {
        match message {
            Message::Subscribe(topic) => {
                writer.write_all(&[(topic.len() as u8) << 2 | Self::TAG_SUBSCRIBE])?;
                writer.write_all(topic.as_ref())?;
            }
            Message::Unsubscribe(topic) => {
                writer.write_all(&[(topic.len() as u8) << 2 | Self::TAG_UNSUBSCRIBE])?;
                writer.write_all(topic.as_ref())?;
            }
            Message::Broadcast(topic, payload) => {
                writer.write_all(&[(topic.len() as u8) << 2 | Self::TAG_BROADCAST])?;
                writer.write_all(topic.as_ref())?;
                writer.write_all(payload)?;
            }
        }
        Ok(())
    }

    /// Returns the encoded size of a message in bytes.
    ///
    /// This is useful for pre-allocating buffers or for length-prefixed framing.
    pub fn encoded_len(message: &Message) -> usize {
        match message {
            Message::Subscribe(topic) => 1 + topic.len(),
            Message::Unsubscribe(topic) => 1 + topic.len(),
            Message::Broadcast(topic, payload) => 1 + topic.len() + payload.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::io::Cursor;

    // ==================== Round-Trip Tests ====================

    #[test]
    fn test_roundtrip_empty() {
        block_on(async {
            let data = b"";
            let mut buf = Vec::new();

            write_length_prefixed(&mut buf, data).await.unwrap();
            let mut cursor = Cursor::new(buf);
            let result = read_length_prefixed(&mut cursor, 1024).await.unwrap();

            assert_eq!(result, data);
        });
    }

    #[test]
    fn test_roundtrip_small() {
        block_on(async {
            let data = b"hello world";
            let mut buf = Vec::new();

            write_length_prefixed(&mut buf, data).await.unwrap();
            let mut cursor = Cursor::new(buf);
            let result = read_length_prefixed(&mut cursor, 1024).await.unwrap();

            assert_eq!(result, data);
        });
    }

    #[test]
    fn test_roundtrip_large() {
        block_on(async {
            let data = vec![0xABu8; 10000];
            let mut buf = Vec::new();

            write_length_prefixed(&mut buf, &data).await.unwrap();
            let mut cursor = Cursor::new(buf);
            let result = read_length_prefixed(&mut cursor, 100000).await.unwrap();

            assert_eq!(result, data);
        });
    }

    #[test]
    fn test_roundtrip_at_max_size() {
        block_on(async {
            let data = vec![0xCDu8; 1000];
            let mut buf = Vec::new();

            write_length_prefixed(&mut buf, &data).await.unwrap();
            let mut cursor = Cursor::new(buf);
            // max_size exactly equals data size
            let result = read_length_prefixed(&mut cursor, 1000).await.unwrap();

            assert_eq!(result, data);
        });
    }

    // ==================== Varint Encoding Tests ====================

    #[test]
    fn test_varint_small_value() {
        block_on(async {
            let mut buf = Vec::new();
            write_varint(&mut buf, 42).await.unwrap();

            // Small values (< 128) should use only 1 byte
            assert_eq!(buf.len(), 1);
            assert_eq!(buf[0], 42);
        });
    }

    #[test]
    fn test_varint_boundary_127() {
        block_on(async {
            let mut buf = Vec::new();
            write_varint(&mut buf, 127).await.unwrap();

            // 127 is the max single-byte value
            assert_eq!(buf.len(), 1);
            assert_eq!(buf[0], 127);
        });
    }

    #[test]
    fn test_varint_boundary_128() {
        block_on(async {
            let mut buf = Vec::new();
            write_varint(&mut buf, 128).await.unwrap();

            // 128 requires 2 bytes
            assert_eq!(buf.len(), 2);
        });
    }

    #[test]
    fn test_varint_large_value() {
        block_on(async {
            let mut buf = Vec::new();
            write_varint(&mut buf, 16384).await.unwrap();

            // Larger values need more bytes
            assert!(buf.len() >= 2);

            // Read it back
            let mut cursor = Cursor::new(buf);
            let result = read_varint(&mut cursor).await.unwrap();
            assert_eq!(result, 16384);
        });
    }

    // ==================== Error Condition Tests ====================

    #[test]
    fn test_read_exceeds_max_size() {
        block_on(async {
            let data = vec![0u8; 100];
            let mut buf = Vec::new();

            write_length_prefixed(&mut buf, &data).await.unwrap();
            let mut cursor = Cursor::new(buf);

            // Try to read with max_size smaller than data
            let result = read_length_prefixed(&mut cursor, 50).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        });
    }

    #[test]
    fn test_read_empty_stream_returns_empty() {
        block_on(async {
            let mut cursor = Cursor::new(Vec::<u8>::new());
            let result = read_length_prefixed(&mut cursor, 1024).await.unwrap();

            // Empty stream at start returns empty vec (varint reads 0)
            assert!(result.is_empty());
        });
    }

    #[test]
    fn test_read_truncated_varint() {
        block_on(async {
            // A varint that indicates more bytes should follow but doesn't
            let buf = vec![0x80]; // High bit set, needs continuation
            let mut cursor = Cursor::new(buf);

            let result = read_varint(&mut cursor).await;
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        });
    }

    // ==================== Multiple Messages Tests ====================

    #[test]
    fn test_multiple_messages_in_sequence() {
        block_on(async {
            let msg1 = b"first";
            let msg2 = b"second message";
            let msg3 = b"third";

            let mut buf = Vec::new();
            write_length_prefixed(&mut buf, msg1).await.unwrap();
            write_length_prefixed(&mut buf, msg2).await.unwrap();
            write_length_prefixed(&mut buf, msg3).await.unwrap();

            let mut cursor = Cursor::new(buf);
            let r1 = read_length_prefixed(&mut cursor, 1024).await.unwrap();
            let r2 = read_length_prefixed(&mut cursor, 1024).await.unwrap();
            let r3 = read_length_prefixed(&mut cursor, 1024).await.unwrap();

            assert_eq!(r1, msg1);
            assert_eq!(r2, msg2);
            assert_eq!(r3, msg3);
        });
    }
}
