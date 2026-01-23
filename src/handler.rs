//! Connection handler for the scatter protocol.
//!
//! This handler maintains a single long-lived bidirectional substream per connection,
//! allowing multiple messages to be sent without the overhead of reopening substreams.

use std::collections::VecDeque;
use std::io;
use std::task::{Context, Poll};

use futures::FutureExt;
use futures::future::BoxFuture;
use libp2p::core::upgrade::ReadyUpgrade;
use libp2p::swarm::handler::{
    ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
    StreamUpgradeError,
};
use libp2p::swarm::{ConnectionHandler, ConnectionHandlerEvent, StreamProtocol, SubstreamProtocol};
use tracing::{debug, trace, warn};

use crate::Config;
use crate::codec::{Codec, read_length_prefixed, write_length_prefixed};
use crate::protocol::Message;

/// Protocol identifier for scatter.
const PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/me.romac/scatter/1.0.0");

/// Maximum number of attempts to open an outbound substream before giving up.
const MAX_SUBSTREAM_ATTEMPTS: usize = 5;

/// Maximum number of pending outbound messages.
const MAX_QUEUE_SIZE: usize = 1024;

/// Event sent from the handler to the behaviour.
#[derive(Debug)]
pub enum HandlerEvent {
    /// A message was received from the remote peer.
    Received(Message),
    /// An error occurred on the substream.
    Error(io::Error),
}

/// The connection handler for the scatter protocol.
pub struct Handler {
    /// Protocol configuration.
    config: Config,
    /// State of the inbound substream.
    inbound: InboundState,
    /// State of the outbound substream.
    outbound: OutboundState,
    /// Queue of messages waiting to be sent.
    pending_messages: VecDeque<Message>,
    /// Number of failed attempts to open an outbound substream.
    outbound_substream_attempts: usize,
    /// Events to emit to the behaviour.
    pending_events: VecDeque<HandlerEvent>,
    /// Whether we've requested an outbound substream.
    outbound_substream_requested: bool,
}

type SubstreamFuture<T> = BoxFuture<'static, (Substream, io::Result<T>)>;

/// State of the inbound substream.
enum InboundState {
    /// No inbound substream yet.
    None,
    /// Waiting for the next message.
    Reading(SubstreamFuture<Message>),
}

/// State of the outbound substream.
enum OutboundState {
    /// No outbound substream yet.
    None,
    /// We have a substream ready to send messages.
    Ready(Substream),
    /// Currently sending a message.
    Sending(SubstreamFuture<()>),
    /// The substream has been closed or errored.
    Closed,
}

impl OutboundState {
    /// Take the outbound state, leaving None in its place.
    fn take(&mut self) -> Self {
        std::mem::replace(self, OutboundState::None)
    }
}

/// A negotiated substream.
type Substream = libp2p::Stream;

impl Handler {
    /// Create a new handler with the given configuration.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            inbound: InboundState::None,
            outbound: OutboundState::None,
            pending_messages: VecDeque::new(),
            outbound_substream_attempts: 0,
            pending_events: VecDeque::new(),
            outbound_substream_requested: false,
        }
    }

    /// Start reading from an inbound substream.
    fn start_inbound_read(&mut self, mut stream: Substream) {
        trace!("Starting inbound read on substream");

        let max_size = self.config.max_message_size;
        self.inbound = InboundState::Reading(Box::pin(async move {
            let result = read_message(&mut stream, max_size).await;
            (stream, result)
        }));
    }

    /// Start sending a message on the outbound substream.
    fn start_outbound_send(&mut self, mut stream: Substream, message: Message) {
        trace!(?message, "Starting outbound send on substream");

        self.outbound = OutboundState::Sending(Box::pin(async move {
            let result = send_message(&mut stream, &message).await;
            (stream, result)
        }));
    }
}

impl Default for Handler {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl ConnectionHandler for Handler {
    type FromBehaviour = Message;
    type ToBehaviour = HandlerEvent;
    type InboundProtocol = ReadyUpgrade<StreamProtocol>;
    type OutboundProtocol = ReadyUpgrade<StreamProtocol>;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(ReadyUpgrade::new(PROTOCOL_NAME), ())
    }

    fn on_behaviour_event(&mut self, message: Self::FromBehaviour) {
        // Drop messages if queue is full
        if self.pending_messages.len() >= MAX_QUEUE_SIZE {
            warn!("Dropping message: queue full");
            return;
        }

        trace!(
            ?message,
            queue_len = self.pending_messages.len(),
            "Queueing message from behaviour"
        );

        self.pending_messages.push_back(message);
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: stream,
                ..
            }) => {
                // We got an inbound substream, start reading from it
                trace!("Inbound substream negotiated, starting read");
                self.start_inbound_read(stream);
            }

            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                ..
            }) => {
                // Reset the attempt counter on success
                self.outbound_substream_attempts = 0;
                self.outbound_substream_requested = false;

                // If we have pending messages, start sending immediately
                if let Some(message) = self.pending_messages.pop_front() {
                    trace!(
                        ?message,
                        "Outbound substream negotiated, sending queued message"
                    );
                    self.start_outbound_send(stream, message);
                } else {
                    trace!("Outbound substream negotiated, no pending messages");
                    self.outbound = OutboundState::Ready(stream);
                }
            }

            ConnectionEvent::DialUpgradeError(DialUpgradeError { error, .. }) => {
                self.outbound_substream_requested = false;
                self.outbound_substream_attempts += 1;

                match error {
                    StreamUpgradeError::Timeout => {
                        debug!("Outbound substream upgrade timed out");
                    }
                    StreamUpgradeError::NegotiationFailed => {
                        debug!("Outbound substream protocol negotiation failed");
                    }
                    StreamUpgradeError::Io(e) => {
                        debug!("Outbound substream I/O error: {}", e);
                    }
                    StreamUpgradeError::Apply(v) => match v {},
                }

                if self.outbound_substream_attempts >= MAX_SUBSTREAM_ATTEMPTS {
                    warn!(
                        "Failed to open outbound substream after {} attempts, giving up",
                        MAX_SUBSTREAM_ATTEMPTS
                    );

                    self.outbound = OutboundState::Closed;

                    // Clear pending messages since we can't send them
                    self.pending_messages.clear();
                }
            }

            ConnectionEvent::ListenUpgradeError(_) => {
                // Inbound upgrade errors are not fatal, we just wait for another inbound stream
                debug!("Inbound substream upgrade failed");
            }

            _ => {}
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        trace!(
            pending_messages = self.pending_messages.len(),
            pending_events = self.pending_events.len(),
            inbound_state = ?matches!(&self.inbound, InboundState::Reading(_)),
            outbound_state = ?std::mem::discriminant(&self.outbound),
            "Handler::poll called"
        );

        // First, emit any pending events
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        // Poll the inbound substream
        match &mut self.inbound {
            InboundState::Reading(future) => {
                trace!("Polling inbound substream for messages");

                if let Poll::Ready((stream, result)) = future.poll_unpin(cx) {
                    match result {
                        Ok(message) => {
                            // Successfully read a message, continue reading
                            trace!(?message, "Received message on inbound substream");

                            self.pending_events
                                .push_back(HandlerEvent::Received(message));

                            self.start_inbound_read(stream);
                        }
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            // Stream was closed gracefully by remote
                            debug!("Inbound substream closed by remote");

                            self.inbound = InboundState::None;
                        }
                        Err(e) => {
                            // Error reading from stream
                            debug!("Error reading from inbound substream: {e}");

                            self.pending_events.push_back(HandlerEvent::Error(e));
                            self.inbound = InboundState::None;
                        }
                    }

                    // Return pending event if we added one
                    if let Some(event) = self.pending_events.pop_front() {
                        return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
                    }
                }
            }
            InboundState::None => {}
        }

        // Poll the outbound substream in a loop to handle state transitions
        loop {
            match self.outbound.take() {
                OutboundState::Sending(mut future) => {
                    match future.poll_unpin(cx) {
                        Poll::Ready((stream, Ok(()))) => {
                            // Successfully sent, check for more messages
                            trace!("Message sent successfully on outbound substream");

                            if let Some(message) = self.pending_messages.pop_front() {
                                trace!(?message, "Sending next queued message");
                                self.start_outbound_send(stream, message);
                                // Continue loop to poll the new Sending future
                            } else {
                                self.outbound = OutboundState::Ready(stream);
                                break;
                            }
                        }
                        Poll::Ready((_, Err(e))) => {
                            // Error sending, need to reopen substream
                            debug!("Error sending on outbound substream: {}", e);
                            self.outbound = OutboundState::None;
                            // Don't clear pending messages, we'll retry with a new substream
                            // Continue loop to potentially request new substream
                        }
                        Poll::Pending => {
                            self.outbound = OutboundState::Sending(future);
                            break;
                        }
                    }
                }

                OutboundState::Ready(stream) => {
                    // If we have pending messages, start sending
                    if let Some(message) = self.pending_messages.pop_front() {
                        trace!(?message, "Outbound ready, sending queued message");
                        self.start_outbound_send(stream, message);
                        // Continue loop to poll the new Sending future
                    } else {
                        self.outbound = OutboundState::Ready(stream);
                        break;
                    }
                }

                OutboundState::None => {
                    // Request a new outbound substream if we have messages and haven't given up
                    if !self.pending_messages.is_empty()
                        && !self.outbound_substream_requested
                        && self.outbound_substream_attempts < MAX_SUBSTREAM_ATTEMPTS
                    {
                        trace!(
                            pending_count = self.pending_messages.len(),
                            "Requesting outbound substream for pending messages"
                        );

                        self.outbound_substream_requested = true;

                        return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                            protocol: SubstreamProtocol::new(ReadyUpgrade::new(PROTOCOL_NAME), ()),
                        });
                    }
                    break;
                }

                OutboundState::Closed => {
                    self.outbound = OutboundState::Closed;
                    break;
                }
            }
        }

        Poll::Pending
    }

    fn connection_keep_alive(&self) -> bool {
        // Keep connection alive if we have pending messages or active substreams
        !self.pending_messages.is_empty()
            || !matches!(self.inbound, InboundState::None)
            || !matches!(self.outbound, OutboundState::None | OutboundState::Closed)
    }
}

/// Read a single message from the stream.
async fn read_message(stream: &mut Substream, max_size: usize) -> io::Result<Message> {
    let packet = read_length_prefixed(stream, max_size).await?;
    if packet.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "empty packet (stream closed)",
        ));
    }
    Codec::decode(&packet)
}

/// Send a single message on the stream.
async fn send_message(stream: &mut Substream, message: &Message) -> io::Result<()> {
    let mut buf = Vec::with_capacity(Codec::encoded_len(message));
    Codec::encode(message, &mut buf)?;
    write_length_prefixed(stream, buf).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Topic;
    use bytes::Bytes;
    use futures::executor::block_on;
    use futures::io::Cursor;
    use std::task::Poll;

    // ==================== Handler Creation Tests ====================

    #[test]
    fn test_handler_default() {
        let handler = Handler::default();
        assert!(handler.pending_messages.is_empty());
        assert_eq!(handler.outbound_substream_attempts, 0);
        assert!(!handler.outbound_substream_requested);
    }

    #[test]
    fn test_handler_with_config() {
        let config = Config::default().max_message_size(1024);
        let handler = Handler::new(config);
        assert_eq!(handler.config.max_message_size, 1024);
    }

    // ==================== Queue Management Tests ====================

    #[test]
    fn test_on_behaviour_event_queues_messages() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");
        let msg = Message::Subscribe(topic);

        handler.on_behaviour_event(msg.clone());

        assert_eq!(handler.pending_messages.len(), 1);
        assert_eq!(handler.pending_messages[0], msg);
    }

    #[test]
    fn test_queue_overflow_drops_messages() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        // Fill the queue to capacity
        for _ in 0..MAX_QUEUE_SIZE {
            handler.on_behaviour_event(Message::Subscribe(topic));
        }
        assert_eq!(handler.pending_messages.len(), MAX_QUEUE_SIZE);

        // Try to add one more - should be dropped
        handler.on_behaviour_event(Message::Unsubscribe(topic));
        assert_eq!(handler.pending_messages.len(), MAX_QUEUE_SIZE);

        // Verify all messages are Subscribe (the Unsubscribe was dropped)
        for msg in &handler.pending_messages {
            assert!(matches!(msg, Message::Subscribe(_)));
        }
    }

    #[test]
    fn test_queue_preserves_order() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        handler.on_behaviour_event(Message::Subscribe(topic));
        handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"msg")));
        handler.on_behaviour_event(Message::Unsubscribe(topic));

        assert_eq!(handler.pending_messages.len(), 3);
        assert!(matches!(handler.pending_messages[0], Message::Subscribe(_)));
        assert!(matches!(
            handler.pending_messages[1],
            Message::Broadcast(_, _)
        ));
        assert!(matches!(
            handler.pending_messages[2],
            Message::Unsubscribe(_)
        ));
    }

    // ==================== Connection Keep-Alive Tests ====================

    #[test]
    fn test_connection_keep_alive_idle() {
        let handler = Handler::default();
        // No pending messages, no active streams
        assert!(!handler.connection_keep_alive());
    }

    #[test]
    fn test_connection_keep_alive_with_pending_messages() {
        let mut handler = Handler::default();
        handler.on_behaviour_event(Message::Subscribe(Topic::new(b"topic")));
        assert!(handler.connection_keep_alive());
    }

    // ==================== Poll Tests ====================

    #[test]
    fn test_poll_emits_pending_events() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        // Manually add a pending event
        handler
            .pending_events
            .push_back(HandlerEvent::Received(Message::Subscribe(topic)));

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        match handler.poll(&mut cx) {
            Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(HandlerEvent::Received(msg))) => {
                assert_eq!(msg, Message::Subscribe(topic));
            }
            _ => panic!("Expected NotifyBehaviour with Received event"),
        }
    }

    #[test]
    fn test_poll_requests_outbound_substream_when_messages_pending() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        handler.on_behaviour_event(Message::Subscribe(topic));

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        match handler.poll(&mut cx) {
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. }) => {
                assert!(handler.outbound_substream_requested);
            }
            _ => panic!("Expected OutboundSubstreamRequest"),
        }
    }

    #[test]
    fn test_poll_does_not_request_substream_twice() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        handler.on_behaviour_event(Message::Subscribe(topic));

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // First poll should request substream
        let result = handler.poll(&mut cx);
        assert!(matches!(
            result,
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
        ));

        // Second poll should return Pending (not request again)
        let result = handler.poll(&mut cx);
        assert!(matches!(result, Poll::Pending));
    }

    #[test]
    fn test_poll_pending_when_idle() {
        let mut handler = Handler::default();

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(handler.poll(&mut cx), Poll::Pending));
    }

    #[test]
    fn test_poll_does_not_request_substream_when_closed() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        handler.outbound = OutboundState::Closed;
        handler.on_behaviour_event(Message::Subscribe(topic));

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Should return Pending, not request substream
        assert!(matches!(handler.poll(&mut cx), Poll::Pending));
    }

    #[test]
    fn test_poll_does_not_request_substream_after_max_attempts() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        handler.outbound_substream_attempts = MAX_SUBSTREAM_ATTEMPTS;
        handler.on_behaviour_event(Message::Subscribe(topic));

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Should return Pending, not request substream
        assert!(matches!(handler.poll(&mut cx), Poll::Pending));
    }

    // ==================== on_connection_event Tests ====================

    #[test]
    fn test_dial_upgrade_error_increments_attempts() {
        let mut handler = Handler::default();

        let error = StreamUpgradeError::<std::convert::Infallible>::Timeout;
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });

        handler.on_connection_event(event);

        assert_eq!(handler.outbound_substream_attempts, 1);
        assert!(!handler.outbound_substream_requested);
    }

    #[test]
    fn test_dial_upgrade_error_closes_after_max_attempts() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        // Add pending messages
        handler.on_behaviour_event(Message::Subscribe(topic));
        handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"data")));

        // Simulate max failures
        handler.outbound_substream_attempts = MAX_SUBSTREAM_ATTEMPTS - 1;

        let error = StreamUpgradeError::<std::convert::Infallible>::NegotiationFailed;
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });

        handler.on_connection_event(event);

        assert_eq!(handler.outbound_substream_attempts, MAX_SUBSTREAM_ATTEMPTS);
        assert!(matches!(handler.outbound, OutboundState::Closed));
        // Pending messages should be cleared
        assert!(handler.pending_messages.is_empty());
    }

    #[test]
    fn test_dial_upgrade_error_io_error() {
        let mut handler = Handler::default();

        let io_err = io::Error::new(io::ErrorKind::ConnectionReset, "connection reset");
        let error = StreamUpgradeError::<std::convert::Infallible>::Io(io_err);
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });

        handler.on_connection_event(event);

        assert_eq!(handler.outbound_substream_attempts, 1);
    }

    // ==================== read_message / send_message Tests ====================

    /// Helper to encode a message to a Vec for testing.
    fn encode_to_vec(msg: &Message) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Codec::encoded_len(msg));
        Codec::encode(msg, &mut buf).unwrap();
        buf
    }

    #[test]
    fn test_send_message_and_read_message_roundtrip() {
        block_on(async {
            let topic = Topic::new(b"test-topic");
            let original = Message::Broadcast(topic, Bytes::from_static(b"hello world"));

            // Write message to buffer
            let mut buf = Vec::new();
            let bytes = encode_to_vec(&original);
            crate::codec::write_length_prefixed(&mut buf, bytes)
                .await
                .unwrap();

            // Read it back
            let mut cursor = Cursor::new(buf);
            let packet = crate::codec::read_length_prefixed(&mut cursor, 1024)
                .await
                .unwrap();
            let received = Codec::decode(&packet).unwrap();

            assert_eq!(original, received);
        });
    }

    #[test]
    fn test_read_message_empty_packet_is_eof() {
        block_on(async {
            // Write a zero-length message (varint 0)
            let mut buf = Vec::new();
            crate::codec::write_length_prefixed(&mut buf, b"")
                .await
                .unwrap();

            let mut cursor = Cursor::new(buf);
            let packet = crate::codec::read_length_prefixed(&mut cursor, 1024)
                .await
                .unwrap();

            // Empty packet should be treated as EOF by read_message
            assert!(packet.is_empty());
            // Codec::decode on empty returns error
            let result = Codec::decode(&packet);
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_send_message_subscribe() {
        block_on(async {
            let topic = Topic::new(b"my-topic");
            let msg = Message::Subscribe(topic);

            let mut buf = Vec::new();
            let bytes = encode_to_vec(&msg);
            crate::codec::write_length_prefixed(&mut buf, bytes)
                .await
                .unwrap();

            // Verify we can read it back
            let mut cursor = Cursor::new(buf);
            let packet = crate::codec::read_length_prefixed(&mut cursor, 1024)
                .await
                .unwrap();
            let received = Codec::decode(&packet).unwrap();

            assert_eq!(msg, received);
        });
    }

    #[test]
    fn test_send_message_unsubscribe() {
        block_on(async {
            let topic = Topic::new(b"my-topic");
            let msg = Message::Unsubscribe(topic);

            let mut buf = Vec::new();
            let bytes = encode_to_vec(&msg);
            crate::codec::write_length_prefixed(&mut buf, bytes)
                .await
                .unwrap();

            let mut cursor = Cursor::new(buf);
            let packet = crate::codec::read_length_prefixed(&mut cursor, 1024)
                .await
                .unwrap();
            let received = Codec::decode(&packet).unwrap();

            assert_eq!(msg, received);
        });
    }
}
