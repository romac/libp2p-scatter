//! Connection handler for the scatter protocol.
//!
//! This handler maintains a single long-lived bidirectional substream per connection,
//! allowing multiple messages to be sent without the overhead of reopening substreams.

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, Stream};
use libp2p::swarm::handler::{
    ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
    ListenUpgradeError, StreamUpgradeError,
};
use libp2p::swarm::{ConnectionHandler, ConnectionHandlerEvent, SubstreamProtocol};
use tracing::{debug, trace, warn};

use crate::protocol::{
    FramedSubstreamRead, FramedSubstreamWrite, Inbound, Message, Outbound, OutboundProtocol,
    ProtocolConfig,
};
use crate::{Config, legacy};

/// Maximum number of attempts to open an outbound substream before giving up.
const MAX_SUBSTREAM_ATTEMPTS: usize = 5;

/// Maximum number of legacy substreams being opened at the same time.
const MAX_LEGACY_SUBSTREAMS: usize = 8;

/// Event sent from the handler to the behaviour.
#[derive(Debug)]
pub enum HandlerEvent {
    /// A message was received from the remote peer.
    Received(Message),
    /// An I/O error occurred reading from the inbound substream.
    InboundError(io::Error),
    /// The inbound substream was closed by the remote peer.
    InboundClosed,
    /// The connection fell back to the legacy protocol.
    /// Sent at most once per connection.
    LegacyFallback,
    /// Outbound messages may have been lost, but the handler can still send.
    OutboundFailed,
    /// The handler stopped sending messages on this connection.
    OutboundClosed,
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
    /// Number of outbound failures in a row: failed substream upgrades and failed sends.
    outbound_substream_attempts: usize,
    /// Events to emit to the behaviour.
    pending_events: VecDeque<HandlerEvent>,
    /// Whether we've requested an outbound substream.
    outbound_substream_requested: bool,
    /// Whether the outbound sink holds data that has not been flushed yet.
    outbound_needs_flush: bool,
    /// Number of legacy substreams being opened.
    legacy_substreams: usize,
    /// Whether the connection fell back to the legacy protocol.
    legacy_fallback: bool,
}

/// State of the inbound substream.
enum InboundState {
    /// No inbound substream yet.
    None,
    /// Active framed reader for receiving messages.
    Active(FramedSubstreamRead<libp2p::Stream>),
}

/// State of the outbound substream.
enum OutboundState {
    /// No outbound substream yet.
    None,
    /// Active framed writer ready to send messages.
    Ready(FramedSubstreamWrite<libp2p::Stream>),
    /// The remote only supports the legacy protocol, which uses
    /// a new substream for each message.
    Legacy,
    /// The substream has been closed or errored.
    Closed,
}

impl OutboundState {
    /// Take the outbound state, leaving None in its place.
    fn take(&mut self) -> Self {
        std::mem::replace(self, OutboundState::None)
    }
}

/// Result of polling the inbound substream.
enum InboundPollResult {
    /// A message was received.
    Received(Message),
    /// An error occurred.
    Error(io::Error),
    /// The stream was closed by the remote.
    Closed,
    /// No data available yet.
    Pending,
}

/// Result of polling the outbound substream.
enum OutboundPollResult {
    /// Need to request a new outbound substream.
    RequestSubstream(OutboundProtocol),
    /// No action needed (either sent messages or waiting).
    Continue,
}

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
            outbound_needs_flush: false,
            legacy_substreams: 0,
            legacy_fallback: false,
        }
    }

    /// Poll the inbound substream for incoming messages.
    fn poll_inbound(&mut self, cx: &mut Context<'_>) -> InboundPollResult {
        let framed = match &mut self.inbound {
            InboundState::Active(framed) => framed,
            InboundState::None => return InboundPollResult::Pending,
        };

        match Pin::new(framed).poll_next(cx) {
            Poll::Ready(Some(Ok(message))) => {
                trace!(?message, "Received message on inbound substream");
                InboundPollResult::Received(message)
            }
            Poll::Ready(Some(Err(e))) => {
                debug!("Error reading from inbound substream: {e}");
                self.inbound = InboundState::None;
                InboundPollResult::Error(e)
            }
            Poll::Ready(None) => {
                debug!("Inbound substream closed by remote");
                self.inbound = InboundState::None;
                InboundPollResult::Closed
            }
            Poll::Pending => InboundPollResult::Pending,
        }
    }

    /// Poll the outbound substream and send any pending messages.
    fn poll_outbound(&mut self, cx: &mut Context<'_>) -> OutboundPollResult {
        loop {
            match self.outbound.take() {
                OutboundState::Ready(mut sink) => {
                    match self.try_send_message(&mut sink, cx) {
                        SendResult::Sent => {
                            // Message sent, put sink back and try to send more
                            self.outbound = OutboundState::Ready(sink);
                        }
                        SendResult::Pending(message) => {
                            // Not ready to send, put message and sink back
                            self.pending_messages.push_front(message);
                            self.outbound = OutboundState::Ready(sink);
                            return OutboundPollResult::Continue;
                        }
                        SendResult::Error => {
                            // Error occurred, request a new substream for the remaining messages
                            self.outbound = OutboundState::None;
                            self.outbound_needs_flush = false;
                            self.on_outbound_failure(true);
                        }
                        SendResult::NothingToSend => {
                            self.outbound = OutboundState::Ready(sink);
                            return OutboundPollResult::Continue;
                        }
                    }
                }

                OutboundState::None => {
                    if self.should_request_substream() {
                        trace!(
                            pending_count = self.pending_messages.len(),
                            "Requesting outbound substream for pending messages"
                        );
                        self.outbound_substream_requested = true;
                        return OutboundPollResult::RequestSubstream(OutboundProtocol::Stream(
                            ProtocolConfig::from(&self.config),
                        ));
                    }
                    return OutboundPollResult::Continue;
                }

                OutboundState::Legacy => {
                    self.outbound = OutboundState::Legacy;
                    if self.legacy_substreams >= MAX_LEGACY_SUBSTREAMS {
                        return OutboundPollResult::Continue;
                    }
                    while let Some(message) = self.pending_messages.pop_front() {
                        let Some(encoded) = legacy::encode(&message) else {
                            warn!(
                                topic = %message.topic(),
                                "Dropping message: topic too long for the legacy protocol"
                            );
                            continue;
                        };
                        trace!(?message, "Requesting legacy substream for message");
                        self.legacy_substreams += 1;
                        return OutboundPollResult::RequestSubstream(OutboundProtocol::Legacy(
                            encoded,
                        ));
                    }
                    return OutboundPollResult::Continue;
                }

                OutboundState::Closed => {
                    self.outbound = OutboundState::Closed;
                    return OutboundPollResult::Continue;
                }
            }
        }
    }

    /// Try to send the next pending message on the sink.
    ///
    /// When there is no message to send, flush any data still buffered in the sink.
    fn try_send_message(
        &mut self,
        sink: &mut FramedSubstreamWrite<libp2p::Stream>,
        cx: &mut Context<'_>,
    ) -> SendResult {
        let message = match self.pending_messages.pop_front() {
            Some(msg) => msg,
            None => return self.flush_outbound(sink, cx),
        };

        trace!(?message, "Sending message on outbound substream");

        // Check if sink is ready
        match Pin::new(&mut *sink).poll_ready(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => {
                debug!("Error on outbound substream: {}", e);
                self.pending_messages.push_front(message);
                return SendResult::Error;
            }
            Poll::Pending => return SendResult::Pending(message),
        }

        // Start sending the message
        if let Err(e) = Pin::new(&mut *sink).start_send(message) {
            debug!("Error sending on outbound substream: {}", e);
            return SendResult::Error;
        }

        // Flush the message
        match Pin::new(&mut *sink).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                trace!("Message sent successfully on outbound substream");
                self.outbound_needs_flush = false;
                self.outbound_substream_attempts = 0;
                SendResult::Sent
            }
            Poll::Ready(Err(e)) => {
                debug!("Error flushing on outbound substream: {}", e);
                SendResult::Error
            }
            Poll::Pending => {
                // Flush is pending but message was accepted,
                // keep flushing on later polls until it completes
                self.outbound_needs_flush = true;
                SendResult::Sent
            }
        }
    }

    /// Flush data still buffered in the sink, if any.
    fn flush_outbound(
        &mut self,
        sink: &mut FramedSubstreamWrite<libp2p::Stream>,
        cx: &mut Context<'_>,
    ) -> SendResult {
        if !self.outbound_needs_flush {
            return SendResult::NothingToSend;
        }

        match Pin::new(&mut *sink).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                trace!("Flushed outbound substream");
                self.outbound_needs_flush = false;
                self.outbound_substream_attempts = 0;
                SendResult::NothingToSend
            }
            Poll::Ready(Err(e)) => {
                debug!("Error flushing on outbound substream: {}", e);
                SendResult::Error
            }
            Poll::Pending => SendResult::NothingToSend,
        }
    }

    /// Record an outbound failure and give up after too many failures in a row.
    /// Otherwise, notify the behaviour if messages may have been lost.
    fn on_outbound_failure(&mut self, messages_lost: bool) {
        self.outbound_substream_attempts += 1;

        if self.outbound_substream_attempts < MAX_SUBSTREAM_ATTEMPTS {
            if messages_lost {
                self.pending_events.push_back(HandlerEvent::OutboundFailed);
            }
            return;
        }

        if !matches!(self.outbound, OutboundState::Closed) {
            warn!(
                "Outbound substream failed {} times in a row, giving up",
                MAX_SUBSTREAM_ATTEMPTS
            );

            self.outbound = OutboundState::Closed;

            // Clear pending messages since we can't send them
            self.pending_messages.clear();

            self.pending_events.push_back(HandlerEvent::OutboundClosed);
        }
    }

    /// Notify the behaviour the first time the connection falls back to the legacy protocol.
    fn on_legacy_fallback(&mut self) {
        if !self.legacy_fallback {
            self.legacy_fallback = true;
            self.pending_events.push_back(HandlerEvent::LegacyFallback);
        }
    }

    /// Check if we should request a new outbound substream.
    fn should_request_substream(&self) -> bool {
        !self.pending_messages.is_empty()
            && !self.outbound_substream_requested
            && self.outbound_substream_attempts < MAX_SUBSTREAM_ATTEMPTS
    }
}

/// Result of attempting to send a message.
enum SendResult {
    /// Message was sent successfully.
    Sent,
    /// Sink not ready, message returned for retry.
    Pending(Message),
    /// An error occurred.
    Error,
    /// No message to send.
    NothingToSend,
}

impl Default for Handler {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl ConnectionHandler for Handler {
    type FromBehaviour = Message;
    type ToBehaviour = HandlerEvent;
    type InboundProtocol = ProtocolConfig;
    type OutboundProtocol = OutboundProtocol;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(ProtocolConfig::from(&self.config), ())
    }

    fn on_behaviour_event(&mut self, message: Self::FromBehaviour) {
        // Drop messages if outbound substream is permanently closed
        if matches!(self.outbound, OutboundState::Closed) {
            warn!("Dropping message: outbound substream permanently closed");
            return;
        }

        // Subscription messages carry state, so they are not dropped when the queue is full.
        // Keep only the latest one for each topic, which bounds them by the number of topics.
        if let Message::Subscribe(topic) | Message::Unsubscribe(topic) = message {
            let pending = self.pending_messages.iter_mut().find(|pending| {
                matches!(pending, Message::Subscribe(t) | Message::Unsubscribe(t) if *t == topic)
            });

            match pending {
                Some(pending) => *pending = message,
                None => self.pending_messages.push_back(message),
            }
            return;
        }

        // Drop messages if queue is full
        if self.pending_messages.len() >= self.config.max_outbound_queue_size {
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
                protocol, ..
            }) => {
                match protocol {
                    Inbound::Stream(stream) => {
                        // We got an inbound substream, create a framed reader for it
                        trace!("Inbound substream negotiated");
                        self.inbound = InboundState::Active(stream);
                    }
                    Inbound::Legacy(message) => {
                        trace!(?message, "Received message on legacy substream");
                        self.on_legacy_fallback();
                        self.pending_events
                            .push_back(HandlerEvent::Received(message));
                    }
                }
            }

            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol, ..
            }) => {
                match protocol {
                    Outbound::Stream(stream) => {
                        // Create a framed writer for the outbound substream
                        trace!("Outbound substream negotiated");
                        self.outbound_substream_requested = false;
                        self.outbound = OutboundState::Ready(stream);
                    }
                    Outbound::LegacyOnly => {
                        trace!("Remote only supports the legacy protocol");
                        self.outbound_substream_requested = false;
                        self.outbound = OutboundState::Legacy;
                        self.on_legacy_fallback();
                    }
                    Outbound::LegacySent => {
                        trace!("Message sent on legacy substream");
                        self.legacy_substreams -= 1;
                        self.outbound_substream_attempts = 0;
                    }
                }
            }

            ConnectionEvent::DialUpgradeError(DialUpgradeError { error, .. }) => {
                let messages_lost = match self.outbound {
                    // The message sent on this legacy substream is lost.
                    OutboundState::Legacy => {
                        self.legacy_substreams -= 1;
                        true
                    }
                    _ => {
                        self.outbound_substream_requested = false;
                        false
                    }
                };

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
                    StreamUpgradeError::Apply(e) => {
                        debug!("Outbound substream upgrade failed: {}", e);
                    }
                }

                self.on_outbound_failure(messages_lost);
            }

            ConnectionEvent::ListenUpgradeError(ListenUpgradeError { error, .. }) => {
                // Inbound upgrade errors are not fatal, we just wait for another inbound stream
                debug!("Inbound substream upgrade failed: {}", error);
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
            inbound_state = ?matches!(&self.inbound, InboundState::Active(_)),
            outbound_state = ?std::mem::discriminant(&self.outbound),
            "Handler::poll called"
        );

        // First, emit any pending events
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        // Poll the inbound substream for messages
        match self.poll_inbound(cx) {
            InboundPollResult::Received(message) => {
                return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                    HandlerEvent::Received(message),
                ));
            }
            InboundPollResult::Error(e) => {
                return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                    HandlerEvent::InboundError(e),
                ));
            }
            InboundPollResult::Closed => {
                return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                    HandlerEvent::InboundClosed,
                ));
            }
            InboundPollResult::Pending => {}
        }

        // Poll the outbound substream for sending messages
        if let OutboundPollResult::RequestSubstream(protocol) = self.poll_outbound(cx) {
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(protocol, ()),
            });
        }

        // Emit events from failed sends now, since nothing else may wake the handler
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        Poll::Pending
    }

    fn connection_keep_alive(&self) -> bool {
        // Keep connection alive if we have pending messages, active substreams,
        // or a remote using the legacy protocol
        !self.pending_messages.is_empty()
            || !matches!(self.inbound, InboundState::None)
            || !matches!(self.outbound, OutboundState::None | OutboundState::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Topic;
    use bytes::Bytes;
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

        let max_queue_size = handler.config.max_outbound_queue_size;

        // Fill the queue to capacity
        for _ in 0..max_queue_size {
            handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"msg")));
        }
        assert_eq!(handler.pending_messages.len(), max_queue_size);

        // Try to add one more - should be dropped
        handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"more")));
        assert_eq!(handler.pending_messages.len(), max_queue_size);

        // Verify the last message was dropped
        for msg in &handler.pending_messages {
            assert!(matches!(msg, Message::Broadcast(_, payload) if payload.as_ref() == b"msg"));
        }
    }

    #[test]
    fn test_queue_preserves_order() {
        let mut handler = Handler::default();
        let topic1 = Topic::new(b"topic1");
        let topic2 = Topic::new(b"topic2");

        handler.on_behaviour_event(Message::Subscribe(topic1));
        handler.on_behaviour_event(Message::Broadcast(topic1, Bytes::from_static(b"msg")));
        handler.on_behaviour_event(Message::Unsubscribe(topic2));

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

        let error = StreamUpgradeError::<io::Error>::Timeout;
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

        let error = StreamUpgradeError::<io::Error>::NegotiationFailed;
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
        let error = StreamUpgradeError::<io::Error>::Io(io_err);
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });

        handler.on_connection_event(event);

        assert_eq!(handler.outbound_substream_attempts, 1);
    }

    // ==================== Queue Overflow Behavior Tests ====================

    #[test]
    fn test_queue_overflow_with_custom_config() {
        // Test with a smaller queue size
        let config = Config::default().max_outbound_queue_size(10);
        let mut handler = Handler::new(config);
        let topic = Topic::new(b"topic");

        // Fill the queue
        for i in 0..10 {
            handler
                .on_behaviour_event(Message::Broadcast(topic, Bytes::from(format!("msg-{}", i))));
        }
        assert_eq!(handler.pending_messages.len(), 10);

        // Additional messages should be dropped
        handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"more")));

        assert_eq!(handler.pending_messages.len(), 10);

        // Verify the original messages are preserved (FIFO order)
        if let Message::Broadcast(_, payload) = &handler.pending_messages[0] {
            assert_eq!(payload.as_ref(), b"msg-0");
        } else {
            panic!("Expected Broadcast message");
        }
    }

    #[test]
    fn test_queue_drains_correctly_when_space_available() {
        let config = Config::default().max_outbound_queue_size(5);
        let mut handler = Handler::new(config);
        let topic = Topic::new(b"topic");

        // Fill the queue
        for _ in 0..5 {
            handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"msg")));
        }
        assert_eq!(handler.pending_messages.len(), 5);

        // Simulate draining one message (as if it was sent)
        handler.pending_messages.pop_front();
        assert_eq!(handler.pending_messages.len(), 4);

        // Now we should be able to add another message
        handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"new")));
        assert_eq!(handler.pending_messages.len(), 5);

        // Verify the new message is at the back
        assert!(matches!(
            handler.pending_messages.back(),
            Some(Message::Broadcast(_, payload)) if payload.as_ref() == b"new"
        ));
    }

    #[test]
    fn test_subscription_messages_are_kept_when_queue_is_full() {
        let config = Config::default().max_outbound_queue_size(3);
        let mut handler = Handler::new(config);
        let topic1 = Topic::new(b"topic1");
        let topic2 = Topic::new(b"topic2");

        handler.on_behaviour_event(Message::Subscribe(topic1));
        handler.on_behaviour_event(Message::Broadcast(topic1, Bytes::from_static(b"data")));
        handler.on_behaviour_event(Message::Broadcast(topic1, Bytes::from_static(b"data")));
        assert_eq!(handler.pending_messages.len(), 3);

        // A broadcast is dropped
        handler.on_behaviour_event(Message::Broadcast(topic1, Bytes::from_static(b"more")));
        assert_eq!(handler.pending_messages.len(), 3);

        // A subscription message replaces the pending one for the same topic
        handler.on_behaviour_event(Message::Subscribe(topic1));
        handler.on_behaviour_event(Message::Unsubscribe(topic1));
        assert_eq!(handler.pending_messages.len(), 3);
        assert_eq!(handler.pending_messages[0], Message::Unsubscribe(topic1));

        // A subscription message for another topic is added
        handler.on_behaviour_event(Message::Subscribe(topic2));
        assert_eq!(handler.pending_messages.len(), 4);
        assert_eq!(handler.pending_messages[3], Message::Subscribe(topic2));
    }

    // ==================== Substream Recovery Tests ====================

    #[test]
    fn test_substream_retry_preserves_messages() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        // Add messages to queue
        handler.on_behaviour_event(Message::Subscribe(topic));
        handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"important")));

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // First poll requests substream
        let result = handler.poll(&mut cx);
        assert!(matches!(
            result,
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
        ));
        assert_eq!(handler.pending_messages.len(), 2);

        // Simulate dial error (not max attempts yet)
        let error = StreamUpgradeError::<io::Error>::Timeout;
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });
        handler.on_connection_event(event);

        // Messages should still be pending
        assert_eq!(handler.pending_messages.len(), 2);
        assert_eq!(handler.outbound_substream_attempts, 1);

        // Next poll should request another substream
        let result = handler.poll(&mut cx);
        assert!(matches!(
            result,
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
        ));
    }

    #[test]
    fn test_substream_retry_up_to_max_attempts() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        handler.on_behaviour_event(Message::Subscribe(topic));

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Simulate failures up to MAX_SUBSTREAM_ATTEMPTS - 1
        for attempt in 0..(MAX_SUBSTREAM_ATTEMPTS - 1) {
            // Request substream
            let result = handler.poll(&mut cx);
            assert!(
                matches!(
                    result,
                    Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
                ),
                "Should request substream on attempt {}",
                attempt
            );

            // Simulate failure
            let error = StreamUpgradeError::<io::Error>::Timeout;
            let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });
            handler.on_connection_event(event);

            // Messages should still be pending
            assert_eq!(
                handler.pending_messages.len(),
                1,
                "Messages should be preserved after attempt {}",
                attempt
            );
        }

        // Request substream one more time (this is the MAX_SUBSTREAM_ATTEMPTS-th request)
        let result = handler.poll(&mut cx);
        assert!(matches!(
            result,
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
        ));

        // Final failure should close and clear messages
        let error = StreamUpgradeError::<io::Error>::Timeout;
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });
        handler.on_connection_event(event);

        assert!(matches!(handler.outbound, OutboundState::Closed));
        assert!(
            handler.pending_messages.is_empty(),
            "Messages should be cleared after max attempts"
        );

        // The behaviour is notified, and further polls should not request substreams
        let result = handler.poll(&mut cx);
        assert!(matches!(
            result,
            Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                HandlerEvent::OutboundClosed
            ))
        ));
        let result = handler.poll(&mut cx);
        assert!(matches!(result, Poll::Pending));
    }

    #[test]
    fn test_new_messages_after_substream_closed() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        // Close the substream
        handler.outbound = OutboundState::Closed;

        // Try to add messages - they should be dropped, not queued
        handler.on_behaviour_event(Message::Subscribe(topic));
        handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"data")));

        // Messages are dropped when substream is permanently closed
        assert_eq!(handler.pending_messages.len(), 0);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Poll should return Pending
        let result = handler.poll(&mut cx);
        assert!(matches!(result, Poll::Pending));
    }

    // ==================== Connection Keep-Alive Edge Cases ====================

    #[test]
    fn test_keep_alive_with_ready_outbound() {
        let mut handler = Handler::default();

        // Simulate having a ready outbound substream
        // We can't easily create a real Stream, but we can verify the logic
        // by checking that the handler would keep alive in certain states

        // With no messages and no active streams, should not keep alive
        assert!(!handler.connection_keep_alive());

        // With pending messages, should keep alive
        handler.on_behaviour_event(Message::Subscribe(Topic::new(b"topic")));
        assert!(handler.connection_keep_alive());
    }

    #[test]
    fn test_keep_alive_false_when_closed() {
        let mut handler = Handler {
            outbound: OutboundState::Closed,
            ..Handler::default()
        };

        // Messages are dropped when substream is closed, so queue stays empty
        handler.on_behaviour_event(Message::Subscribe(Topic::new(b"topic")));
        assert_eq!(handler.pending_messages.len(), 0);

        // No messages and Closed state, should not keep alive
        assert!(!handler.connection_keep_alive());
    }

    // ==================== Legacy Protocol Tests ====================

    #[test]
    fn test_legacy_only_remote_switches_to_legacy_mode() {
        let mut handler = Handler::default();
        let message = Message::Subscribe(Topic::new(b"topic"));
        handler.on_behaviour_event(message.clone());

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // First, negotiate a long-lived substream
        match handler.poll(&mut cx) {
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { protocol }) => {
                assert!(matches!(protocol.upgrade(), OutboundProtocol::Stream(_)));
            }
            _ => panic!("Expected OutboundSubstreamRequest"),
        }

        // The remote only supports the legacy protocol
        handler.on_connection_event(ConnectionEvent::FullyNegotiatedOutbound(
            FullyNegotiatedOutbound {
                protocol: Outbound::LegacyOnly,
                info: (),
            },
        ));
        assert!(matches!(handler.outbound, OutboundState::Legacy));
        assert!(!handler.outbound_substream_requested);

        // The behaviour is notified of the fallback
        assert!(matches!(
            handler.poll(&mut cx),
            Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                HandlerEvent::LegacyFallback
            ))
        ));

        // The message is sent on its own legacy substream
        match handler.poll(&mut cx) {
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { protocol }) => {
                match protocol.upgrade() {
                    OutboundProtocol::Legacy(encoded) => {
                        assert_eq!(*encoded, legacy::encode(&message).unwrap());
                    }
                    _ => panic!("Expected legacy upgrade"),
                }
            }
            _ => panic!("Expected OutboundSubstreamRequest"),
        }
        assert!(handler.pending_messages.is_empty());
        assert_eq!(handler.legacy_substreams, 1);
    }

    #[test]
    fn test_legacy_mode_limits_concurrent_substreams() {
        let mut handler = Handler {
            outbound: OutboundState::Legacy,
            ..Handler::default()
        };
        let topic = Topic::new(b"topic");

        for _ in 0..=MAX_LEGACY_SUBSTREAMS {
            handler.on_behaviour_event(Message::Broadcast(topic, Bytes::from_static(b"data")));
        }

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        for _ in 0..MAX_LEGACY_SUBSTREAMS {
            assert!(matches!(
                handler.poll(&mut cx),
                Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
            ));
        }

        // All legacy substreams are in use
        assert!(matches!(handler.poll(&mut cx), Poll::Pending));
        assert_eq!(handler.pending_messages.len(), 1);

        // Sending a message frees a legacy substream
        handler.on_connection_event(ConnectionEvent::FullyNegotiatedOutbound(
            FullyNegotiatedOutbound {
                protocol: Outbound::LegacySent,
                info: (),
            },
        ));
        assert!(matches!(
            handler.poll(&mut cx),
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
        ));
        assert!(handler.pending_messages.is_empty());
    }

    #[test]
    fn test_legacy_mode_dial_error_frees_substream() {
        let mut handler = Handler {
            outbound: OutboundState::Legacy,
            legacy_substreams: 1,
            ..Handler::default()
        };

        let error = StreamUpgradeError::<io::Error>::Timeout;
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });
        handler.on_connection_event(event);

        assert_eq!(handler.legacy_substreams, 0);
        assert_eq!(handler.outbound_substream_attempts, 1);
        assert!(matches!(handler.outbound, OutboundState::Legacy));
    }

    /// Simulate a failed upgrade of an outbound substream.
    fn fail_outbound(handler: &mut Handler) {
        let error = StreamUpgradeError::<io::Error>::Timeout;
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });
        handler.on_connection_event(event);
    }

    fn count_events(handler: &Handler, f: impl Fn(&HandlerEvent) -> bool) -> usize {
        handler
            .pending_events
            .iter()
            .filter(|event| f(event))
            .count()
    }

    #[test]
    fn test_legacy_mode_dial_error_notifies_outbound_failed() {
        let mut handler = Handler {
            outbound: OutboundState::Legacy,
            legacy_substreams: 2,
            ..Handler::default()
        };

        fail_outbound(&mut handler);
        fail_outbound(&mut handler);

        let failures = count_events(&handler, |e| matches!(e, HandlerEvent::OutboundFailed));
        assert_eq!(failures, 2);
    }

    #[test]
    fn test_failed_subscription_retry_is_notified_again() {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let topic = Topic::new(b"topic");

        let mut handler = Handler {
            outbound: OutboundState::Legacy,
            ..Handler::default()
        };

        handler.on_behaviour_event(Message::Subscribe(topic));
        assert!(matches!(
            handler.poll(&mut cx),
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
        ));
        fail_outbound(&mut handler);
        assert!(matches!(
            handler.poll(&mut cx),
            Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                HandlerEvent::OutboundFailed
            ))
        ));

        // The behaviour sends the subscription again, and this message is lost too
        handler.on_behaviour_event(Message::Subscribe(topic));
        assert!(matches!(
            handler.poll(&mut cx),
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. })
        ));
        fail_outbound(&mut handler);
        assert!(matches!(
            handler.poll(&mut cx),
            Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                HandlerEvent::OutboundFailed
            ))
        ));
    }

    #[test]
    fn test_lost_legacy_messages_count_toward_the_failure_limit() {
        let mut handler = Handler {
            outbound: OutboundState::Legacy,
            legacy_substreams: MAX_SUBSTREAM_ATTEMPTS,
            ..Handler::default()
        };

        for _ in 0..MAX_SUBSTREAM_ATTEMPTS {
            fail_outbound(&mut handler);
        }

        assert!(matches!(handler.outbound, OutboundState::Closed));
        let failures = count_events(&handler, |e| matches!(e, HandlerEvent::OutboundFailed));
        assert_eq!(failures, MAX_SUBSTREAM_ATTEMPTS - 1);
        assert!(matches!(
            handler.pending_events.back(),
            Some(HandlerEvent::OutboundClosed)
        ));
    }

    #[test]
    fn test_sent_legacy_message_resets_the_failure_count() {
        let mut handler = Handler {
            outbound: OutboundState::Legacy,
            legacy_substreams: MAX_SUBSTREAM_ATTEMPTS,
            ..Handler::default()
        };

        for _ in 0..MAX_SUBSTREAM_ATTEMPTS - 1 {
            fail_outbound(&mut handler);
        }
        handler.on_connection_event(ConnectionEvent::FullyNegotiatedOutbound(
            FullyNegotiatedOutbound {
                protocol: Outbound::LegacySent,
                info: (),
            },
        ));

        assert_eq!(handler.outbound_substream_attempts, 0);
        assert!(matches!(handler.outbound, OutboundState::Legacy));
    }

    #[test]
    fn test_stream_dial_error_keeps_messages_and_does_not_notify() {
        let mut handler = Handler::default();
        handler.on_behaviour_event(Message::Subscribe(Topic::new(b"topic")));

        let error = StreamUpgradeError::<io::Error>::Timeout;
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });
        handler.on_connection_event(event);

        assert_eq!(handler.pending_messages.len(), 1);
        assert!(handler.pending_events.is_empty());
    }

    #[test]
    fn test_giving_up_notifies_outbound_closed() {
        let mut handler = Handler {
            outbound_substream_attempts: MAX_SUBSTREAM_ATTEMPTS - 1,
            ..Handler::default()
        };

        let error = StreamUpgradeError::<io::Error>::NegotiationFailed;
        let event = ConnectionEvent::DialUpgradeError(DialUpgradeError { info: (), error });
        handler.on_connection_event(event);

        assert!(matches!(handler.outbound, OutboundState::Closed));
        assert!(matches!(
            handler.pending_events.pop_front(),
            Some(HandlerEvent::OutboundClosed)
        ));
        assert!(handler.pending_events.is_empty());
    }

    #[test]
    fn test_legacy_mode_drops_message_with_long_topic() {
        let mut handler = Handler {
            outbound: OutboundState::Legacy,
            ..Handler::default()
        };
        let message = Message::Subscribe(Topic::new(b"topic"));

        handler.on_behaviour_event(Message::Subscribe(Topic::new(&[b'x'; Topic::MAX_LENGTH])));
        handler.on_behaviour_event(message.clone());

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        match handler.poll(&mut cx) {
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { protocol }) => {
                match protocol.upgrade() {
                    OutboundProtocol::Legacy(encoded) => {
                        assert_eq!(*encoded, legacy::encode(&message).unwrap());
                    }
                    _ => panic!("Expected legacy upgrade"),
                }
            }
            _ => panic!("Expected OutboundSubstreamRequest"),
        }
        assert!(handler.pending_messages.is_empty());
        assert_eq!(handler.legacy_substreams, 1);
    }

    #[test]
    fn test_legacy_inbound_message_is_emitted() {
        let mut handler = Handler::default();
        let message = Message::Broadcast(Topic::new(b"topic"), Bytes::from_static(b"data"));

        handler.on_connection_event(ConnectionEvent::FullyNegotiatedInbound(
            FullyNegotiatedInbound {
                protocol: Inbound::Legacy(message.clone()),
                info: (),
            },
        ));

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(
            handler.poll(&mut cx),
            Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                HandlerEvent::LegacyFallback
            ))
        ));

        match handler.poll(&mut cx) {
            Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(HandlerEvent::Received(msg))) => {
                assert_eq!(msg, message);
            }
            _ => panic!("Expected NotifyBehaviour with Received event"),
        }
    }

    #[test]
    fn test_legacy_fallback_is_notified_once() {
        let mut handler = Handler::default();
        let topic = Topic::new(b"topic");

        // The connection falls back to the legacy protocol in both directions
        handler.on_connection_event(ConnectionEvent::FullyNegotiatedOutbound(
            FullyNegotiatedOutbound {
                protocol: Outbound::LegacyOnly,
                info: (),
            },
        ));
        for _ in 0..2 {
            handler.on_connection_event(ConnectionEvent::FullyNegotiatedInbound(
                FullyNegotiatedInbound {
                    protocol: Inbound::Legacy(Message::Subscribe(topic)),
                    info: (),
                },
            ));
        }

        let fallbacks = handler
            .pending_events
            .iter()
            .filter(|event| matches!(event, HandlerEvent::LegacyFallback))
            .count();
        assert_eq!(fallbacks, 1);
    }

    #[test]
    fn test_keep_alive_in_legacy_mode() {
        let handler = Handler {
            outbound: OutboundState::Legacy,
            ..Handler::default()
        };
        assert!(handler.connection_keep_alive());
    }
}
