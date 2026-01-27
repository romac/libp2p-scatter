use std::collections::VecDeque;
use std::fmt;
use std::task::{Context, Poll};

use bytes::Bytes;
use fnv::{FnvHashMap, FnvHashSet};
use libp2p::swarm::derive_prelude::FromSwarm;
use libp2p::swarm::{
    CloseConnection, ConnectionHandler, ConnectionId, NetworkBehaviour, NotifyHandler, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};

use crate::Config;
use crate::handler::Handler;
use crate::protocol::{Message, Topic};

#[cfg(feature = "metrics")]
use crate::metrics::{Metrics, Registry};

/// Events produced by the behaviour.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    /// A peer has subscribed to a topic.
    Subscribed(PeerId, Topic),
    /// A peer has unsubscribed from a topic.
    Unsubscribed(PeerId, Topic),
    /// A message has been received from a peer on a topic.
    Received(PeerId, Topic, Bytes),
}

/// Network behaviour that handles the scatter protocol
#[derive(Default)]
pub struct Behaviour {
    config: Config,
    /// Map from peer to the set of topics they are subscribed to.
    connected_peers: FnvHashMap<PeerId, FnvHashSet<Topic>>,
    /// Reverse index: map from topic to the set of peers subscribed to it.
    topic_subscribers: FnvHashMap<Topic, FnvHashSet<PeerId>>,
    subscribed_topics: FnvHashSet<Topic>,
    events: VecDeque<ToSwarm<Event, Message>>,

    #[cfg(feature = "metrics")]
    metrics: Option<Metrics>,
}

impl fmt::Debug for Behaviour {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Behaviour")
            .field("config", &self.config)
            .field("subscribed_topics", &self.subscribed_topics)
            .field("connected_peers", &self.connected_peers)
            .finish()
    }
}

impl Behaviour {
    /// Creates a new behaviour with the given configuration.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            ..Default::default()
        }
    }

    /// Creates a new behaviour with the given configuration,
    /// and registers metrics using the provided registry.
    #[cfg(feature = "metrics")]
    pub fn new_with_metrics(config: Config, registry: &mut Registry) -> Self {
        Self {
            config,
            metrics: Some(Metrics::new(registry)),
            ..Default::default()
        }
    }

    /// Returns an iterator over the topics this node is subscribed to.
    pub fn subscribed(&self) -> impl Iterator<Item = Topic> + '_ {
        self.subscribed_topics.iter().copied()
    }

    /// Returns an iterator over the peers subscribed to the given topic.
    pub fn peers(&self, topic: Topic) -> impl Iterator<Item = PeerId> + '_ {
        self.topic_subscribers
            .get(&topic)
            .into_iter()
            .flat_map(|peers| peers.iter().copied())
    }

    /// Returns an iterator over the topics the given peer is subscribed to.
    pub fn topics(&self, peer: &PeerId) -> Option<impl Iterator<Item = &Topic> + '_> {
        self.connected_peers.get(peer).map(|topics| topics.iter())
    }

    /// Subscribe to a topic.
    pub fn subscribe(&mut self, topic: Topic) -> bool {
        // Already subscribed.
        if self.subscribed_topics.contains(&topic) {
            return false;
        }

        for peer in self.connected_peers.keys() {
            self.events.push_back(ToSwarm::NotifyHandler {
                peer_id: *peer,
                handler: NotifyHandler::Any,
                event: Message::Subscribe(topic),
            });
        }

        self.subscribed_topics.insert(topic);

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &mut self.metrics {
            metrics.subscribe(&topic);
        }

        true
    }

    /// Unsubscribe from a topic.
    pub fn unsubscribe(&mut self, topic: Topic) -> bool {
        if !self.subscribed_topics.remove(&topic) {
            return false;
        }

        for peer in self.connected_peers.keys() {
            self.events.push_back(ToSwarm::NotifyHandler {
                peer_id: *peer,
                handler: NotifyHandler::Any,
                event: Message::Unsubscribe(topic),
            });
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &mut self.metrics {
            metrics.unsubscribe(&topic);
        }

        true
    }

    /// Broadcast a message to all target peers subscribed to the given topic.
    pub fn broadcast(&mut self, topic: Topic, payload: impl Into<Bytes>) {
        let payload = payload.into();

        for (peer_id, sub_topic) in &self.connected_peers {
            // Peer must be subscribed to the topic.
            if !sub_topic.contains(&topic) {
                continue;
            }

            #[cfg(feature = "metrics")]
            if let Some(metrics) = &mut self.metrics {
                metrics.msg_sent(&topic, payload.len());
            }

            self.events.push_back(ToSwarm::NotifyHandler {
                peer_id: *peer_id,
                handler: NotifyHandler::Any,
                event: Message::Broadcast(topic, payload.clone()),
            });
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &mut self.metrics {
            metrics.register_published_message(&topic);
        }
    }

    fn inject_connected(&mut self, peer: &PeerId) {
        self.connected_peers.insert(*peer, FnvHashSet::default());
        for topic in &self.subscribed_topics {
            self.events.push_back(ToSwarm::NotifyHandler {
                peer_id: *peer,
                event: Message::Subscribe(*topic),
                handler: NotifyHandler::Any,
            });
        }
    }

    fn inject_disconnected(&mut self, peer: &PeerId) {
        if let Some(topics) = self.connected_peers.remove(peer) {
            for topic in topics {
                // Remove from reverse index
                if let Some(peers) = self.topic_subscribers.get_mut(&topic) {
                    peers.remove(peer);
                }

                self.events
                    .push_back(ToSwarm::GenerateEvent(Event::Unsubscribed(*peer, topic)));

                #[cfg(feature = "metrics")]
                if let Some(metrics) = &mut self.metrics {
                    metrics.dec_topic_peers(&topic);
                }
            }
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Handler;
    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(Handler::new(self.config.clone()))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _addr: &Multiaddr,
        _role_override: libp2p::core::Endpoint,
        _port_use: libp2p::core::transport::PortUse,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(Handler::new(self.config.clone()))
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        match event {
            FromSwarm::ConnectionEstablished(c) => {
                // We only care about the first time a peer connects.
                if c.other_established == 0 {
                    self.inject_connected(&c.peer_id);
                }
            }
            FromSwarm::ConnectionClosed(c) => {
                // We only care about when the last connection to a peer is closed.
                if c.remaining_established == 0 {
                    self.inject_disconnected(&c.peer_id);
                }
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer: PeerId,
        connection_id: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::ToBehaviour,
    ) {
        use crate::handler::HandlerEvent;
        use Message::*;

        match event {
            HandlerEvent::Received(Subscribe(topic)) => {
                #[cfg(feature = "metrics")]
                if let Some(metrics) = self.metrics.as_mut() {
                    metrics.inc_topic_peers(&topic);
                }

                self.connected_peers.entry(peer).or_default().insert(topic);
                self.topic_subscribers
                    .entry(topic)
                    .or_default()
                    .insert(peer);

                self.events
                    .push_back(ToSwarm::GenerateEvent(Event::Subscribed(peer, topic)));
            }

            HandlerEvent::Received(Broadcast(topic, msg)) => {
                #[cfg(feature = "metrics")]
                if let Some(metrics) = self.metrics.as_mut() {
                    metrics.msg_received(&topic, msg.len());
                }

                self.events
                    .push_back(ToSwarm::GenerateEvent(Event::Received(peer, topic, msg)));
            }

            HandlerEvent::Received(Unsubscribe(topic)) => {
                self.connected_peers.entry(peer).or_default().remove(&topic);
                if let Some(peers) = self.topic_subscribers.get_mut(&topic) {
                    peers.remove(&peer);
                }

                #[cfg(feature = "metrics")]
                if let Some(metrics) = self.metrics.as_mut() {
                    metrics.dec_topic_peers(&topic);
                }

                self.events
                    .push_back(ToSwarm::GenerateEvent(Event::Unsubscribed(peer, topic)));
            }

            HandlerEvent::Error(e) => {
                tracing::debug!("Handler error: {e}");

                self.events.push_back(ToSwarm::CloseConnection {
                    peer_id: peer,
                    connection: CloseConnection::One(connection_id),
                });
            }
        }
    }

    fn poll(&mut self, _: &mut Context) -> Poll<ToSwarm<Event, Message>> {
        if let Some(event) = self.events.pop_front() {
            Poll::Ready(event)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    struct DummySwarm {
        peer_id: PeerId,
        behaviour: Arc<Mutex<Behaviour>>,
        connections: FnvHashMap<PeerId, Arc<Mutex<Behaviour>>>,
    }

    impl DummySwarm {
        fn new() -> Self {
            Self {
                peer_id: PeerId::random(),
                behaviour: Default::default(),
                connections: Default::default(),
            }
        }

        fn peer_id(&self) -> &PeerId {
            &self.peer_id
        }

        fn dial(&mut self, other: &mut DummySwarm) {
            self.behaviour
                .lock()
                .unwrap()
                .inject_connected(other.peer_id());
            self.connections
                .insert(*other.peer_id(), other.behaviour.clone());
            other
                .behaviour
                .lock()
                .unwrap()
                .inject_connected(self.peer_id());
            other
                .connections
                .insert(*self.peer_id(), self.behaviour.clone());
        }

        fn next(&self) -> Option<Event> {
            use crate::handler::HandlerEvent;

            let waker = futures::task::noop_waker();
            let mut ctx = Context::from_waker(&waker);
            let mut me = self.behaviour.lock().unwrap();
            loop {
                match me.poll(&mut ctx) {
                    Poll::Ready(ToSwarm::NotifyHandler { peer_id, event, .. }) => {
                        if let Some(other) = self.connections.get(&peer_id) {
                            let mut other = other.lock().unwrap();
                            other.on_connection_handler_event(
                                *self.peer_id(),
                                ConnectionId::new_unchecked(0),
                                HandlerEvent::Received(event),
                            );
                        }
                    }
                    Poll::Ready(ToSwarm::GenerateEvent(event)) => {
                        return Some(event);
                    }
                    Poll::Ready(_) => panic!(),
                    Poll::Pending => {
                        return None;
                    }
                }
            }
        }

        fn subscribe(&self, topic: Topic) {
            let mut me = self.behaviour.lock().unwrap();
            me.subscribe(topic);
        }

        fn unsubscribe(&self, topic: &Topic) {
            let mut me = self.behaviour.lock().unwrap();
            me.unsubscribe(*topic);
        }

        fn broadcast(&self, topic: &Topic, msg: Bytes) {
            let mut me = self.behaviour.lock().unwrap();
            me.broadcast(*topic, msg);
        }
    }

    // ==================== Basic Tests ====================

    #[test]
    fn test_broadcast() {
        let topic = Topic::new(b"topic");
        let msg = Bytes::from_static(b"msg");
        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        a.subscribe(topic);
        a.dial(&mut b);
        assert!(a.next().is_none());
        assert_eq!(b.next().unwrap(), Event::Subscribed(*a.peer_id(), topic));
        b.subscribe(topic);
        assert!(b.next().is_none());
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic));
        b.broadcast(&topic, msg.clone());
        assert!(b.next().is_none());
        assert_eq!(a.next().unwrap(), Event::Received(*b.peer_id(), topic, msg));
        a.unsubscribe(&topic);
        assert!(a.next().is_none());
        assert_eq!(b.next().unwrap(), Event::Unsubscribed(*a.peer_id(), topic));
    }

    // ==================== Multi-Peer Tests ====================

    #[test]
    fn test_three_peers_broadcast() {
        let topic = Topic::new(b"topic");
        let msg = Bytes::from_static(b"hello all");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();
        let mut c = DummySwarm::new();

        // A subscribes first
        a.subscribe(topic);

        // A connects to B and C
        a.dial(&mut b);
        a.dial(&mut c);

        // Process A's outbound messages (subscriptions sent on connect)
        assert!(a.next().is_none());

        // B and C should receive A's subscription
        assert_eq!(b.next().unwrap(), Event::Subscribed(*a.peer_id(), topic));
        assert_eq!(c.next().unwrap(), Event::Subscribed(*a.peer_id(), topic));

        // B and C also subscribe
        b.subscribe(topic);
        c.subscribe(topic);

        // Process B and C's outbound messages
        assert!(b.next().is_none());
        assert!(c.next().is_none());

        // A receives subscriptions from B and C
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic));
        assert_eq!(a.next().unwrap(), Event::Subscribed(*c.peer_id(), topic));

        // A broadcasts - B and C should both receive it
        a.broadcast(&topic, msg.clone());
        assert!(a.next().is_none());
        assert_eq!(
            b.next().unwrap(),
            Event::Received(*a.peer_id(), topic, msg.clone())
        );
        assert_eq!(c.next().unwrap(), Event::Received(*a.peer_id(), topic, msg));
    }

    #[test]
    fn test_peer_joins_after_subscription() {
        let topic = Topic::new(b"topic");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        // A subscribes before connection
        a.subscribe(topic);

        // Now A connects to B
        a.dial(&mut b);

        // Process A's outbound messages
        assert!(a.next().is_none());

        // B should receive A's subscription (sent on connect)
        assert_eq!(b.next().unwrap(), Event::Subscribed(*a.peer_id(), topic));
    }

    #[test]
    fn test_broadcast_reaches_only_subscribers() {
        let topic1 = Topic::new(b"topic1");
        let topic2 = Topic::new(b"topic2");
        let msg = Bytes::from_static(b"for topic1 only");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();
        let mut c = DummySwarm::new();

        a.dial(&mut b);
        a.dial(&mut c);

        // B subscribes to topic1, C subscribes to topic2
        b.subscribe(topic1);
        c.subscribe(topic2);

        // Process B and C's outbound messages
        assert!(b.next().is_none());
        assert!(c.next().is_none());

        // A gets both subscriptions
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic1));
        assert_eq!(a.next().unwrap(), Event::Subscribed(*c.peer_id(), topic2));

        // A broadcasts to topic1
        a.broadcast(&topic1, msg.clone());
        assert!(a.next().is_none());

        // Only B should receive it
        assert_eq!(
            b.next().unwrap(),
            Event::Received(*a.peer_id(), topic1, msg)
        );
        assert!(c.next().is_none());
    }

    // ==================== Topic Management Tests ====================

    #[test]
    fn test_subscribe_multiple_topics() {
        let topic1 = Topic::new(b"topic1");
        let topic2 = Topic::new(b"topic2");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        a.dial(&mut b);

        a.subscribe(topic1);
        a.subscribe(topic2);

        // Process A's outbound messages
        assert!(a.next().is_none());

        // B should receive both subscriptions
        assert_eq!(b.next().unwrap(), Event::Subscribed(*a.peer_id(), topic1));
        assert_eq!(b.next().unwrap(), Event::Subscribed(*a.peer_id(), topic2));
    }

    #[test]
    fn test_unsubscribe_one_topic_keep_other() {
        let topic1 = Topic::new(b"topic1");
        let topic2 = Topic::new(b"topic2");
        let msg = Bytes::from_static(b"msg");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        a.dial(&mut b);

        // A subscribes to both topics
        a.subscribe(topic1);
        a.subscribe(topic2);
        assert!(a.next().is_none());
        assert_eq!(b.next().unwrap(), Event::Subscribed(*a.peer_id(), topic1));
        assert_eq!(b.next().unwrap(), Event::Subscribed(*a.peer_id(), topic2));

        // B also subscribes to both
        b.subscribe(topic1);
        b.subscribe(topic2);
        assert!(b.next().is_none());
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic1));
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic2));

        // A unsubscribes from topic1 only
        a.unsubscribe(&topic1);
        assert!(a.next().is_none());
        assert_eq!(b.next().unwrap(), Event::Unsubscribed(*a.peer_id(), topic1));

        // B broadcasts to topic2 - A should still receive it
        b.broadcast(&topic2, msg.clone());
        assert!(b.next().is_none());
        assert_eq!(
            a.next().unwrap(),
            Event::Received(*b.peer_id(), topic2, msg.clone())
        );

        // B broadcasts to topic1 - A should NOT receive it
        b.broadcast(&topic1, msg);
        assert!(b.next().is_none());
        assert!(a.next().is_none());
    }

    #[test]
    fn test_subscribe_same_topic_twice() {
        let topic = Topic::new(b"topic");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        a.dial(&mut b);

        // A subscribes twice
        a.subscribe(topic);
        a.subscribe(topic);

        // Process A's outbound messages
        assert!(a.next().is_none());

        // B should receive only one subscription event
        assert_eq!(b.next().unwrap(), Event::Subscribed(*a.peer_id(), topic));
    }

    #[test]
    fn test_unsubscribe_not_subscribed() {
        let topic = Topic::new(b"topic");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        a.dial(&mut b);

        // B subscribes so A has the topic in its topics map
        b.subscribe(topic);
        assert!(b.next().is_none());
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic));

        // A unsubscribes from topic it never subscribed to - should work (no-op locally)
        a.unsubscribe(&topic);
        assert!(a.next().is_none());

        // B should not receive unsubscribe (because A was not subscribed)
        assert!(b.next().is_none());
    }

    // ==================== Connection Lifecycle Tests ====================

    #[test]
    fn test_disconnect_removes_peer() {
        let topic = Topic::new(b"topic");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();
        let b_peer_id = *b.peer_id();

        a.dial(&mut b);

        b.subscribe(topic);
        assert!(b.next().is_none());
        assert_eq!(a.next().unwrap(), Event::Subscribed(b_peer_id, topic));

        // Verify B is in A's topics map for this topic
        {
            let behaviour = a.behaviour.lock().unwrap();
            let peers: Vec<_> = behaviour.peers(topic).collect();
            assert!(peers.contains(&b_peer_id));
        }

        // Simulate disconnect
        {
            let mut behaviour = a.behaviour.lock().unwrap();
            behaviour.inject_disconnected(&b_peer_id);
        }

        // Verify B is removed from A's topics map
        {
            let behaviour = a.behaviour.lock().unwrap();
            // Either the topic has no peers or B is not in the list
            let peers: Vec<_> = behaviour.peers(topic).collect();
            assert!(!peers.contains(&b_peer_id));
        }
    }

    // ==================== Broadcast Behavior Tests ====================

    #[test]
    fn test_broadcast_no_subscribers() {
        let topic = Topic::new(b"topic");
        let msg = Bytes::from_static(b"msg");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        a.dial(&mut b);

        // A broadcasts to topic with no subscribers
        a.broadcast(&topic, msg);

        // No events should be generated
        assert!(a.next().is_none());
        assert!(b.next().is_none());
    }

    #[test]
    fn test_multiple_broadcasts_in_sequence() {
        let topic = Topic::new(b"topic");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        a.dial(&mut b);

        b.subscribe(topic);
        assert!(b.next().is_none());
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic));

        // A sends multiple broadcasts
        for i in 0..5 {
            let msg = Bytes::from(format!("msg{}", i));
            a.broadcast(&topic, msg.clone());
            assert!(a.next().is_none());
            assert_eq!(b.next().unwrap(), Event::Received(*a.peer_id(), topic, msg));
        }
    }

    #[test]
    fn test_broadcast_empty_payload() {
        let topic = Topic::new(b"topic");
        let msg = Bytes::new();

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        a.dial(&mut b);

        b.subscribe(topic);
        assert!(b.next().is_none());
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic));

        a.broadcast(&topic, msg.clone());
        assert!(a.next().is_none());
        assert_eq!(b.next().unwrap(), Event::Received(*a.peer_id(), topic, msg));
    }

    // ==================== API Tests ====================

    #[test]
    fn test_subscribed_iterator() {
        let topic1 = Topic::new(b"topic1");
        let topic2 = Topic::new(b"topic2");

        let a = DummySwarm::new();
        a.subscribe(topic1);
        a.subscribe(topic2);

        let behaviour = a.behaviour.lock().unwrap();
        let subscribed: Vec<_> = behaviour.subscribed().collect();
        assert!(subscribed.contains(&topic1));
        assert!(subscribed.contains(&topic2));
        assert_eq!(subscribed.len(), 2);
    }

    #[test]
    fn test_peers_iterator() {
        let topic = Topic::new(b"topic");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();
        let mut c = DummySwarm::new();

        a.dial(&mut b);
        a.dial(&mut c);

        b.subscribe(topic);
        c.subscribe(topic);

        // Process outbound messages
        assert!(b.next().is_none());
        assert!(c.next().is_none());

        // Process subscription events
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic));
        assert_eq!(a.next().unwrap(), Event::Subscribed(*c.peer_id(), topic));

        let behaviour = a.behaviour.lock().unwrap();
        let peers: Vec<_> = behaviour.peers(topic).collect();
        assert!(peers.contains(b.peer_id()));
        assert!(peers.contains(c.peer_id()));
        assert_eq!(peers.len(), 2);
    }

    #[test]
    fn test_topics_iterator() {
        let topic1 = Topic::new(b"topic1");
        let topic2 = Topic::new(b"topic2");

        let mut a = DummySwarm::new();
        let mut b = DummySwarm::new();

        a.dial(&mut b);

        b.subscribe(topic1);
        b.subscribe(topic2);

        // Process outbound messages
        assert!(b.next().is_none());

        // Process subscription events
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic1));
        assert_eq!(a.next().unwrap(), Event::Subscribed(*b.peer_id(), topic2));

        let behaviour = a.behaviour.lock().unwrap();
        let topics: Vec<_> = behaviour.topics(b.peer_id()).unwrap().copied().collect();
        assert!(topics.contains(&topic1));
        assert!(topics.contains(&topic2));
        assert_eq!(topics.len(), 2);
    }
}
