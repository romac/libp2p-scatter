//! Tests for exchanging messages with peers running libp2p-scatter 0.3.

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{PeerId, SwarmBuilder, noise, tcp, yamux};
use libp2p_scatter::{Behaviour, Config, Event, Topic};
use libp2p_scatter_v03 as v03;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

fn build_swarm<B: NetworkBehaviour>(behaviour: B) -> Swarm<B> {
    SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .expect("Failed to create TCP transport")
        .with_behaviour(|_| behaviour)
        .expect("Failed to create behaviour")
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build()
}

/// A node running the current version and a node running libp2p-scatter 0.3.
struct Pair {
    node: Swarm<Behaviour>,
    v03: Swarm<v03::Behaviour>,
}

#[derive(Debug)]
enum PairEvent {
    Node(SwarmEvent<Event>),
    V03(SwarmEvent<v03::Event>),
}

impl Pair {
    fn new(config: Config) -> Self {
        Self {
            node: build_swarm(Behaviour::new(config)),
            v03: build_swarm(v03::Behaviour::new(v03::Config::default())),
        }
    }

    fn node_id(&self) -> PeerId {
        *self.node.local_peer_id()
    }

    fn v03_id(&self) -> PeerId {
        *self.v03.local_peer_id()
    }

    /// Connects the node to the 0.3 node.
    async fn connect(&mut self) {
        self.v03
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .expect("Failed to start listening");

        let addr = loop {
            if let SwarmEvent::NewListenAddr { address, .. } = self.v03.select_next_some().await {
                break address;
            }
        };

        self.node.dial(addr).expect("Failed to dial");

        let mut node_connected = false;
        let mut v03_connected = false;

        timeout(TIMEOUT, async {
            while !(node_connected && v03_connected) {
                match self.next().await {
                    PairEvent::Node(SwarmEvent::ConnectionEstablished { .. }) => {
                        node_connected = true;
                    }
                    PairEvent::V03(SwarmEvent::ConnectionEstablished { .. }) => {
                        v03_connected = true;
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("Timeout waiting for connection");
    }

    async fn next(&mut self) -> PairEvent {
        let event = tokio::select! {
            event = self.node.select_next_some() => PairEvent::Node(event),
            event = self.v03.select_next_some() => PairEvent::V03(event),
        };
        tracing::debug!(?event, "Pair event");
        event
    }

    /// Drives both swarms, collecting scatter events until the predicate returns true.
    ///
    /// Panics if the connection closes.
    async fn wait_until<F>(&mut self, mut predicate: F) -> (Vec<Event>, Vec<v03::Event>)
    where
        F: FnMut(&[Event], &[v03::Event]) -> bool,
    {
        let mut node_events = Vec::new();
        let mut v03_events = Vec::new();

        timeout(TIMEOUT, async {
            while !predicate(&node_events, &v03_events) {
                match self.next().await {
                    PairEvent::Node(SwarmEvent::Behaviour(event)) => node_events.push(event),
                    PairEvent::V03(SwarmEvent::Behaviour(event)) => v03_events.push(event),
                    PairEvent::Node(SwarmEvent::ConnectionClosed { cause, .. })
                    | PairEvent::V03(SwarmEvent::ConnectionClosed { cause, .. }) => {
                        panic!("Connection closed: {cause:?}");
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("Timeout waiting for events");

        (node_events, v03_events)
    }

    /// Subscribes both nodes to the topic and waits until each sees the other's subscription.
    async fn subscribe_both(&mut self, topic: &[u8]) {
        let (node_id, v03_id) = (self.node_id(), self.v03_id());

        self.node.behaviour_mut().subscribe(Topic::new(topic));
        self.v03.behaviour_mut().subscribe(v03::Topic::new(topic));

        self.wait_until(|node, v03| {
            node.iter().any(
                |e| matches!(e, Event::Subscribed(p, t) if *p == v03_id && t.as_ref() == topic),
            ) && v03.iter().any(
                |e| matches!(e, v03::Event::Subscribed(p, t) if *p == node_id && t.as_ref() == topic),
            )
        })
        .await;
    }
}

#[tokio::test]
#[test_log::test]
async fn test_subscriptions_on_connect_with_v03_peer() {
    let mut pair = Pair::new(Config::default().legacy_protocol(true));
    let (node_id, v03_id) = (pair.node_id(), pair.v03_id());

    pair.node.behaviour_mut().subscribe(Topic::new(b"topic"));
    pair.v03
        .behaviour_mut()
        .subscribe(v03::Topic::new(b"topic"));

    pair.connect().await;

    pair.wait_until(|node, v03| {
        node.iter().any(
            |e| matches!(e, Event::Subscribed(p, t) if *p == v03_id && t.as_ref() == b"topic"),
        ) && v03.iter().any(
            |e| matches!(e, v03::Event::Subscribed(p, t) if *p == node_id && t.as_ref() == b"topic"),
        )
    })
    .await;
}

#[tokio::test]
#[test_log::test]
async fn test_broadcast_with_v03_peer() {
    let mut pair = Pair::new(Config::default().legacy_protocol(true));
    let (node_id, v03_id) = (pair.node_id(), pair.v03_id());

    pair.connect().await;
    pair.subscribe_both(b"topic").await;

    pair.node
        .behaviour_mut()
        .broadcast(Topic::new(b"topic"), Bytes::from_static(b"from node"));
    pair.v03
        .behaviour_mut()
        .broadcast(&v03::Topic::new(b"topic"), Bytes::from_static(b"from v03"));

    pair.wait_until(|node, v03| {
        node.iter().any(|e| {
            matches!(e, Event::Received(p, t, m)
                if *p == v03_id && t.as_ref() == b"topic" && m.as_ref() == b"from v03")
        }) && v03.iter().any(|e| {
            matches!(e, v03::Event::Received(p, t, m)
                if *p == node_id && t.as_ref() == b"topic" && m.as_ref() == b"from node")
        })
    })
    .await;
}

#[tokio::test]
#[test_log::test]
async fn test_unsubscribe_with_v03_peer() {
    let mut pair = Pair::new(Config::default().legacy_protocol(true));
    let (node_id, v03_id) = (pair.node_id(), pair.v03_id());

    pair.connect().await;
    pair.subscribe_both(b"topic").await;

    pair.node.behaviour_mut().unsubscribe(Topic::new(b"topic"));
    pair.v03
        .behaviour_mut()
        .unsubscribe(&v03::Topic::new(b"topic"));

    pair.wait_until(|node, v03| {
        node.iter().any(
            |e| matches!(e, Event::Unsubscribed(p, t) if *p == v03_id && t.as_ref() == b"topic"),
        ) && v03.iter().any(
            |e| matches!(e, v03::Event::Unsubscribed(p, t) if *p == node_id && t.as_ref() == b"topic"),
        )
    })
    .await;
}

#[tokio::test]
#[test_log::test]
async fn test_many_broadcasts_with_v03_peer() {
    const COUNT: usize = 100;

    let mut pair = Pair::new(Config::default().legacy_protocol(true));

    pair.connect().await;
    pair.subscribe_both(b"topic").await;

    for i in 0..COUNT {
        let payload = Bytes::from(i.to_string());
        pair.node
            .behaviour_mut()
            .broadcast(Topic::new(b"topic"), payload.clone());
        pair.v03
            .behaviour_mut()
            .broadcast(&v03::Topic::new(b"topic"), payload);
    }

    let (node_events, v03_events) = pair
        .wait_until(|node, v03| {
            let node_received = node
                .iter()
                .filter(|e| matches!(e, Event::Received(..)))
                .count();
            let v03_received = v03
                .iter()
                .filter(|e| matches!(e, v03::Event::Received(..)))
                .count();
            node_received >= COUNT && v03_received >= COUNT
        })
        .await;

    // Messages sent on separate legacy substreams can arrive in any order
    let expected: HashSet<Bytes> = (0..COUNT).map(|i| Bytes::from(i.to_string())).collect();
    let node_received: HashSet<Bytes> = node_events
        .into_iter()
        .filter_map(|e| match e {
            Event::Received(_, _, m) => Some(m),
            _ => None,
        })
        .collect();
    let v03_received: HashSet<Bytes> = v03_events
        .into_iter()
        .filter_map(|e| match e {
            v03::Event::Received(_, _, m) => Some(m),
            _ => None,
        })
        .collect();

    assert_eq!(node_received, expected);
    assert_eq!(v03_received, expected);
}

#[tokio::test]
#[test_log::test]
async fn test_v03_peer_disconnects_by_default() {
    let mut pair = Pair::new(Config::default());

    pair.v03
        .behaviour_mut()
        .subscribe(v03::Topic::new(b"topic"));

    pair.connect().await;

    // The 0.3 node fails to negotiate a substream for its subscription,
    // and closes the connection.
    timeout(TIMEOUT, async {
        loop {
            match pair.next().await {
                PairEvent::Node(SwarmEvent::Behaviour(event)) => {
                    panic!("Unexpected event: {event:?}");
                }
                PairEvent::Node(SwarmEvent::ConnectionClosed { .. }) => break,
                _ => {}
            }
        }
    })
    .await
    .expect("Timeout waiting for connection to close");
}
