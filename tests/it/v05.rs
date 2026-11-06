//! Tests for exchanging messages with peers running libp2p-scatter 0.5, the upgrade path from the
//! previous release line.
//!
//! libp2p-scatter 0.5 uses libp2p 0.56, so the 0.5 nodes have their own libp2p types.
//!
//! Nodes that share the current protocol must use it, whether `legacy_protocol` is on or not.
//! Nodes that only share the legacy protocol must fall back to it.

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{Multiaddr, PeerId, StreamProtocol, SwarmBuilder, noise, tcp, yamux};
#[cfg(feature = "metrics")]
use libp2p_scatter::metrics::Registry;
use libp2p_scatter::{Behaviour, Config, Event, Topic};
use libp2p_scatter_v05 as v05;
use libp2p_v056 as libp2p_v05;
use tokio::time::timeout;

#[cfg(feature = "metrics")]
use super::legacy::legacy_connections;

const TIMEOUT: Duration = Duration::from_secs(30);
const TOPIC: &[u8] = b"topic";

/// A protocol name that libp2p-scatter 0.5 does not use by default.
const OTHER_PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/me.romac/scatter-test/1.0.0");

/// The payload size and number of messages to send at once. The number of messages stays below
/// the default `max_outbound_queue_size`, above which a node drops messages.
const LOADS: &[(usize, usize)] = &[(1, 500), (64, 500), (64 * 1024, 50), (1024 * 1024, 5)];

type V05Swarm = libp2p_v05::Swarm<v05::Behaviour>;
type V05SwarmEvent = libp2p_v05::swarm::SwarmEvent<v05::Event>;

fn from_v05_peer_id(peer: libp2p_v05::PeerId) -> PeerId {
    PeerId::from_bytes(&peer.to_bytes()).expect("Invalid peer ID")
}

fn v05_addr(addr: Multiaddr) -> libp2p_v05::Multiaddr {
    addr.to_string().parse().expect("Invalid address")
}

fn from_v05_addr(addr: libp2p_v05::Multiaddr) -> Multiaddr {
    addr.to_string().parse().expect("Invalid address")
}

fn build_swarm(behaviour: Behaviour) -> Swarm<Behaviour> {
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

fn build_v05_swarm(config: v05::Config) -> V05Swarm {
    libp2p_v05::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            libp2p_v05::tcp::Config::default(),
            libp2p_v05::noise::Config::new,
            libp2p_v05::yamux::Config::default,
        )
        .expect("Failed to create TCP transport")
        .with_behaviour(|_| v05::Behaviour::new(config))
        .expect("Failed to create behaviour")
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build()
}

/// Starts listening on a random local port, and returns the listen address.
async fn listen(swarm: &mut Swarm<Behaviour>) -> Multiaddr {
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .expect("Failed to start listening");

    loop {
        if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
            return address;
        }
    }
}

/// Starts listening on a random local port, and returns the listen address.
async fn listen_v05(swarm: &mut V05Swarm) -> Multiaddr {
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .expect("Failed to start listening");

    loop {
        if let V05SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
            return from_v05_addr(address);
        }
    }
}

/// A payload that differs for each sequence number, to detect lost or reordered messages.
fn payload(seq: usize, size: usize) -> Bytes {
    let mut payload = vec![seq as u8; size];
    let len = size.min(size_of::<u64>());
    payload[..len].copy_from_slice(&(seq as u64).to_le_bytes()[..len]);
    Bytes::from(payload)
}

fn payloads(size: usize, count: usize) -> Vec<Bytes> {
    (0..count).map(|seq| payload(seq, size)).collect()
}

/// The messages that the node received from the sender on `TOPIC`.
fn messages(events: &[Event], sender: PeerId) -> Vec<Bytes> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Received(p, t, m) if *p == sender && t.as_ref() == TOPIC => Some(m.clone()),
            _ => None,
        })
        .collect()
}

/// The messages that the 0.5 node received from the sender on `TOPIC`.
fn v05_messages(events: &[v05::Event], sender: PeerId) -> Vec<Bytes> {
    events
        .iter()
        .filter_map(|e| match e {
            v05::Event::Received(p, t, m)
                if from_v05_peer_id(*p) == sender && t.as_ref() == TOPIC =>
            {
                Some(m.clone())
            }
            _ => None,
        })
        .collect()
}

fn sorted(mut messages: Vec<Bytes>) -> Vec<Bytes> {
    messages.sort();
    messages
}

/// The node that opens the connection.
#[derive(Clone, Copy, Debug)]
enum Dialer {
    Node,
    V05,
}

const DIALERS: [Dialer; 2] = [Dialer::Node, Dialer::V05];

/// Two nodes that share the current protocol.
#[derive(Clone, Copy, Debug)]
struct Scenario {
    dialer: Dialer,
    /// `Config::legacy_protocol` on the node.
    node_legacy: bool,
    /// `Config::legacy_protocol` on the 0.5 node.
    v05_legacy: bool,
}

impl Scenario {
    /// Each combination of the dialer and of `legacy_protocol` on the two nodes.
    fn all() -> impl Iterator<Item = Self> {
        DIALERS.into_iter().flat_map(|dialer| {
            [(false, false), (true, false), (false, true), (true, true)].map(
                |(node_legacy, v05_legacy)| Self {
                    dialer,
                    node_legacy,
                    v05_legacy,
                },
            )
        })
    }

    fn config(self) -> Config {
        Config::default().legacy_protocol(self.node_legacy)
    }

    fn v05_config(self) -> v05::Config {
        v05::Config::default().legacy_protocol(self.v05_legacy)
    }

    async fn pair(self) -> Pair {
        tracing::info!(scenario = ?self, "Starting scenario");
        Pair::new(
            self.dialer,
            Behaviour::new(self.config()),
            self.v05_config(),
        )
        .await
    }
}

/// The configuration of a node that only shares the legacy protocol with a 0.5 node that has the
/// configuration of [`fallback_v05_config`].
fn fallback_config() -> Config {
    Config::default()
        .protocol_name(OTHER_PROTOCOL_NAME)
        .legacy_protocol(true)
}

fn fallback_v05_config() -> v05::Config {
    v05::Config::default().legacy_protocol(true)
}

/// A node running the current version and a node running libp2p-scatter 0.5.
struct Pair {
    node: Swarm<Behaviour>,
    v05: V05Swarm,
}

impl Pair {
    async fn new(dialer: Dialer, behaviour: Behaviour, v05_config: v05::Config) -> Self {
        let mut node = build_swarm(behaviour);
        let mut v05 = build_v05_swarm(v05_config);

        match dialer {
            Dialer::Node => {
                let addr = listen_v05(&mut v05).await;
                node.dial(addr).expect("Failed to dial");
            }
            Dialer::V05 => {
                let addr = listen(&mut node).await;
                v05.dial(v05_addr(addr)).expect("Failed to dial");
            }
        }

        Self { node, v05 }
    }

    fn node_id(&self) -> PeerId {
        *self.node.local_peer_id()
    }

    fn v05_id(&self) -> PeerId {
        from_v05_peer_id(*self.v05.local_peer_id())
    }

    /// Drives both swarms, collecting scatter events until the predicate returns true.
    ///
    /// Panics if the connection closes.
    async fn wait_until<F>(&mut self, mut predicate: F) -> (Vec<Event>, Vec<v05::Event>)
    where
        F: FnMut(&[Event], &[v05::Event]) -> bool,
    {
        let mut node_events = Vec::new();
        let mut v05_events = Vec::new();

        timeout(TIMEOUT, async {
            while !predicate(&node_events, &v05_events) {
                tokio::select! {
                    event = self.node.select_next_some() => match event {
                        SwarmEvent::Behaviour(event) => node_events.push(event),
                        SwarmEvent::ConnectionClosed { cause, .. } => {
                            panic!("Connection closed: {cause:?}");
                        }
                        _ => {}
                    },
                    event = self.v05.select_next_some() => match event {
                        V05SwarmEvent::Behaviour(event) => v05_events.push(event),
                        V05SwarmEvent::ConnectionClosed { cause, .. } => {
                            panic!("Connection closed: {cause:?}");
                        }
                        _ => {}
                    },
                }
            }
        })
        .await
        .expect("Timeout waiting for events");

        (node_events, v05_events)
    }

    /// Subscribes both nodes to `TOPIC` and waits until each sees the other's subscription.
    async fn subscribe_both(&mut self) {
        let (node_id, v05_id) = (self.node_id(), self.v05_id());

        self.node.behaviour_mut().subscribe(Topic::new(TOPIC));
        self.v05.behaviour_mut().subscribe(v05::Topic::new(TOPIC));

        self.wait_until(|node, v05| {
            node.iter().any(
                |e| matches!(e, Event::Subscribed(p, t) if *p == v05_id && t.as_ref() == TOPIC),
            ) && v05.iter().any(
                |e| matches!(e, v05::Event::Subscribed(p, t) if from_v05_peer_id(*p) == node_id && t.as_ref() == TOPIC),
            )
        })
        .await;
    }

    /// Broadcasts the payloads from the node, and returns the messages that the 0.5 node received.
    async fn send_from_node(&mut self, payloads: &[Bytes]) -> Vec<Bytes> {
        let node_id = self.node_id();

        for payload in payloads {
            self.node
                .behaviour_mut()
                .broadcast(Topic::new(TOPIC), payload.clone())
                .unwrap();
        }

        let (_, events) = self
            .wait_until(|_, v05| v05_messages(v05, node_id).len() >= payloads.len())
            .await;

        v05_messages(&events, node_id)
    }

    /// Broadcasts the payloads from the 0.5 node, and returns the messages that the node received.
    async fn send_from_v05(&mut self, payloads: &[Bytes]) -> Vec<Bytes> {
        let v05_id = self.v05_id();

        for payload in payloads {
            self.v05
                .behaviour_mut()
                .broadcast(v05::Topic::new(TOPIC), payload.clone())
                .unwrap();
        }

        let (events, _) = self
            .wait_until(|node, _| messages(node, v05_id).len() >= payloads.len())
            .await;

        messages(&events, v05_id)
    }
}

#[tokio::test]
#[test_log::test]
async fn test_subscriptions_with_v05_peer() {
    for scenario in Scenario::all() {
        let mut pair = scenario.pair().await;

        pair.subscribe_both().await;
    }
}

#[tokio::test]
#[test_log::test]
async fn test_broadcast_with_v05_peer() {
    for scenario in Scenario::all() {
        let mut pair = scenario.pair().await;

        pair.subscribe_both().await;

        for &(size, count) in LOADS {
            let expected = payloads(size, count);

            // The current protocol keeps the order of the messages.
            assert!(
                pair.send_from_node(&expected).await == expected,
                "{scenario:?}: wrong messages of {size} bytes from node to 0.5 node"
            );
            assert!(
                pair.send_from_v05(&expected).await == expected,
                "{scenario:?}: wrong messages of {size} bytes from 0.5 node to node"
            );
        }
    }
}

#[tokio::test]
#[test_log::test]
async fn test_unsubscribe_with_v05_peer() {
    for scenario in Scenario::all() {
        let mut pair = scenario.pair().await;
        let (node_id, v05_id) = (pair.node_id(), pair.v05_id());

        pair.subscribe_both().await;

        pair.node.behaviour_mut().unsubscribe(Topic::new(TOPIC));
        pair.wait_until(|_, v05| {
            v05.iter().any(
                |e| matches!(e, v05::Event::Unsubscribed(p, t) if from_v05_peer_id(*p) == node_id && t.as_ref() == TOPIC),
            )
        })
        .await;

        pair.v05.behaviour_mut().unsubscribe(v05::Topic::new(TOPIC));
        pair.wait_until(|node, _| {
            node.iter().any(
                |e| matches!(e, Event::Unsubscribed(p, t) if *p == v05_id && t.as_ref() == TOPIC),
            )
        })
        .await;
    }
}

#[tokio::test]
#[test_log::test]
async fn test_legacy_fallback_with_v05_peer() {
    for dialer in DIALERS {
        let behaviour = Behaviour::new(fallback_config());
        let mut pair = Pair::new(dialer, behaviour, fallback_v05_config()).await;

        pair.subscribe_both().await;

        for &(size, count) in LOADS {
            let expected = sorted(payloads(size, count));

            // Messages sent on separate legacy substreams can arrive in any order.
            assert!(
                sorted(pair.send_from_node(&expected).await) == expected,
                "{dialer:?} dials: wrong messages of {size} bytes from node to 0.5 node"
            );
            assert!(
                sorted(pair.send_from_v05(&expected).await) == expected,
                "{dialer:?} dials: wrong messages of {size} bytes from 0.5 node to node"
            );
        }
    }
}

#[cfg(feature = "metrics")]
#[tokio::test]
#[test_log::test]
async fn test_no_legacy_fallback_when_v05_peer_shares_current_protocol() {
    for scenario in Scenario::all() {
        let mut registry = Registry::default();
        let behaviour = Behaviour::new_with_metrics(scenario.config(), &mut registry);
        let mut pair = Pair::new(scenario.dialer, behaviour, scenario.v05_config()).await;

        pair.subscribe_both().await;

        let expected = payloads(64, 10);
        pair.send_from_node(&expected).await;
        pair.send_from_v05(&expected).await;

        assert_eq!(legacy_connections(&registry), 0, "{scenario:?}");
    }
}

#[cfg(feature = "metrics")]
#[tokio::test]
#[test_log::test]
async fn test_metrics_count_legacy_fallback_with_v05_peer() {
    for dialer in DIALERS {
        let mut registry = Registry::default();
        let behaviour = Behaviour::new_with_metrics(fallback_config(), &mut registry);
        let mut pair = Pair::new(dialer, behaviour, fallback_v05_config()).await;

        pair.subscribe_both().await;

        assert_eq!(legacy_connections(&registry), 1, "{dialer:?} dials");
    }
}
