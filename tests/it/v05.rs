//! Tests for exchanging messages with peers running libp2p-scatter 0.5, the upgrade path from the
//! previous release line.
//!
//! libp2p-scatter 0.5 uses libp2p 0.56, so the 0.5 nodes have their own libp2p types. The
//! `legacy_protocol` option is off by default on both nodes, so these tests only pass if the nodes
//! can communicate on the current protocol across the two libp2p versions.

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{Multiaddr, PeerId};
use libp2p_scatter::{Behaviour, Event, Topic};
use libp2p_scatter_v05 as v05;
use libp2p_v056 as libp2p_v05;
use tokio::time::timeout;

use crate::common::{create_listening_swarm, create_swarm};

const TIMEOUT: Duration = Duration::from_secs(30);
const TOPIC: &[u8] = b"topic";

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

fn build_v05_swarm() -> V05Swarm {
    libp2p_v05::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            libp2p_v05::tcp::Config::default(),
            libp2p_v05::noise::Config::new,
            libp2p_v05::yamux::Config::default,
        )
        .expect("Failed to create TCP transport")
        .with_behaviour(|_| v05::Behaviour::new(v05::Config::default()))
        .expect("Failed to create behaviour")
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build()
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

/// The node that opens the connection.
#[derive(Clone, Copy, Debug)]
enum Dialer {
    Node,
    V05,
}

const DIALERS: [Dialer; 2] = [Dialer::Node, Dialer::V05];

/// A node running the current version and a node running libp2p-scatter 0.5.
struct Pair {
    node: Swarm<Behaviour>,
    v05: V05Swarm,
}

impl Pair {
    async fn new(dialer: Dialer) -> Self {
        let mut v05 = build_v05_swarm();

        let node = match dialer {
            Dialer::Node => {
                let addr = listen_v05(&mut v05).await;
                let mut node = create_swarm();
                node.dial(addr).expect("Failed to dial");
                node
            }
            Dialer::V05 => {
                let (node, addr) = create_listening_swarm().await;
                v05.dial(v05_addr(addr)).expect("Failed to dial");
                node
            }
        };

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
}

#[tokio::test]
#[test_log::test]
async fn test_subscriptions_with_v05_peer() {
    for dialer in DIALERS {
        let mut pair = Pair::new(dialer).await;

        pair.subscribe_both().await;
    }
}

#[tokio::test]
#[test_log::test]
async fn test_broadcast_with_v05_peer() {
    for dialer in DIALERS {
        let mut pair = Pair::new(dialer).await;
        let (node_id, v05_id) = (pair.node_id(), pair.v05_id());

        pair.subscribe_both().await;

        for &(size, count) in LOADS {
            let expected: Vec<Bytes> = (0..count).map(|seq| payload(seq, size)).collect();

            for message in &expected {
                pair.node
                    .behaviour_mut()
                    .broadcast(Topic::new(TOPIC), message.clone())
                    .unwrap();
            }
            let (_, events) = pair
                .wait_until(|_, v05| v05_messages(v05, node_id).len() >= count)
                .await;
            assert!(
                v05_messages(&events, node_id) == expected,
                "{dialer:?} dials: wrong messages of {size} bytes from node to 0.5 node"
            );

            for message in &expected {
                pair.v05
                    .behaviour_mut()
                    .broadcast(v05::Topic::new(TOPIC), message.clone())
                    .unwrap();
            }
            let (events, _) = pair
                .wait_until(|node, _| messages(node, v05_id).len() >= count)
                .await;
            assert!(
                messages(&events, v05_id) == expected,
                "{dialer:?} dials: wrong messages of {size} bytes from 0.5 node to node"
            );
        }
    }
}

#[tokio::test]
#[test_log::test]
async fn test_unsubscribe_with_v05_peer() {
    for dialer in DIALERS {
        let mut pair = Pair::new(dialer).await;
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
