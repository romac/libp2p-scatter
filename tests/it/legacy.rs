//! Tests for exchanging messages with peers running libp2p-scatter 0.3.
//!
//! libp2p-scatter 0.3 uses libp2p 0.56, so the 0.3 nodes have their own
//! libp2p types. The `v03_*` helpers convert peer IDs, addresses, and keys
//! between the two libp2p versions.

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use libp2p::identity::Keypair;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{Multiaddr, PeerId, SwarmBuilder, noise, tcp, yamux};
#[cfg(feature = "metrics")]
use libp2p_scatter::metrics::Registry;
use libp2p_scatter::{Behaviour, Config, Event, Topic};
use libp2p_scatter_v03 as v03;
use libp2p_v056 as libp2p_v03;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

type V03Swarm = libp2p_v03::Swarm<v03::Behaviour>;
type V03SwarmEvent = libp2p_v03::swarm::SwarmEvent<v03::Event>;

fn v03_peer_id(peer: PeerId) -> libp2p_v03::PeerId {
    libp2p_v03::PeerId::from_bytes(&peer.to_bytes()).expect("Invalid peer ID")
}

fn from_v03_peer_id(peer: libp2p_v03::PeerId) -> PeerId {
    PeerId::from_bytes(&peer.to_bytes()).expect("Invalid peer ID")
}

fn v03_addr(addr: Multiaddr) -> libp2p_v03::Multiaddr {
    addr.to_string().parse().expect("Invalid address")
}

fn from_v03_addr(addr: libp2p_v03::Multiaddr) -> Multiaddr {
    addr.to_string().parse().expect("Invalid address")
}

fn v03_keypair(keypair: &Keypair) -> libp2p_v03::identity::Keypair {
    let encoded = keypair.to_protobuf_encoding().expect("Invalid keypair");
    libp2p_v03::identity::Keypair::from_protobuf_encoding(&encoded).expect("Invalid keypair")
}

/// Returns the value of the `legacy_connections` metric.
#[cfg(feature = "metrics")]
fn legacy_connections(registry: &Registry) -> u64 {
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, registry)
        .expect("Failed to encode metrics");

    text.lines()
        .find_map(|line| line.strip_prefix("legacy_connections_total "))
        .expect("Metric legacy_connections not found")
        .parse()
        .expect("Invalid value for metric legacy_connections")
}

fn build_swarm(keypair: Keypair, behaviour: Behaviour) -> Swarm<Behaviour> {
    SwarmBuilder::with_existing_identity(keypair)
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

fn build_v03_swarm(keypair: libp2p_v03::identity::Keypair) -> V03Swarm {
    libp2p_v03::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            libp2p_v03::tcp::Config::default(),
            libp2p_v03::noise::Config::new,
            libp2p_v03::yamux::Config::default,
        )
        .expect("Failed to create TCP transport")
        .with_behaviour(|_| v03::Behaviour::new(v03::Config::default()))
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
async fn listen_v03(swarm: &mut V03Swarm) -> Multiaddr {
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .expect("Failed to start listening");

    loop {
        if let V03SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
            return from_v03_addr(address);
        }
    }
}

/// A node running the current version and a node running libp2p-scatter 0.3.
struct Pair {
    node: Swarm<Behaviour>,
    v03: V03Swarm,
}

#[derive(Debug)]
enum PairEvent {
    Node(SwarmEvent<Event>),
    V03(V03SwarmEvent),
}

impl Pair {
    fn new(config: Config) -> Self {
        Self::with_behaviour(Behaviour::new(config))
    }

    fn with_behaviour(behaviour: Behaviour) -> Self {
        Self {
            node: build_swarm(Keypair::generate_ed25519(), behaviour),
            v03: build_v03_swarm(libp2p_v03::identity::Keypair::generate_ed25519()),
        }
    }

    fn node_id(&self) -> PeerId {
        *self.node.local_peer_id()
    }

    fn v03_id(&self) -> PeerId {
        from_v03_peer_id(*self.v03.local_peer_id())
    }

    /// Connects the node to the 0.3 node.
    async fn connect(&mut self) {
        let addr = listen_v03(&mut self.v03).await;
        self.node.dial(addr).expect("Failed to dial");

        let mut node_connected = false;
        let mut v03_connected = false;

        timeout(TIMEOUT, async {
            while !(node_connected && v03_connected) {
                match self.next().await {
                    PairEvent::Node(SwarmEvent::ConnectionEstablished { .. }) => {
                        node_connected = true;
                    }
                    PairEvent::V03(V03SwarmEvent::ConnectionEstablished { .. }) => {
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
                    PairEvent::V03(V03SwarmEvent::Behaviour(event)) => v03_events.push(event),
                    PairEvent::Node(SwarmEvent::ConnectionClosed { cause, .. }) => {
                        panic!("Connection closed: {cause:?}");
                    }
                    PairEvent::V03(V03SwarmEvent::ConnectionClosed { cause, .. }) => {
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
                |e| matches!(e, v03::Event::Subscribed(p, t) if from_v03_peer_id(*p) == node_id && t.as_ref() == topic),
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
            |e| matches!(e, v03::Event::Subscribed(p, t) if from_v03_peer_id(*p) == node_id && t.as_ref() == b"topic"),
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
                if from_v03_peer_id(*p) == node_id && t.as_ref() == b"topic" && m.as_ref() == b"from node")
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
            |e| matches!(e, v03::Event::Unsubscribed(p, t) if from_v03_peer_id(*p) == node_id && t.as_ref() == b"topic"),
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

#[cfg(feature = "metrics")]
#[tokio::test]
#[test_log::test]
async fn test_metrics_count_legacy_connections() {
    let mut registry = Registry::default();
    let config = Config::default().legacy_protocol(true);
    let mut pair = Pair::with_behaviour(Behaviour::new_with_metrics(config, &mut registry));

    // The connection falls back only when a message is sent
    pair.connect().await;
    assert_eq!(legacy_connections(&registry), 0);

    // Messages in both directions use the legacy protocol on the same connection
    pair.subscribe_both(b"topic").await;
    assert_eq!(legacy_connections(&registry), 1);
}

// ==================== Rolling Upgrade ====================

const UPGRADE_TOPIC: &[u8] = b"upgrade";

/// The version that a node runs.
#[derive(Clone, Copy)]
enum Version {
    /// libp2p-scatter 0.3.
    V03,
    /// The current version, with the given `Config::legacy_protocol`.
    Current { legacy_protocol: bool },
}

enum NodeSwarm {
    V03(V03Swarm),
    Current(Swarm<Behaviour>),
}

/// A node that runs libp2p-scatter 0.3 or the current version,
/// and is subscribed to [`UPGRADE_TOPIC`].
struct MixedNode {
    keypair: Keypair,
    swarm: NodeSwarm,
    addr: Multiaddr,
    /// Metrics of the node. Only nodes that run the current version have metrics.
    #[cfg(feature = "metrics")]
    registry: Registry,
}

impl MixedNode {
    async fn start(keypair: Keypair, version: Version) -> Self {
        #[cfg(feature = "metrics")]
        let mut registry = Registry::default();

        let (swarm, addr) = match version {
            Version::V03 => {
                let mut swarm = build_v03_swarm(v03_keypair(&keypair));
                swarm
                    .behaviour_mut()
                    .subscribe(v03::Topic::new(UPGRADE_TOPIC));
                let addr = listen_v03(&mut swarm).await;
                (NodeSwarm::V03(swarm), addr)
            }
            Version::Current { legacy_protocol } => {
                let config = Config::default().legacy_protocol(legacy_protocol);
                #[cfg(feature = "metrics")]
                let behaviour = Behaviour::new_with_metrics(config, &mut registry);
                #[cfg(not(feature = "metrics"))]
                let behaviour = Behaviour::new(config);

                let mut swarm = build_swarm(keypair.clone(), behaviour);
                swarm.behaviour_mut().subscribe(Topic::new(UPGRADE_TOPIC));
                let addr = listen(&mut swarm).await;
                (NodeSwarm::Current(swarm), addr)
            }
        };

        Self {
            keypair,
            swarm,
            addr,
            #[cfg(feature = "metrics")]
            registry,
        }
    }

    /// Returns the value of the `legacy_connections` metric.
    #[cfg(feature = "metrics")]
    fn legacy_connections(&self) -> u64 {
        legacy_connections(&self.registry)
    }

    fn peer_id(&self) -> PeerId {
        self.keypair.public().to_peer_id()
    }

    fn dial(&mut self, addr: Multiaddr) {
        match &mut self.swarm {
            NodeSwarm::V03(swarm) => swarm.dial(v03_addr(addr)).expect("Failed to dial"),
            NodeSwarm::Current(swarm) => swarm.dial(addr).expect("Failed to dial"),
        }
    }

    fn is_connected(&self, peer: &PeerId) -> bool {
        match &self.swarm {
            NodeSwarm::V03(swarm) => swarm.is_connected(&v03_peer_id(*peer)),
            NodeSwarm::Current(swarm) => swarm.is_connected(peer),
        }
    }

    /// Returns the peers that this node knows are subscribed to the topic.
    fn subscribers(&self) -> HashSet<PeerId> {
        match &self.swarm {
            NodeSwarm::V03(swarm) => swarm
                .behaviour()
                .peers(&v03::Topic::new(UPGRADE_TOPIC))
                .map(|peers| peers.copied().map(from_v03_peer_id).collect())
                .unwrap_or_default(),
            NodeSwarm::Current(swarm) => {
                swarm.behaviour().peers(Topic::new(UPGRADE_TOPIC)).collect()
            }
        }
    }

    fn broadcast(&mut self, payload: Bytes) {
        match &mut self.swarm {
            NodeSwarm::V03(swarm) => swarm
                .behaviour_mut()
                .broadcast(&v03::Topic::new(UPGRADE_TOPIC), payload),
            NodeSwarm::Current(swarm) => swarm
                .behaviour_mut()
                .broadcast(Topic::new(UPGRADE_TOPIC), payload),
        }
    }

    /// Processes the pending swarm events, and returns the messages received on the topic.
    fn drain_events(&mut self) -> Vec<(PeerId, Bytes)> {
        let mut received = Vec::new();

        match &mut self.swarm {
            NodeSwarm::V03(swarm) => {
                while let Some(event) = swarm.next().now_or_never().flatten() {
                    if let V03SwarmEvent::Behaviour(v03::Event::Received(peer, topic, payload)) =
                        event
                        && topic.as_ref() == UPGRADE_TOPIC
                    {
                        received.push((from_v03_peer_id(peer), payload));
                    }
                }
            }
            NodeSwarm::Current(swarm) => {
                while let Some(event) = swarm.next().now_or_never().flatten() {
                    if let SwarmEvent::Behaviour(Event::Received(peer, topic, payload)) = event
                        && topic.as_ref() == UPGRADE_TOPIC
                    {
                        received.push((peer, payload));
                    }
                }
            }
        }

        received
    }
}

/// A fully connected network of nodes that run libp2p-scatter 0.3 or the current version.
struct MixedNetwork {
    nodes: Vec<MixedNode>,
}

impl MixedNetwork {
    /// Starts a fully connected network of nodes that run the given version.
    async fn start(count: usize, version: Version) -> Self {
        let mut nodes = Vec::with_capacity(count);
        for _ in 0..count {
            nodes.push(MixedNode::start(Keypair::generate_ed25519(), version).await);
        }

        for i in 0..count {
            for j in i + 1..count {
                let addr = nodes[j].addr.clone();
                nodes[i].dial(addr);
            }
        }

        let mut network = Self { nodes };
        network.wait_until_meshed().await;
        network
    }

    /// Restarts the node at `index` with the given version and the same identity,
    /// then reconnects it to the other nodes.
    async fn restart(&mut self, index: usize, version: Version) {
        let peer_id = self.nodes[index].peer_id();
        let keypair = self.nodes.remove(index).keypair;

        // Wait until the other nodes see that the old node is gone, so that
        // they send their subscriptions again when the new node connects.
        self.drive_until(|nodes, _| nodes.iter().all(|node| !node.is_connected(&peer_id)))
            .await;

        let mut node = MixedNode::start(keypair, version).await;
        for other in &self.nodes {
            node.dial(other.addr.clone());
        }
        self.nodes.insert(index, node);

        self.wait_until_meshed().await;
    }

    /// Checks that no connection fell back to the legacy protocol.
    #[cfg(feature = "metrics")]
    fn assert_no_legacy_connections(&self) {
        for (i, node) in self.nodes.iter().enumerate() {
            assert_eq!(
                node.legacy_connections(),
                0,
                "node {i} has connections that fell back to the legacy protocol"
            );
        }
    }

    /// Waits until each node is connected to all other nodes,
    /// and knows that they are subscribed to the topic.
    async fn wait_until_meshed(&mut self) {
        let peer_ids: HashSet<PeerId> = self.nodes.iter().map(MixedNode::peer_id).collect();

        self.drive_until(|nodes, _| {
            nodes.iter().all(|node| {
                let mut others = peer_ids.clone();
                others.remove(&node.peer_id());
                others.iter().all(|peer| node.is_connected(peer)) && node.subscribers() == others
            })
        })
        .await;
    }

    /// Makes each node broadcast a message, and checks that all other nodes receive it.
    async fn assert_broadcasts_reach_all_nodes(&mut self, round: usize) {
        let payload = |i: usize| Bytes::from(format!("round {round} from node {i}"));
        let peer_ids: Vec<PeerId> = self.nodes.iter().map(MixedNode::peer_id).collect();

        for (i, node) in self.nodes.iter_mut().enumerate() {
            node.broadcast(payload(i));
        }

        let expected: Vec<HashSet<(PeerId, Bytes)>> = (0..peer_ids.len())
            .map(|i| {
                (0..peer_ids.len())
                    .filter(|&j| j != i)
                    .map(|j| (peer_ids[j], payload(j)))
                    .collect()
            })
            .collect();

        let received = self
            .drive_until(|_, received| {
                received
                    .iter()
                    .zip(&expected)
                    .all(|(received, expected)| received.len() >= expected.len())
            })
            .await;

        for (i, (received, expected)) in received.into_iter().zip(&expected).enumerate() {
            assert_eq!(
                received.len(),
                expected.len(),
                "node {i} received unexpected messages in round {round}"
            );
            assert_eq!(
                &received.into_iter().collect::<HashSet<_>>(),
                expected,
                "node {i} received the wrong messages in round {round}"
            );
        }
    }

    /// Drives all nodes until the predicate returns true,
    /// and returns the messages that each node received.
    async fn drive_until<F>(&mut self, mut done: F) -> Vec<Vec<(PeerId, Bytes)>>
    where
        F: FnMut(&[MixedNode], &[Vec<(PeerId, Bytes)>]) -> bool,
    {
        let mut received = vec![Vec::new(); self.nodes.len()];

        timeout(TIMEOUT, async {
            while !done(&self.nodes, &received) {
                for (node, received) in self.nodes.iter_mut().zip(&mut received) {
                    received.extend(node.drain_events());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Timeout waiting for the network");

        received
    }
}

#[tokio::test]
#[test_log::test]
async fn test_rolling_upgrade_from_v03() {
    const NODES: usize = 4;

    let mut network = MixedNetwork::start(NODES, Version::V03).await;
    network.assert_broadcasts_reach_all_nodes(0).await;

    // Until the last upgrade, the network has nodes on both versions.
    for index in 0..NODES {
        let version = Version::Current {
            legacy_protocol: true,
        };
        network.restart(index, version).await;
        network.assert_broadcasts_reach_all_nodes(index + 1).await;

        // The upgraded node falls back to the legacy protocol
        // on its connection to each node that still runs 0.3.
        #[cfg(feature = "metrics")]
        assert_eq!(
            network.nodes[index].legacy_connections(),
            (NODES - 1 - index) as u64
        );
    }
}

#[tokio::test]
#[test_log::test]
async fn test_rolling_upgrade_turns_off_legacy_protocol() {
    const NODES: usize = 4;

    // All nodes run the current version, and still support the legacy protocol.
    let version = Version::Current {
        legacy_protocol: true,
    };
    let mut network = MixedNetwork::start(NODES, version).await;
    network.assert_broadcasts_reach_all_nodes(0).await;

    // Until the last restart, the network has nodes with and without
    // legacy support. No connection falls back to the legacy protocol.
    for index in 0..NODES {
        let version = Version::Current {
            legacy_protocol: false,
        };
        network.restart(index, version).await;
        network.assert_broadcasts_reach_all_nodes(index + 1).await;

        #[cfg(feature = "metrics")]
        network.assert_no_legacy_connections();
    }
}
