#![allow(dead_code)]

//! Common test utilities for integration tests.
//!
//! This module provides utilities for creating and managing test networks
//! with arbitrary numbers of nodes.

use std::collections::HashMap;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{noise, tcp, yamux, Multiaddr, PeerId, SwarmBuilder};
use libp2p_scatter::{Behaviour, Config, Event};
use tokio::time::timeout;

/// Default timeout for waiting on events.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

// =============================================================================
// Single Swarm Creation
// =============================================================================

/// Creates a new Swarm with the scatter Behaviour using TCP transport.
pub fn create_swarm() -> Swarm<Behaviour> {
    SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .expect("Failed to create TCP transport")
        .with_behaviour(|_| Behaviour::new(Config::default()))
        .expect("Failed to create behaviour")
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build()
}

/// Creates a swarm and starts listening on a random local port.
/// Returns the swarm and its listen address.
pub async fn create_listening_swarm() -> (Swarm<Behaviour>, Multiaddr) {
    let mut swarm = create_swarm();
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .expect("Failed to start listening");

    // Wait for the listen address to be assigned
    loop {
        if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
            return (swarm, address);
        }
    }
}

// =============================================================================
// Multi-Node Test Network
// =============================================================================

/// A test network with an arbitrary number of nodes.
///
/// Nodes are identified by index (0, 1, 2, ...). The network can be configured
/// with different topologies using the builder pattern.
///
/// # Example
///
/// ```ignore
/// // Create a fully connected 5-node network
/// let mut network = TestNetwork::new(5).fully_connected().await;
///
/// // Subscribe node 0 to a topic
/// let topic = Topic::new(b"test");
/// network.node_mut(0).behaviour_mut().subscribe(topic);
///
/// // Wait for all other nodes to see the subscription
/// network.wait_for_events(|events| {
///     events.iter().filter(|(idx, e)| {
///         *idx != 0 && matches!(e, Event::Subscribed(_, t) if *t == topic)
///     }).count() >= 4
/// }).await;
/// ```
pub struct TestNetwork {
    nodes: Vec<Node>,
}

/// A node in the test network.
pub struct Node {
    swarm: Swarm<Behaviour>,
    addr: Multiaddr,
}

impl Node {
    /// Returns the peer ID of this node.
    pub fn peer_id(&self) -> PeerId {
        *self.swarm.local_peer_id()
    }

    /// Returns the listen address of this node.
    pub fn addr(&self) -> &Multiaddr {
        &self.addr
    }

    /// Returns a reference to the swarm.
    pub fn swarm(&self) -> &Swarm<Behaviour> {
        &self.swarm
    }

    /// Returns a mutable reference to the swarm.
    pub fn swarm_mut(&mut self) -> &mut Swarm<Behaviour> {
        &mut self.swarm
    }

    /// Returns a reference to the scatter behaviour.
    pub fn behaviour(&self) -> &Behaviour {
        self.swarm.behaviour()
    }

    /// Returns a mutable reference to the scatter behaviour.
    pub fn behaviour_mut(&mut self) -> &mut Behaviour {
        self.swarm.behaviour_mut()
    }
}

/// Network topology configurations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    /// No connections - nodes are isolated.
    Disconnected,
    /// Star topology - node 0 is the hub, all others connect to it.
    Star,
    /// Linear/chain topology - each node connects to the next (0-1-2-3-...).
    Linear,
    /// Ring topology - linear plus last node connects to first.
    Ring,
    /// Fully connected - every node connects to every other node.
    FullyConnected,
}

impl TestNetwork {
    /// Creates a new test network with the specified number of nodes.
    /// Nodes are created but not connected. Use topology methods to connect them.
    pub async fn new(count: usize) -> Self {
        assert!(count > 0, "Network must have at least one node");

        let mut nodes = Vec::with_capacity(count);

        // Create all nodes concurrently
        let mut futs = FuturesUnordered::new();
        for _ in 0..count {
            futs.push(create_listening_swarm());
        }

        while let Some((swarm, addr)) = futs.next().await {
            nodes.push(Node { swarm, addr });
        }

        Self { nodes }
    }

    /// Returns the number of nodes in the network.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns true if the network has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Returns a reference to a node by index.
    pub fn node(&self, index: usize) -> &Node {
        &self.nodes[index]
    }

    /// Returns a mutable reference to a node by index.
    pub fn node_mut(&mut self, index: usize) -> &mut Node {
        &mut self.nodes[index]
    }

    /// Returns the peer ID of a node by index.
    pub fn peer_id(&self, index: usize) -> PeerId {
        self.nodes[index].peer_id()
    }

    /// Returns all peer IDs in the network.
    pub fn peer_ids(&self) -> Vec<PeerId> {
        self.nodes.iter().map(|n| n.peer_id()).collect()
    }

    /// Returns an iterator over all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter()
    }

    /// Returns a mutable iterator over all nodes.
    pub fn nodes_mut(&mut self) -> impl Iterator<Item = &mut Node> {
        self.nodes.iter_mut()
    }

    /// Connects nodes according to the specified topology.
    pub async fn connect(mut self, topology: Topology) -> Self {
        match topology {
            Topology::Disconnected => {}
            Topology::Star => self.connect_star().await,
            Topology::Linear => self.connect_linear().await,
            Topology::Ring => self.connect_ring().await,
            Topology::FullyConnected => self.connect_fully().await,
        }
        self
    }

    /// Convenience method: creates a star topology network.
    pub async fn star(count: usize) -> Self {
        Self::new(count).await.connect(Topology::Star).await
    }

    /// Convenience method: creates a fully connected network.
    pub async fn fully_connected(count: usize) -> Self {
        Self::new(count)
            .await
            .connect(Topology::FullyConnected)
            .await
    }

    /// Convenience method: creates a linear chain network.
    pub async fn linear(count: usize) -> Self {
        Self::new(count).await.connect(Topology::Linear).await
    }

    /// Convenience method: creates a ring network.
    pub async fn ring(count: usize) -> Self {
        Self::new(count).await.connect(Topology::Ring).await
    }

    /// Connects node `from` to node `to`.
    pub async fn connect_nodes(&mut self, from: usize, to: usize) {
        let addr = self.nodes[to].addr.clone();
        self.nodes[from].swarm.dial(addr).expect("Failed to dial");

        // Wait for connection on both sides
        let mut from_connected = false;
        let mut to_connected = false;

        let result = timeout(DEFAULT_TIMEOUT, async {
            loop {
                for (idx, node) in self.nodes.iter_mut().enumerate() {
                    if let Some(SwarmEvent::ConnectionEstablished { .. }) =
                        node.swarm.next().now_or_never().flatten()
                    {
                        if idx == from {
                            from_connected = true;
                        } else if idx == to {
                            to_connected = true;
                        }
                    }
                }

                if from_connected && to_connected {
                    break;
                }

                // Small yield to avoid busy loop
                tokio::task::yield_now().await;
            }
        })
        .await;

        result.expect("Timeout waiting for connection");
    }

    /// Creates star topology: node 0 is hub, all others connect to it.
    async fn connect_star(&mut self) {
        if self.nodes.len() < 2 {
            return;
        }

        let hub_addr = self.nodes[0].addr.clone();
        let expected_connections = (self.nodes.len() - 1) * 2; // Each connection seen by both sides

        // All non-hub nodes dial the hub
        for node in self.nodes.iter_mut().skip(1) {
            node.swarm.dial(hub_addr.clone()).expect("Failed to dial");
        }

        self.wait_for_connections(expected_connections).await;
    }

    /// Creates linear topology: 0-1-2-3-...
    async fn connect_linear(&mut self) {
        if self.nodes.len() < 2 {
            return;
        }

        let expected_connections = (self.nodes.len() - 1) * 2;
        let addrs: Vec<_> = self.nodes.iter().map(|n| n.addr.clone()).collect();

        // Each node (except last) dials the next
        for i in 0..self.nodes.len() - 1 {
            self.nodes[i]
                .swarm
                .dial(addrs[i + 1].clone())
                .expect("Failed to dial");
        }

        self.wait_for_connections(expected_connections).await;
    }

    /// Creates ring topology: linear + last connects to first.
    async fn connect_ring(&mut self) {
        if self.nodes.len() < 2 {
            return;
        }

        let expected_connections = self.nodes.len() * 2;
        let addrs: Vec<_> = self.nodes.iter().map(|n| n.addr.clone()).collect();

        // Linear connections
        for i in 0..self.nodes.len() - 1 {
            self.nodes[i]
                .swarm
                .dial(addrs[i + 1].clone())
                .expect("Failed to dial");
        }

        // Close the ring
        let last = self.nodes.len() - 1;
        self.nodes[last]
            .swarm
            .dial(addrs[0].clone())
            .expect("Failed to dial");

        self.wait_for_connections(expected_connections).await;
    }

    /// Creates fully connected topology: every node connects to every other.
    #[allow(clippy::needless_range_loop)]
    async fn connect_fully(&mut self) {
        if self.nodes.len() < 2 {
            return;
        }

        let n = self.nodes.len();
        let expected_connections = n * (n - 1); // Each pair has 2 connection events
        let addrs: Vec<_> = self.nodes.iter().map(|n| n.addr.clone()).collect();

        // Each node dials all nodes with higher index
        for i in 0..n {
            for j in i + 1..n {
                self.nodes[i]
                    .swarm
                    .dial(addrs[j].clone())
                    .expect("Failed to dial");
            }
        }

        self.wait_for_connections(expected_connections).await;
    }

    /// Waits for the expected number of connection events across all nodes.
    async fn wait_for_connections(&mut self, expected: usize) {
        let mut connections = 0;

        let result = timeout(Duration::from_secs(5), async {
            loop {
                for node in self.nodes.iter_mut() {
                    while let Some(event) = node.swarm.next().now_or_never().flatten() {
                        if let SwarmEvent::ConnectionEstablished { .. } = event {
                            connections += 1;
                        }
                    }
                }

                if connections >= expected {
                    break;
                }

                tokio::task::yield_now().await;
            }
        })
        .await;

        result.unwrap_or_else(|_| {
            panic!(
                "Timeout waiting for connections: got {}, expected {}",
                connections, expected
            )
        });
    }

    /// Drives all swarms, collecting scatter events until the predicate returns true.
    ///
    /// The predicate receives a slice of (node_index, event) pairs collected so far.
    /// Returns all collected events.
    pub async fn wait_for_events<F>(&mut self, predicate: F) -> Vec<(usize, Event)>
    where
        F: FnMut(&[(usize, Event)]) -> bool,
    {
        self.wait_for_events_timeout(DEFAULT_TIMEOUT, predicate)
            .await
    }

    /// Like `wait_for_events` but with a custom timeout.
    pub async fn wait_for_events_timeout<F>(
        &mut self,
        duration: Duration,
        mut predicate: F,
    ) -> Vec<(usize, Event)>
    where
        F: FnMut(&[(usize, Event)]) -> bool,
    {
        let mut events = Vec::new();

        let result = timeout(duration, async {
            loop {
                for (idx, node) in self.nodes.iter_mut().enumerate() {
                    while let Some(event) = node.swarm.next().now_or_never().flatten() {
                        tracing::debug!(node = idx, ?event, "Network event");
                        if let SwarmEvent::Behaviour(e) = event {
                            events.push((idx, e));
                            if predicate(&events) {
                                return;
                            }
                        }
                    }
                }

                tokio::task::yield_now().await;
            }
        })
        .await;

        result.expect("Timeout waiting for events");
        events
    }

    /// Drives all swarms for a duration, collecting all scatter events.
    pub async fn collect_events(&mut self, duration: Duration) -> Vec<(usize, Event)> {
        let mut events = Vec::new();

        let _ = timeout(duration, async {
            loop {
                for (idx, node) in self.nodes.iter_mut().enumerate() {
                    while let Some(event) = node.swarm.next().now_or_never().flatten() {
                        if let SwarmEvent::Behaviour(e) = event {
                            events.push((idx, e));
                        }
                    }
                }

                tokio::task::yield_now().await;
            }
        })
        .await;

        events
    }

    /// Drives all swarms briefly to process pending events without collecting them.
    pub async fn drive(&mut self) {
        let _ = timeout(Duration::from_millis(50), async {
            loop {
                for node in self.nodes.iter_mut() {
                    while node.swarm.next().now_or_never().flatten().is_some() {}
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
    }

    /// Waits for a specific node to receive an event matching the predicate.
    pub async fn wait_for_event_on<F>(&mut self, node_index: usize, mut predicate: F) -> Event
    where
        F: FnMut(&Event) -> bool,
    {
        let result = timeout(DEFAULT_TIMEOUT, async {
            loop {
                for (idx, node) in self.nodes.iter_mut().enumerate() {
                    while let Some(event) = node.swarm.next().now_or_never().flatten() {
                        tracing::debug!(node = idx, ?event, "Network event");
                        if idx == node_index {
                            if let SwarmEvent::Behaviour(e) = event {
                                if predicate(&e) {
                                    return e;
                                }
                            }
                        }
                    }
                }

                tokio::task::yield_now().await;
            }
        })
        .await;

        result.expect("Timeout waiting for event")
    }

    /// Returns a map of which nodes are connected to which other nodes.
    pub fn connection_map(&self) -> HashMap<usize, Vec<usize>> {
        let peer_to_idx: HashMap<PeerId, usize> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.peer_id(), i))
            .collect();

        self.nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| {
                let connected: Vec<usize> = node
                    .swarm
                    .connected_peers()
                    .filter_map(|p| peer_to_idx.get(p).copied())
                    .collect();
                (idx, connected)
            })
            .collect()
    }

    /// Asserts that the network has the expected connectivity.
    pub fn assert_connected(&self, expected_edges: usize) {
        let map = self.connection_map();
        let total_edges: usize = map.values().map(|v| v.len()).sum();
        // Each edge is counted twice (once from each side)
        assert_eq!(
            total_edges,
            expected_edges * 2,
            "Expected {} edges, found {} (counted {} times)",
            expected_edges,
            total_edges / 2,
            total_edges
        );
    }
}
