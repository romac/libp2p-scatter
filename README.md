[![API Documentation][docs-image]][docs-link]
[![Build Status][build-image]][build-link]
[![codecov][codecov-image]][codecov-link]
![Rust Stable][rustc-image]
![Apache 2.0 Licensed][license-apache-image]
![MIT Licensed][license-mit-image]

# libp2p-scatter

Implementation of a `rust-libp2p` protocol for broadcast messages to connected peers.

Originally forked from https://github.com/cloudpeers/libp2p-broadcast.

## API Overview

```rust
// Create behaviour
let mut behaviour = Behaviour::new(Config::default());

// Subscribe to topics
behaviour.subscribe(Topic::new(b"announcements"));

// Broadcast messages
behaviour.broadcast(&topic, Bytes::from("Hello!"));

// Query subscriptions
for topic in behaviour.subscribed() {
    println!("Subscribed to: {topic}");
}

// Query peers on a topic
if let Some(peers) = behaviour.peers(&topic) {
    for peer in peers {
        println!("Peer: {peer}");
    }
}
```

## Example

```rust
use futures::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::{Multiaddr, identity, noise, tcp, yamux};

use bytes::Bytes;
use libp2p_scatter as scatter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments (if any)
    // Usage: ./example [to-peer-multiaddr]
    let args: Vec<String> = std::env::args().collect();

    // If a multiaddr is provided, the node will attempt to connect to that peer.
    let connect_addr = if args.len() > 1 {
        Some(args[1].parse::<Multiaddr>()?)
    } else {
        None
    };

    // Create a new libp2p identity
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = local_key.public().to_peer_id();
    println!("Local peer id: {local_peer_id}");

    // Create a scatter behaviour with default config
    let behaviour = scatter::Behaviour::new(scatter::Config::default());

    // Build the swarm
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|_| behaviour)?
        .build();

    // Listen on a local address
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    if let Some(addr) = connect_addr {
        swarm.dial(addr.clone())?;
        println!("Dialed {addr}");
    }

    // Subscribe to a topic
    let topic = scatter::Topic::new(b"my-topic");
    swarm.behaviour_mut().subscribe(topic);

    // Event loop
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::Behaviour(scatter::Event::Subscribed(peer_id, topic)) => {
                println!("Peer {peer_id} subscribed to topic: {topic}");

                // Broadcast a message when a peer subscribes
                let message = Bytes::from("Hello, peer!");
                swarm.behaviour_mut().broadcast(&topic, message);
            }
            SwarmEvent::Behaviour(scatter::Event::Unsubscribed(peer_id, topic)) => {
                println!("Peer {peer_id} unsubscribed from topic: {topic}");
            }
            SwarmEvent::Behaviour(scatter::Event::Received(peer_id, topic, message)) => {
                println!("Received message from {peer_id} on {topic}: {message:?}");
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("Listening on {address}");
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                println!("Connected to {peer_id}");
            }
            _ => {}
        }
    }
}
```

## License

MIT OR Apache-2.0

[build-image]: https://github.com/romac/libp2p-scatter/workflows/Rust/badge.svg
[build-link]: https://github.com/romac/libp2p-scatter/actions?query=workflow%3ARust
[docs-image]: https://docs.rs/libp2p-scatter/badge.svg
[docs-link]: https://docs.rs/libp2p-scatter
[license-apache-image]: https://img.shields.io/badge/license-Apache_2.0-blue.svg
[license-mit-image]: https://img.shields.io/badge/license-MIT-blue.svg
[rustc-image]: https://img.shields.io/badge/rustc-stable-orange.svg
[codecov-image]: https://codecov.io/github/romac/libp2p-scatter/branch/main/graph/badge.svg?token=EQNU0W2JJ0
[codecov-link]: https://codecov.io/github/romac/libp2p-scatter
