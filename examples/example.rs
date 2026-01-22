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

    let topic = scatter::Topic::new(b"my-topic");

    if let Some(addr) = connect_addr {
        swarm.dial(addr.clone())?;
        println!("Dialed {addr}");
    }

    // Subscribe to a topic
    swarm.behaviour_mut().subscribe(topic);
    println!("Subscribed to topic: {topic}");

    // Event loop
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::Behaviour(scatter::Event::Subscribed(peer_id, topic)) => {
                println!("Peer {peer_id} subscribed to topic: {topic}");

                let message = Bytes::from(format!("A new peer has joined: {peer_id}!"));
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
