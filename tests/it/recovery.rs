//! Tests that a node sends its subscriptions again when outbound messages may be lost.

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{SwarmBuilder, noise, tcp, yamux};
use libp2p_scatter::{Behaviour, Config, Event, Topic};
use tokio::time::timeout;

const MAX_MESSAGE_SIZE: usize = 1024;

fn create_swarm(config: Config) -> Swarm<Behaviour> {
    SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .expect("Failed to create TCP transport")
        .with_behaviour(|_| Behaviour::new(config))
        .expect("Failed to create behaviour")
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build()
}

#[tokio::test]
#[test_log::test]
async fn test_subscriptions_are_sent_again_after_the_outbound_stream_fails() {
    let topic = Topic::new(b"topic");

    let mut sender = create_swarm(Config::default());
    let sender_id = *sender.local_peer_id();

    // The receiver drops its inbound stream when a message is too large,
    // which makes the next writes of the sender fail.
    let mut receiver = create_swarm(Config::default().max_message_size(MAX_MESSAGE_SIZE));
    let receiver_id = *receiver.local_peer_id();

    receiver
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .unwrap();
    let addr = loop {
        if let SwarmEvent::NewListenAddr { address, .. } = receiver.select_next_some().await {
            break address;
        }
    };

    sender.behaviour_mut().subscribe(topic);
    receiver.behaviour_mut().subscribe(topic);
    sender.dial(addr).unwrap();

    // Wait until each node knows that the other is subscribed.
    let (mut sender_knows, mut receiver_knows) = (false, false);
    timeout(Duration::from_secs(10), async {
        while !(sender_knows && receiver_knows) {
            tokio::select! {
                event = sender.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Subscribed(peer, t)) = event {
                        sender_knows |= peer == receiver_id && t == topic;
                    }
                }
                event = receiver.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Subscribed(peer, t)) = event {
                        receiver_knows |= peer == sender_id && t == topic;
                    }
                }
            }
        }
    })
    .await
    .expect("Timeout waiting for the initial subscriptions");

    sender
        .behaviour_mut()
        .broadcast(topic, Bytes::from(vec![0; 2 * MAX_MESSAGE_SIZE]));

    // Let the receiver drop its inbound stream.
    let _ = timeout(Duration::from_millis(500), async {
        loop {
            tokio::select! {
                _ = sender.select_next_some() => {}
                _ = receiver.select_next_some() => {}
            }
        }
    })
    .await;

    // Send a single message. Its write fails, and nothing else is sent after it.
    sender
        .behaviour_mut()
        .broadcast(topic, Bytes::from_static(b"msg"));

    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                _ = sender.select_next_some() => {}
                event = receiver.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Subscribed(peer, t)) = event
                        && peer == sender_id
                        && t == topic
                    {
                        break;
                    }
                }
            }
        }
    })
    .await
    .expect("The sender did not send its subscription again");

    assert!(sender.is_connected(&receiver_id));
}
