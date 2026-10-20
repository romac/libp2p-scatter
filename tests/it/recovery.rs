//! Tests of how a node handles messages that it cannot send.

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

/// Connects a sender to a receiver, both subscribed to `topic`,
/// and waits until each node knows that the other is subscribed.
async fn subscribed_pair(
    sender_config: Config,
    receiver_config: Config,
    topic: Topic,
) -> (Swarm<Behaviour>, Swarm<Behaviour>) {
    let mut sender = create_swarm(sender_config);
    let sender_id = *sender.local_peer_id();

    let mut receiver = create_swarm(receiver_config);
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

    (sender, receiver)
}

#[tokio::test]
#[test_log::test]
async fn test_subscriptions_are_sent_again_after_the_outbound_stream_fails() {
    let topic = Topic::new(b"topic");

    // The receiver drops its inbound stream when a message is too large,
    // which makes the next writes of the sender fail.
    let (mut sender, mut receiver) = subscribed_pair(
        Config::default(),
        Config::default().max_message_size(MAX_MESSAGE_SIZE),
        topic,
    )
    .await;
    let sender_id = *sender.local_peer_id();
    let receiver_id = *receiver.local_peer_id();

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

#[tokio::test]
#[test_log::test]
async fn test_messages_above_max_size_are_dropped_without_closing_the_stream() {
    let topic = Topic::new(b"topic");

    let (mut sender, mut receiver) = subscribed_pair(
        Config::default().max_message_size(MAX_MESSAGE_SIZE),
        Config::default(),
        topic,
    )
    .await;
    let sender_id = *sender.local_peer_id();
    let receiver_id = *receiver.local_peer_id();

    // More messages than the failed sends in a row after which a connection is closed.
    for _ in 0..10 {
        sender
            .behaviour_mut()
            .broadcast(topic, Bytes::from(vec![0; 2 * MAX_MESSAGE_SIZE]));
    }
    sender
        .behaviour_mut()
        .broadcast(topic, Bytes::from_static(b"msg"));

    let received = timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                _ = sender.select_next_some() => {}
                event = receiver.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Received(peer, t, payload)) = event {
                        break (peer, t, payload);
                    }
                }
            }
        }
    })
    .await
    .expect("The receiver did not receive the message");

    assert_eq!(received, (sender_id, topic, Bytes::from_static(b"msg")));
    assert!(sender.is_connected(&receiver_id));
}
