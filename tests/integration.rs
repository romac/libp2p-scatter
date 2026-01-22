//! Integration tests for the libp2p-scatter protocol.
//!
//! These tests use real libp2p Swarms with TCP transport to verify
//! the protocol works correctly end-to-end.

mod common;

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p_scatter::{Event, Topic};
use tokio::time::timeout;
use tracing::info;

use crate::common::*;

// ==================== Basic Connectivity Tests ====================

#[tokio::test]
#[test_log::test]
async fn test_two_peers_connect() {
    let (swarm_a, swarm_b) = create_connected_pair().await;

    // Verify both swarms know about each other
    let peer_a = *swarm_a.local_peer_id();
    let peer_b = *swarm_b.local_peer_id();

    assert!(swarm_a.is_connected(&peer_b));
    assert!(swarm_b.is_connected(&peer_a));
}

#[tokio::test]
#[test_log::test]
async fn test_subscribe_propagates() {
    let (mut swarm_a, mut swarm_b) = create_connected_pair().await;

    let topic = Topic::new(b"test-topic");
    let peer_a = *swarm_a.local_peer_id();

    // A subscribes
    swarm_a.behaviour_mut().subscribe(topic);

    // B should receive the subscription event
    let event = wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::B,
        |e| matches!(e, Event::Subscribed(_, t) if *t == topic),
    )
    .await
    .expect("Timeout waiting for subscription");

    assert_eq!(event, Event::Subscribed(peer_a, topic));
}

#[tokio::test]
#[test_log::test]
async fn test_broadcast_delivery() {
    let (mut swarm_a, mut swarm_b) = create_connected_pair().await;

    let topic = Topic::new(b"test-topic");
    let peer_a = *swarm_a.local_peer_id();
    let peer_b = *swarm_b.local_peer_id();
    let payload = Bytes::from_static(b"hello world");

    // B subscribes first
    swarm_b.behaviour_mut().subscribe(topic);

    info!("B subscribed to topic");

    // Wait for A to see B's subscription (this also drives B to send the subscription)
    wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::A,
        |e| matches!(e, Event::Subscribed(p, t) if *p == peer_b && *t == topic),
    )
    .await
    .expect("Timeout waiting for subscription");

    info!("A saw B's subscription");

    // // Now A subscribes too (so A is known to B for the topic)
    // swarm_a.behaviour_mut().subscribe(topic);

    // // Wait for B to see A's subscription
    // wait_for_scatter_event(
    //     &mut swarm_a,
    //     &mut swarm_b,
    //     Target::B,
    //     |e| matches!(e, Event::Subscribed(p, t) if *p == peer_a && *t == topic),
    // )
    // .await
    // .expect("Timeout waiting for subscription");

    // A broadcasts
    swarm_a.behaviour_mut().broadcast(&topic, payload.clone());

    info!("A broadcasted message on topic");

    // B should receive the broadcast
    let event = wait_for_scatter_event(&mut swarm_a, &mut swarm_b, Target::B, |e| {
        matches!(e, Event::Received(_, _, _))
    })
    .await
    .expect("Timeout waiting for broadcast");

    info!("B received broadcasted message");

    assert_eq!(event, Event::Received(peer_a, topic, payload));
}

#[tokio::test]
#[test_log::test]
async fn test_unsubscribe_propagates() {
    let (mut swarm_a, mut swarm_b) = create_connected_pair().await;

    let topic = Topic::new(b"test-topic");
    let peer_a = *swarm_a.local_peer_id();

    // B subscribes (so A will have B in its topic peers)
    swarm_b.behaviour_mut().subscribe(topic);

    // Wait for A to see B's subscription
    wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::A,
        |e| matches!(e, Event::Subscribed(_, t) if *t == topic),
    )
    .await;

    // A subscribes
    swarm_a.behaviour_mut().subscribe(topic);

    // Wait for B to see A's subscription
    wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::B,
        |e| matches!(e, Event::Subscribed(p, t) if *p == peer_a && *t == topic),
    )
    .await;

    // A unsubscribes
    swarm_a.behaviour_mut().unsubscribe(&topic);

    // B should receive the unsubscription event
    let event = wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::B,
        |e| matches!(e, Event::Unsubscribed(_, t) if *t == topic),
    )
    .await
    .expect("Timeout waiting for unsubscription");

    assert_eq!(event, Event::Unsubscribed(peer_a, topic));
}

// ==================== Multi-Peer Tests ====================

#[tokio::test]
#[test_log::test]
async fn test_three_peers_broadcast() {
    let (mut swarm_a, mut swarm_b, mut swarm_c) = create_connected_trio().await;

    let topic = Topic::new(b"broadcast-topic");
    let peer_a = *swarm_a.local_peer_id();
    let payload = Bytes::from_static(b"message to all");

    // B and C subscribe
    swarm_b.behaviour_mut().subscribe(topic);
    swarm_c.behaviour_mut().subscribe(topic);

    // Wait for A to see both subscriptions
    let mut b_subscribed = false;
    let mut c_subscribed = false;
    let peer_b = *swarm_b.local_peer_id();
    let peer_c = *swarm_c.local_peer_id();

    let result = timeout(DEFAULT_TIMEOUT, async {
        loop {
            tokio::select! {
                event = swarm_a.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Subscribed(p, t)) = event {
                        if t == topic {
                            if p == peer_b {
                                b_subscribed = true;
                            } else if p == peer_c {
                                c_subscribed = true;
                            }
                        }
                    }
                }
                event = swarm_b.select_next_some() => {
                    // Drive B
                    let _ = event;
                }
                event = swarm_c.select_next_some() => {
                    // Drive C
                    let _ = event;
                }
            }

            if b_subscribed && c_subscribed {
                break;
            }
        }
    })
    .await;
    result.expect("Timeout waiting for subscriptions");

    // A broadcasts
    swarm_a.behaviour_mut().broadcast(&topic, payload.clone());

    // Both B and C should receive it
    let mut b_received = false;
    let mut c_received = false;

    let result = timeout(DEFAULT_TIMEOUT, async {
        loop {
            tokio::select! {
                event = swarm_a.select_next_some() => {
                    let _ = event;
                }
                event = swarm_b.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Received(p, t, msg)) = event {
                        if p == peer_a && t == topic && msg == payload {
                            b_received = true;
                        }
                    }
                }
                event = swarm_c.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Received(p, t, msg)) = event {
                        if p == peer_a && t == topic && msg == payload {
                            c_received = true;
                        }
                    }
                }
            }

            if b_received && c_received {
                break;
            }
        }
    })
    .await;
    result.expect("Timeout waiting for broadcasts");

    assert!(b_received);
    assert!(c_received);
}

#[tokio::test]
#[test_log::test]
async fn test_selective_broadcast() {
    let (mut swarm_a, mut swarm_b, mut swarm_c) = create_connected_trio().await;

    let topic1 = Topic::new(b"topic1");
    let topic2 = Topic::new(b"topic2");
    let peer_a = *swarm_a.local_peer_id();
    let peer_b = *swarm_b.local_peer_id();
    let peer_c = *swarm_c.local_peer_id();
    let payload = Bytes::from_static(b"selective message");

    // B subscribes to topic1, C subscribes to topic2
    swarm_b.behaviour_mut().subscribe(topic1);
    swarm_c.behaviour_mut().subscribe(topic2);

    // Wait for A to see both subscriptions
    let mut b_sub = false;
    let mut c_sub = false;

    let result = timeout(DEFAULT_TIMEOUT, async {
        loop {
            tokio::select! {
                event = swarm_a.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Subscribed(p, t)) = event {
                        if p == peer_b && t == topic1 {
                            b_sub = true;
                        } else if p == peer_c && t == topic2 {
                            c_sub = true;
                        }
                    }
                }
                _ = swarm_b.select_next_some() => {}
                _ = swarm_c.select_next_some() => {}
            }
            if b_sub && c_sub {
                break;
            }
        }
    })
    .await;
    result.expect("Timeout waiting for subscriptions");

    // A broadcasts to topic1 only
    swarm_a.behaviour_mut().broadcast(&topic1, payload.clone());

    // Only B should receive it, not C
    let mut b_received = false;

    let result = timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                _ = swarm_a.select_next_some() => {}
                event = swarm_b.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Received(p, t, msg)) = event {
                        if p == peer_a && t == topic1 && msg == payload {
                            b_received = true;
                            break;
                        }
                    }
                }
                event = swarm_c.select_next_some() => {
                    // C should not receive anything on topic1
                    if let SwarmEvent::Behaviour(Event::Received(_, t, _)) = event {
                        if t == topic1 {
                            panic!("C received message on topic1 which it didn't subscribe to");
                        }
                    }
                }
            }
        }
    })
    .await;
    result.expect("Timeout waiting for B to receive");

    assert!(b_received);
}

// ==================== Topic Management Tests ====================

#[tokio::test]
#[test_log::test]
async fn test_multiple_topics() {
    let (mut swarm_a, mut swarm_b) = create_connected_pair().await;

    let topic1 = Topic::new(b"topic-one");
    let topic2 = Topic::new(b"topic-two");
    let peer_a = *swarm_a.local_peer_id();
    let peer_b = *swarm_b.local_peer_id();

    // // A subscribes to both topics
    // swarm_a.behaviour_mut().subscribe(topic1);
    // swarm_a.behaviour_mut().subscribe(topic2);

    // B should see both subscriptions
    let mut sub1 = false;
    let mut sub2 = false;

    wait_for_scatter_event(&mut swarm_a, &mut swarm_b, Target::B, |e| {
        if let Event::Subscribed(p, t) = e {
            if *p == peer_a && *t == topic1 {
                sub1 = true;
            } else if *p == peer_a && *t == topic2 {
                sub2 = true;
            }
        }
        sub1 && sub2
    })
    .await
    .expect("Timeout waiting for subscriptions");

    // B also subscribes to topic1
    swarm_b.behaviour_mut().subscribe(topic1);

    // Wait for A to see B's subscription
    wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::A,
        |e| matches!(e, Event::Subscribed(p, t) if *p == peer_b && *t == topic1),
    )
    .await;

    // A sends on topic1
    let msg1 = Bytes::from_static(b"message on topic1");
    swarm_a.behaviour_mut().broadcast(&topic1, msg1.clone());

    // B receives on topic1
    let event = wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::B,
        |e| matches!(e, Event::Received(_, t, _) if *t == topic1),
    )
    .await
    .expect("Timeout waiting for message on topic1");

    assert_eq!(event, Event::Received(peer_a, topic1, msg1));
}

#[tokio::test]
#[test_log::test]
async fn test_late_subscriber() {
    let (mut swarm_a, mut swarm_b) = create_connected_pair().await;

    let topic = Topic::new(b"late-sub-topic");
    let peer_b = *swarm_b.local_peer_id();

    // A subscribes first
    swarm_a.behaviour_mut().subscribe(topic);

    // B subscribes later
    swarm_b.behaviour_mut().subscribe(topic);

    // A should see B's subscription
    let event = wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::A,
        |e| matches!(e, Event::Subscribed(p, t) if *p == peer_b && *t == topic),
    )
    .await
    .expect("Timeout waiting for late subscription");

    assert_eq!(event, Event::Subscribed(peer_b, topic));
}

// ==================== Edge Cases ====================

#[tokio::test]
#[test_log::test]
async fn test_empty_payload_broadcast() {
    let (mut swarm_a, mut swarm_b) = create_connected_pair().await;

    let topic = Topic::new(b"empty-payload");
    let peer_a = *swarm_a.local_peer_id();
    let peer_b = *swarm_b.local_peer_id();
    let payload = Bytes::new(); // Empty

    // B subscribes first
    swarm_b.behaviour_mut().subscribe(topic);

    // Wait for A to see B's subscription
    wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::A,
        |e| matches!(e, Event::Subscribed(p, _) if *p == peer_b),
    )
    .await;

    // // A subscribes so A knows B is subscribed to topic
    // swarm_a.behaviour_mut().subscribe(topic);
    //
    // // Wait for B to see A's subscription
    // wait_for_scatter_event(
    //     &mut swarm_a,
    //     &mut swarm_b,
    //     Target::B,
    //     |e| matches!(e, Event::Subscribed(p, _) if *p == peer_a),
    // )
    // .await;

    // A broadcasts empty payload
    swarm_a.behaviour_mut().broadcast(&topic, payload.clone());

    // B should still receive it
    let event = wait_for_scatter_event(&mut swarm_a, &mut swarm_b, Target::B, |e| {
        matches!(e, Event::Received(_, _, _))
    })
    .await
    .expect("Timeout waiting for empty payload broadcast");

    assert_eq!(event, Event::Received(peer_a, topic, payload));
}

#[tokio::test]
#[test_log::test]
async fn test_large_payload_broadcast() {
    let (mut swarm_a, mut swarm_b) = create_connected_pair().await;

    let topic = Topic::new(b"large-payload");
    let peer_a = *swarm_a.local_peer_id();
    let peer_b = *swarm_b.local_peer_id();

    // Create a payload of 100KB
    let payload = Bytes::from(vec![0xABu8; 100_000]);

    // B subscribes first
    swarm_b.behaviour_mut().subscribe(topic);

    // Wait for A to see B's subscription
    wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::A,
        |e| matches!(e, Event::Subscribed(p, _) if *p == peer_b),
    )
    .await;

    // // A subscribes
    // swarm_a.behaviour_mut().subscribe(topic);
    //
    // // Wait for B to see A's subscription
    // wait_for_scatter_event(
    //     &mut swarm_a,
    //     &mut swarm_b,
    //     Target::B,
    //     |e| matches!(e, Event::Subscribed(p, _) if *p == peer_a),
    // )
    // .await;

    // A broadcasts large payload
    swarm_a.behaviour_mut().broadcast(&topic, payload.clone());

    // B should receive it
    let event = wait_for_scatter_event(&mut swarm_a, &mut swarm_b, Target::B, |e| {
        matches!(e, Event::Received(_, _, _))
    })
    .await
    .expect("Timeout waiting for large payload broadcast");

    assert_eq!(event, Event::Received(peer_a, topic, payload));
}

#[tokio::test]
#[test_log::test]
async fn test_multiple_broadcasts_in_sequence() {
    let (mut swarm_a, mut swarm_b) = create_connected_pair().await;

    let topic = Topic::new(b"sequence-topic");
    let peer_a = *swarm_a.local_peer_id();
    let peer_b = *swarm_b.local_peer_id();

    // B subscribes first
    swarm_b.behaviour_mut().subscribe(topic);

    // Wait for A to see B's subscription
    wait_for_scatter_event(
        &mut swarm_a,
        &mut swarm_b,
        Target::A,
        |e| matches!(e, Event::Subscribed(p, _) if *p == peer_b),
    )
    .await;

    // A subscribes
    // swarm_a.behaviour_mut().subscribe(topic);
    //
    // // Wait for B to see A's subscription
    // wait_for_scatter_event(
    //     &mut swarm_a,
    //     &mut swarm_b,
    //     Target::B,
    //     |e| matches!(e, Event::Subscribed(p, _) if *p == peer_a),
    // )
    // .await;

    // A sends multiple messages
    let messages: Vec<Bytes> = (0..5)
        .map(|i| Bytes::from(format!("message-{}", i)))
        .collect();

    for msg in &messages {
        swarm_a.behaviour_mut().broadcast(&topic, msg.clone());
    }

    // B should receive all of them
    let mut received = Vec::new();

    let result = timeout(DEFAULT_TIMEOUT, async {
        loop {
            tokio::select! {
                _ = swarm_a.select_next_some() => {}
                event = swarm_b.select_next_some() => {
                    if let SwarmEvent::Behaviour(Event::Received(p, t, msg)) = event {
                        if p == peer_a && t == topic {
                            received.push(msg);
                        }
                    }
                }
            }

            if received.len() >= messages.len() {
                break;
            }
        }
    })
    .await;
    result.expect("Timeout waiting for messages");

    // Verify all messages were received (order may vary due to async)
    assert_eq!(received.len(), messages.len());
    for msg in &messages {
        assert!(received.contains(msg), "Missing message: {:?}", msg);
    }
}
