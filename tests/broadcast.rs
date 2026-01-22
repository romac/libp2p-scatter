//! Tests for message broadcasting functionality.

mod common;

use bytes::Bytes;
use libp2p_scatter::{Event, Topic};
use tracing::info;

use crate::common::TestNetwork;

#[tokio::test]
#[test_log::test]
async fn test_broadcast_delivery() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"test-topic");
    let peer_0 = network.peer_id(0);
    let peer_1 = network.peer_id(1);
    let payload = Bytes::from_static(b"hello world");

    // Node 1 subscribes first
    network.node_mut(1).behaviour_mut().subscribe(topic);

    info!("Node 1 subscribed to topic");

    // Wait for node 0 to see node 1's subscription
    network
        .wait_for_event_on(
            0,
            |e| matches!(e, Event::Subscribed(p, t) if *p == peer_1 && *t == topic),
        )
        .await;

    info!("Node 0 saw node 1's subscription");

    // Now node 0 subscribes too
    network.node_mut(0).behaviour_mut().subscribe(topic);

    // Wait for node 1 to see node 0's subscription
    network
        .wait_for_event_on(
            1,
            |e| matches!(e, Event::Subscribed(p, t) if *p == peer_0 && *t == topic),
        )
        .await;

    // Node 0 broadcasts
    network
        .node_mut(0)
        .behaviour_mut()
        .broadcast(&topic, payload.clone());

    info!("Node 0 broadcasted message on topic");

    // Node 1 should receive the broadcast
    let event = network
        .wait_for_event_on(1, |e| matches!(e, Event::Received(_, _, _)))
        .await;

    info!("Node 1 received broadcasted message");

    assert_eq!(event, Event::Received(peer_0, topic, payload));
}

#[tokio::test]
#[test_log::test]
async fn test_multiple_topics() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic1 = Topic::new(b"topic-one");
    let topic2 = Topic::new(b"topic-two");
    let peer_0 = network.peer_id(0);
    let peer_1 = network.peer_id(1);

    // Node 0 subscribes to both topics
    network.node_mut(0).behaviour_mut().subscribe(topic1);
    network.node_mut(0).behaviour_mut().subscribe(topic2);

    // Node 1 should see both subscriptions
    network
        .wait_for_events(|events| {
            let has_sub1 = events.iter().any(|(idx, e)| {
                *idx == 1 && matches!(e, Event::Subscribed(p, t) if *p == peer_0 && *t == topic1)
            });
            let has_sub2 = events.iter().any(|(idx, e)| {
                *idx == 1 && matches!(e, Event::Subscribed(p, t) if *p == peer_0 && *t == topic2)
            });
            has_sub1 && has_sub2
        })
        .await;

    // Node 1 also subscribes to topic1
    network.node_mut(1).behaviour_mut().subscribe(topic1);

    // Wait for node 0 to see node 1's subscription
    network
        .wait_for_event_on(
            0,
            |e| matches!(e, Event::Subscribed(p, t) if *p == peer_1 && *t == topic1),
        )
        .await;

    // Node 0 sends on topic1
    let msg1 = Bytes::from_static(b"message on topic1");
    network
        .node_mut(0)
        .behaviour_mut()
        .broadcast(&topic1, msg1.clone());

    // Node 1 receives on topic1
    let event = network
        .wait_for_event_on(1, |e| matches!(e, Event::Received(_, t, _) if *t == topic1))
        .await;

    assert_eq!(event, Event::Received(peer_0, topic1, msg1));
}

#[tokio::test]
#[test_log::test]
async fn test_multiple_broadcasts_in_sequence() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"sequence-topic");
    let peer_0 = network.peer_id(0);
    let peer_1 = network.peer_id(1);

    // Node 1 subscribes first
    network.node_mut(1).behaviour_mut().subscribe(topic);

    // Wait for node 0 to see node 1's subscription
    network
        .wait_for_event_on(0, |e| matches!(e, Event::Subscribed(p, _) if *p == peer_1))
        .await;

    // Node 0 subscribes
    network.node_mut(0).behaviour_mut().subscribe(topic);

    // Wait for node 1 to see node 0's subscription
    network
        .wait_for_event_on(1, |e| matches!(e, Event::Subscribed(p, _) if *p == peer_0))
        .await;

    // Node 0 sends multiple messages
    let messages: Vec<Bytes> = (0..5)
        .map(|i| Bytes::from(format!("message-{}", i)))
        .collect();

    for msg in &messages {
        network
            .node_mut(0)
            .behaviour_mut()
            .broadcast(&topic, msg.clone());
    }

    // Node 1 should receive all of them
    let events = network
        .wait_for_events(|events| {
            let recv_count = events
                .iter()
                .filter(|(idx, e)| {
                    *idx == 1
                        && matches!(e, Event::Received(p, t, _) if *p == peer_0 && *t == topic)
                })
                .count();
            recv_count >= messages.len()
        })
        .await;

    // Extract received messages
    let received: Vec<Bytes> = events
        .iter()
        .filter_map(|(idx, e)| {
            if *idx == 1 {
                if let Event::Received(_, _, msg) = e {
                    return Some(msg.clone());
                }
            }
            None
        })
        .collect();

    // Verify all messages were received (order may vary due to async)
    assert_eq!(received.len(), messages.len());
    for msg in &messages {
        assert!(received.contains(msg), "Missing message: {:?}", msg);
    }
}
