//! Integration tests for the libp2p-scatter protocol.
//!
//! These tests use real libp2p Swarms with TCP transport to verify
//! the protocol works correctly end-to-end.

mod common;

use std::time::Duration;

use bytes::Bytes;
use libp2p_scatter::{Event, Topic};
use tracing::info;

use crate::common::TestNetwork;

// ==================== Basic Connectivity Tests ====================

#[tokio::test]
#[test_log::test]
async fn test_two_peers_connect() {
    let network = TestNetwork::fully_connected(2).await;

    let peer_0 = network.peer_id(0);
    let peer_1 = network.peer_id(1);

    assert!(network.node(0).swarm().is_connected(&peer_1));
    assert!(network.node(1).swarm().is_connected(&peer_0));
}

#[tokio::test]
#[test_log::test]
async fn test_subscribe_propagates() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"test-topic");
    let peer_0 = network.peer_id(0);

    // Node 0 subscribes
    network.node_mut(0).behaviour_mut().subscribe(topic);

    // Node 1 should receive the subscription event
    let event = network
        .wait_for_event_on(1, |e| matches!(e, Event::Subscribed(_, t) if *t == topic))
        .await;

    assert_eq!(event, Event::Subscribed(peer_0, topic));
}

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
        .wait_for_event_on(0, |e| {
            matches!(e, Event::Subscribed(p, t) if *p == peer_1 && *t == topic)
        })
        .await;

    info!("Node 0 saw node 1's subscription");

    // Now node 0 subscribes too
    network.node_mut(0).behaviour_mut().subscribe(topic);

    // Wait for node 1 to see node 0's subscription
    network
        .wait_for_event_on(1, |e| {
            matches!(e, Event::Subscribed(p, t) if *p == peer_0 && *t == topic)
        })
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
async fn test_unsubscribe_propagates() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"test-topic");
    let peer_0 = network.peer_id(0);

    // Node 1 subscribes (so node 0 will have node 1 in its topic peers)
    network.node_mut(1).behaviour_mut().subscribe(topic);

    // Wait for node 0 to see node 1's subscription
    network
        .wait_for_event_on(0, |e| matches!(e, Event::Subscribed(_, t) if *t == topic))
        .await;

    // Node 0 subscribes
    network.node_mut(0).behaviour_mut().subscribe(topic);

    // Wait for node 1 to see node 0's subscription
    network
        .wait_for_event_on(1, |e| {
            matches!(e, Event::Subscribed(p, t) if *p == peer_0 && *t == topic)
        })
        .await;

    // Node 0 unsubscribes
    network.node_mut(0).behaviour_mut().unsubscribe(&topic);

    // Node 1 should receive the unsubscription event
    let event = network
        .wait_for_event_on(1, |e| matches!(e, Event::Unsubscribed(_, t) if *t == topic))
        .await;

    assert_eq!(event, Event::Unsubscribed(peer_0, topic));
}

// ==================== Multi-Peer Tests ====================

#[tokio::test]
#[test_log::test]
async fn test_three_peers_broadcast() {
    let mut network = TestNetwork::star(3).await;

    let topic = Topic::new(b"broadcast-topic");
    let peer_0 = network.peer_id(0);
    let peer_1 = network.peer_id(1);
    let peer_2 = network.peer_id(2);
    let payload = Bytes::from_static(b"message to all");

    // Nodes 1 and 2 subscribe
    network.node_mut(1).behaviour_mut().subscribe(topic);
    network.node_mut(2).behaviour_mut().subscribe(topic);

    // Wait for node 0 to see both subscriptions
    network
        .wait_for_events(|events| {
            let sub_count = events
                .iter()
                .filter(|(idx, e)| {
                    *idx == 0
                        && matches!(e, Event::Subscribed(p, t) if *t == topic && (*p == peer_1 || *p == peer_2))
                })
                .count();
            sub_count >= 2
        })
        .await;

    // Node 0 broadcasts
    network
        .node_mut(0)
        .behaviour_mut()
        .broadcast(&topic, payload.clone());

    // Both nodes 1 and 2 should receive it
    let events = network
        .wait_for_events(|events| {
            let recv_count = events
                .iter()
                .filter(|(idx, e)| {
                    (*idx == 1 || *idx == 2)
                        && matches!(e, Event::Received(p, t, msg) if *p == peer_0 && *t == topic && *msg == payload)
                })
                .count();
            recv_count >= 2
        })
        .await;

    // Verify both received
    let node1_received = events
        .iter()
        .any(|(idx, e)| *idx == 1 && matches!(e, Event::Received(_, _, _)));
    let node2_received = events
        .iter()
        .any(|(idx, e)| *idx == 2 && matches!(e, Event::Received(_, _, _)));

    assert!(node1_received, "Node 1 should have received the broadcast");
    assert!(node2_received, "Node 2 should have received the broadcast");
}

#[tokio::test]
#[test_log::test]
async fn test_selective_broadcast() {
    let mut network = TestNetwork::star(3).await;

    let topic1 = Topic::new(b"topic1");
    let topic2 = Topic::new(b"topic2");
    let peer_0 = network.peer_id(0);
    let peer_1 = network.peer_id(1);
    let peer_2 = network.peer_id(2);
    let payload = Bytes::from_static(b"selective message");

    // Node 1 subscribes to topic1, node 2 subscribes to topic2
    network.node_mut(1).behaviour_mut().subscribe(topic1);
    network.node_mut(2).behaviour_mut().subscribe(topic2);

    // Wait for node 0 to see both subscriptions
    network
        .wait_for_events(|events| {
            let has_1_sub = events.iter().any(|(idx, e)| {
                *idx == 0 && matches!(e, Event::Subscribed(p, t) if *p == peer_1 && *t == topic1)
            });
            let has_2_sub = events.iter().any(|(idx, e)| {
                *idx == 0 && matches!(e, Event::Subscribed(p, t) if *p == peer_2 && *t == topic2)
            });
            has_1_sub && has_2_sub
        })
        .await;

    // Node 0 broadcasts to topic1 only
    network
        .node_mut(0)
        .behaviour_mut()
        .broadcast(&topic1, payload.clone());

    // Only node 1 should receive it
    let event = network
        .wait_for_event_on(1, |e| {
            matches!(e, Event::Received(p, t, msg) if *p == peer_0 && *t == topic1 && *msg == payload)
        })
        .await;

    assert_eq!(event, Event::Received(peer_0, topic1, payload.clone()));

    // Give some time and verify node 2 didn't receive anything on topic1
    let events = network.collect_events(Duration::from_millis(100)).await;
    let node2_received_topic1 = events.iter().any(|(idx, e)| {
        *idx == 2 && matches!(e, Event::Received(_, t, _) if *t == topic1)
    });
    assert!(
        !node2_received_topic1,
        "Node 2 should not have received message on topic1"
    );
}

// ==================== Topic Management Tests ====================

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
        .wait_for_event_on(0, |e| {
            matches!(e, Event::Subscribed(p, t) if *p == peer_1 && *t == topic1)
        })
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
async fn test_late_subscriber() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"late-sub-topic");
    let peer_1 = network.peer_id(1);

    // Node 0 subscribes first
    network.node_mut(0).behaviour_mut().subscribe(topic);

    // Node 1 subscribes later
    network.node_mut(1).behaviour_mut().subscribe(topic);

    // Node 0 should see node 1's subscription
    let event = network
        .wait_for_event_on(0, |e| {
            matches!(e, Event::Subscribed(p, t) if *p == peer_1 && *t == topic)
        })
        .await;

    assert_eq!(event, Event::Subscribed(peer_1, topic));
}

// ==================== Edge Cases ====================

#[tokio::test]
#[test_log::test]
async fn test_empty_payload_broadcast() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"empty-payload");
    let peer_0 = network.peer_id(0);
    let peer_1 = network.peer_id(1);
    let payload = Bytes::new(); // Empty

    // Node 1 subscribes first
    network.node_mut(1).behaviour_mut().subscribe(topic);

    // Wait for node 0 to see node 1's subscription
    network
        .wait_for_event_on(0, |e| matches!(e, Event::Subscribed(p, _) if *p == peer_1))
        .await;

    // Node 0 subscribes so node 0 knows node 1 is subscribed to topic
    network.node_mut(0).behaviour_mut().subscribe(topic);

    // Wait for node 1 to see node 0's subscription
    network
        .wait_for_event_on(1, |e| matches!(e, Event::Subscribed(p, _) if *p == peer_0))
        .await;

    // Node 0 broadcasts empty payload
    network
        .node_mut(0)
        .behaviour_mut()
        .broadcast(&topic, payload.clone());

    // Node 1 should still receive it
    let event = network
        .wait_for_event_on(1, |e| matches!(e, Event::Received(_, _, _)))
        .await;

    assert_eq!(event, Event::Received(peer_0, topic, payload));
}

#[tokio::test]
#[test_log::test]
async fn test_large_payload_broadcast() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"large-payload");
    let peer_0 = network.peer_id(0);
    let peer_1 = network.peer_id(1);

    // Create a payload of 100KB
    let payload = Bytes::from(vec![0xABu8; 100_000]);

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

    // Node 0 broadcasts large payload
    network
        .node_mut(0)
        .behaviour_mut()
        .broadcast(&topic, payload.clone());

    // Node 1 should receive it
    let event = network
        .wait_for_event_on(1, |e| matches!(e, Event::Received(_, _, _)))
        .await;

    assert_eq!(event, Event::Received(peer_0, topic, payload));
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
        network.node_mut(0).behaviour_mut().broadcast(&topic, msg.clone());
    }

    // Node 1 should receive all of them
    let events = network
        .wait_for_events(|events| {
            let recv_count = events
                .iter()
                .filter(|(idx, e)| {
                    *idx == 1 && matches!(e, Event::Received(p, t, _) if *p == peer_0 && *t == topic)
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
