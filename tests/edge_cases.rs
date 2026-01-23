//! Tests for edge cases and boundary conditions.

mod common;

use bytes::Bytes;
use libp2p_scatter::{Event, Topic};

use crate::common::TestNetwork;

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
        .broadcast(topic, payload.clone());

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
        .broadcast(topic, payload.clone());

    // Node 1 should receive it
    let event = network
        .wait_for_event_on(1, |e| matches!(e, Event::Received(_, _, _)))
        .await;

    assert_eq!(event, Event::Received(peer_0, topic, payload));
}
