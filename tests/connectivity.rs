//! Tests for basic connectivity and subscription/unsubscription mechanics.

mod common;

use libp2p_scatter::{Event, Topic};

use crate::common::TestNetwork;

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
        .wait_for_event_on(
            1,
            |e| matches!(e, Event::Subscribed(p, t) if *p == peer_0 && *t == topic),
        )
        .await;

    // Node 0 unsubscribes
    network.node_mut(0).behaviour_mut().unsubscribe(&topic);

    // Node 1 should receive the unsubscription event
    let event = network
        .wait_for_event_on(1, |e| matches!(e, Event::Unsubscribed(_, t) if *t == topic))
        .await;

    assert_eq!(event, Event::Unsubscribed(peer_0, topic));
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
        .wait_for_event_on(
            0,
            |e| matches!(e, Event::Subscribed(p, t) if *p == peer_1 && *t == topic),
        )
        .await;

    assert_eq!(event, Event::Subscribed(peer_1, topic));
}
