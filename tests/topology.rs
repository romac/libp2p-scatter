//! Tests for multi-node network topologies and routing.

mod common;

use std::time::Duration;

use bytes::Bytes;
use libp2p_scatter::{Event, Topic};

use crate::common::TestNetwork;

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
    let node2_received_topic1 = events
        .iter()
        .any(|(idx, e)| *idx == 2 && matches!(e, Event::Received(_, t, _) if *t == topic1));
    assert!(
        !node2_received_topic1,
        "Node 2 should not have received message on topic1"
    );
}
