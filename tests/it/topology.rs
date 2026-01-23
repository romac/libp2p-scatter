//! Tests for multi-node network topologies and routing.

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
        .broadcast(topic, payload.clone());

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
        .broadcast(topic1, payload.clone());

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

/// Test broadcast across a large network with 12 nodes in a star topology.
///
/// Node 0 is the hub, nodes 1-11 are spokes. All spokes subscribe to a topic,
/// then the hub broadcasts a message that should reach all subscribers.
#[tokio::test]
#[test_log::test]
async fn test_large_network_star_broadcast() {
    const NUM_NODES: usize = 12;

    let mut network = TestNetwork::star(NUM_NODES).await;

    let topic = Topic::new(b"large-network-topic");
    let hub = network.peer_id(0);
    let payload = Bytes::from_static(b"broadcast to many");

    // All spoke nodes (1..NUM_NODES) subscribe to the topic
    for i in 1..NUM_NODES {
        network.node_mut(i).behaviour_mut().subscribe(topic);
    }

    // Wait for hub (node 0) to see all subscriptions
    let spoke_peers: Vec<_> = (1..NUM_NODES).map(|i| network.peer_id(i)).collect();

    network
        .wait_for_events_timeout(Duration::from_secs(5), |events| {
            let sub_count = events
                .iter()
                .filter(|(idx, e)| {
                    *idx == 0
                        && matches!(e, Event::Subscribed(p, t) if *t == topic && spoke_peers.contains(p))
                })
                .count();
            sub_count >= NUM_NODES - 1
        })
        .await;

    // Hub broadcasts
    network
        .node_mut(0)
        .behaviour_mut()
        .broadcast(topic, payload.clone());

    // All spoke nodes should receive the broadcast
    let events = network
        .wait_for_events_timeout(Duration::from_secs(5), |events| {
            let recv_count = events
                .iter()
                .filter(|(idx, e)| {
                    *idx != 0
                        && matches!(e, Event::Received(p, t, msg) if *p == hub && *t == topic && *msg == payload)
                })
                .count();
            recv_count >= NUM_NODES - 1
        })
        .await;

    // Verify each spoke received the message
    for i in 1..NUM_NODES {
        let received = events
            .iter()
            .any(|(idx, e)| *idx == i && matches!(e, Event::Received(_, _, _)));
        assert!(received, "Node {} should have received the broadcast", i);
    }
}

/// Test broadcast across a large ring network with 12 nodes.
///
/// In a ring topology, messages don't automatically propagate - only directly
/// connected peers receive broadcasts. This tests that the protocol correctly
/// delivers to immediate neighbors only.
#[tokio::test]
#[test_log::test]
async fn test_large_network_ring_topology() {
    const NUM_NODES: usize = 12;

    let mut network = TestNetwork::ring(NUM_NODES).await;

    let topic = Topic::new(b"ring-topic");
    let sender = network.peer_id(0);
    let payload = Bytes::from_static(b"ring message");

    // Nodes 1 and 11 are neighbors of node 0 in a ring
    // All nodes subscribe so we can test routing
    for i in 0..NUM_NODES {
        network.node_mut(i).behaviour_mut().subscribe(topic);
    }

    // Wait for node 0 to see subscriptions from its neighbors (1 and NUM_NODES-1)
    let neighbor1 = network.peer_id(1);
    let neighbor2 = network.peer_id(NUM_NODES - 1);

    network
        .wait_for_events_timeout(Duration::from_secs(5), |events| {
            let has_neighbor1 = events.iter().any(|(idx, e)| {
                *idx == 0 && matches!(e, Event::Subscribed(p, t) if *p == neighbor1 && *t == topic)
            });
            let has_neighbor2 = events.iter().any(|(idx, e)| {
                *idx == 0 && matches!(e, Event::Subscribed(p, t) if *p == neighbor2 && *t == topic)
            });
            has_neighbor1 && has_neighbor2
        })
        .await;

    // Node 0 broadcasts - only its direct neighbors should receive it
    network
        .node_mut(0)
        .behaviour_mut()
        .broadcast(topic, payload.clone());

    // Wait for neighbors to receive
    let events = network
        .wait_for_events_timeout(Duration::from_secs(2), |events| {
            let neighbor1_recv = events.iter().any(|(idx, e)| {
                *idx == 1
                    && matches!(e, Event::Received(p, t, msg) if *p == sender && *t == topic && *msg == payload)
            });
            let neighbor2_recv = events.iter().any(|(idx, e)| {
                *idx == NUM_NODES - 1
                    && matches!(e, Event::Received(p, t, msg) if *p == sender && *t == topic && *msg == payload)
            });
            neighbor1_recv && neighbor2_recv
        })
        .await;

    // Verify neighbors received
    let neighbor1_received = events
        .iter()
        .any(|(idx, e)| *idx == 1 && matches!(e, Event::Received(_, _, _)));
    let neighbor2_received = events
        .iter()
        .any(|(idx, e)| *idx == NUM_NODES - 1 && matches!(e, Event::Received(_, _, _)));

    assert!(neighbor1_received, "Neighbor 1 should have received");
    assert!(
        neighbor2_received,
        "Neighbor {} should have received",
        NUM_NODES - 1
    );

    // Collect remaining events and verify non-neighbors didn't receive from node 0
    let additional_events = network.collect_events(Duration::from_millis(200)).await;

    // Nodes 2 through NUM_NODES-2 should NOT have received directly from node 0
    for i in 2..NUM_NODES - 1 {
        let received_from_sender = events
            .iter()
            .chain(additional_events.iter())
            .any(|(idx, e)| *idx == i && matches!(e, Event::Received(p, _, _) if *p == sender));
        assert!(
            !received_from_sender,
            "Node {} should NOT have received directly from node 0",
            i
        );
    }
}

/// Test broadcast in a large linear network (chain topology).
///
/// In a linear chain (0-1-2-3-...-9), node 0 broadcasts a message.
/// Only node 1 (the direct neighbor) should receive it. Nodes 2-9 should
/// NOT receive anything because this protocol does not relay messages.
#[tokio::test]
#[test_log::test]
async fn test_large_linear_chain_broadcast() {
    const NUM_NODES: usize = 10;

    let mut network = TestNetwork::linear(NUM_NODES).await;

    let topic = Topic::new(b"chain-topic");
    let sender = network.peer_id(0);
    let payload = Bytes::from_static(b"chain message");

    // All nodes subscribe
    for i in 0..NUM_NODES {
        network.node_mut(i).behaviour_mut().subscribe(topic);
    }

    // Wait for node 0 to see subscription from its only neighbor (node 1)
    let neighbor = network.peer_id(1);
    network
        .wait_for_events_timeout(Duration::from_secs(5), |events| {
            events.iter().any(|(idx, e)| {
                *idx == 0 && matches!(e, Event::Subscribed(p, t) if *p == neighbor && *t == topic)
            })
        })
        .await;

    // Node 0 broadcasts - only node 1 should receive it directly
    network
        .node_mut(0)
        .behaviour_mut()
        .broadcast(topic, payload.clone());

    // Wait for node 1 to receive
    let event = network
        .wait_for_event_on(1, |e| {
            matches!(e, Event::Received(p, t, msg) if *p == sender && *t == topic && *msg == payload)
        })
        .await;

    assert_eq!(event, Event::Received(sender, topic, payload.clone()));

    // Verify nodes further down the chain did NOT receive directly from node 0
    let additional_events = network.collect_events(Duration::from_millis(200)).await;

    for i in 2..NUM_NODES {
        let received_from_sender = additional_events
            .iter()
            .any(|(idx, e)| *idx == i && matches!(e, Event::Received(p, _, _) if *p == sender));
        assert!(
            !received_from_sender,
            "Node {} should NOT have received directly from node 0 (not connected)",
            i
        );
    }
}
