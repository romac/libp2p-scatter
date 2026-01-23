//! Tests for connection lifecycle, disconnections, and state cleanup.

mod common;

use libp2p_scatter::{Event, Topic};

use crate::common::TestNetwork;

#[tokio::test]
#[test_log::test]
async fn test_disconnect_clears_peer_subscriptions() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"test-topic");
    let peer_1 = network.peer_id(1);

    // Node 1 subscribes
    network.node_mut(1).behaviour_mut().subscribe(topic);

    // Wait for node 0 to see node 1's subscription
    network
        .wait_for_event_on(
            0,
            |e| matches!(e, Event::Subscribed(p, t) if *p == peer_1 && *t == topic),
        )
        .await;

    // Verify node 0 tracks node 1 as subscribed to the topic
    {
        let peers: Vec<_> = network.node(0).behaviour().peers(topic).collect();
        assert!(peers.contains(&peer_1), "Node 0 should track node 1 as subscribed");
    }

    // Disconnect node 1 from the network
    network.disconnect_node(1).await;

    // Drive the network to process disconnect events
    network.drive().await;

    // Verify node 0 no longer tracks node 1 as subscribed
    {
        let peers: Vec<_> = network.node(0).behaviour().peers(topic).collect();
        assert!(
            !peers.contains(&peer_1),
            "Node 0 should no longer track node 1 after disconnect"
        );
    }

    // Verify connections are closed
    assert!(
        !network.is_connected(0, 1),
        "Node 0 should not be connected to node 1"
    );
    assert!(
        !network.is_connected(1, 0),
        "Node 1 should not be connected to node 0"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_disconnect_clears_multiple_topic_subscriptions() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic1 = Topic::new(b"topic-one");
    let topic2 = Topic::new(b"topic-two");
    let topic3 = Topic::new(b"topic-three");
    let peer_1 = network.peer_id(1);

    // Node 1 subscribes to multiple topics
    network.node_mut(1).behaviour_mut().subscribe(topic1);
    network.node_mut(1).behaviour_mut().subscribe(topic2);
    network.node_mut(1).behaviour_mut().subscribe(topic3);

    // Wait for node 0 to see all subscriptions
    network
        .wait_for_events(|events| {
            let sub_count = events
                .iter()
                .filter(|(idx, e)| {
                    *idx == 0 && matches!(e, Event::Subscribed(p, _) if *p == peer_1)
                })
                .count();
            sub_count >= 3
        })
        .await;

    // Verify node 0 tracks node 1 on all topics
    {
        let behaviour = network.node(0).behaviour();
        assert!(behaviour.peers(topic1).any(|p| p == peer_1));
        assert!(behaviour.peers(topic2).any(|p| p == peer_1));
        assert!(behaviour.peers(topic3).any(|p| p == peer_1));
    }

    // Disconnect node 1
    network.disconnect_node(1).await;
    network.drive().await;

    // Verify node 0 no longer tracks node 1 on any topic
    {
        let behaviour = network.node(0).behaviour();
        assert!(
            !behaviour.peers(topic1).any(|p| p == peer_1),
            "Node 1 should be removed from topic1"
        );
        assert!(
            !behaviour.peers(topic2).any(|p| p == peer_1),
            "Node 1 should be removed from topic2"
        );
        assert!(
            !behaviour.peers(topic3).any(|p| p == peer_1),
            "Node 1 should be removed from topic3"
        );
    }
}

#[tokio::test]
#[test_log::test]
async fn test_reconnect_after_disconnect() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"reconnect-topic");
    let peer_1 = network.peer_id(1);

    // Node 1 subscribes
    network.node_mut(1).behaviour_mut().subscribe(topic);

    // Wait for node 0 to see the subscription
    network
        .wait_for_event_on(
            0,
            |e| matches!(e, Event::Subscribed(p, _) if *p == peer_1),
        )
        .await;

    // Disconnect
    network.disconnect_node(1).await;
    network.drive().await;

    // Verify disconnected
    assert!(!network.is_connected(0, 1));
    assert!(network.node(0).behaviour().peers(topic).next().is_none());

    // Reconnect
    network.connect_nodes(0, 1).await;

    // Node 1 is still subscribed locally, so it should re-announce on reconnect
    // Wait for node 0 to see the subscription again
    network
        .wait_for_event_on(
            0,
            |e| matches!(e, Event::Subscribed(p, t) if *p == peer_1 && *t == topic),
        )
        .await;

    // Verify node 0 tracks node 1 again
    let peers: Vec<_> = network.node(0).behaviour().peers(topic).collect();
    assert!(
        peers.contains(&peer_1),
        "Node 0 should track node 1 after reconnect"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_disconnect_one_of_many_peers() {
    let mut network = TestNetwork::star(4).await; // Node 0 is hub, connected to 1, 2, 3

    let topic = Topic::new(b"multi-peer-topic");
    let peer_1 = network.peer_id(1);
    let peer_2 = network.peer_id(2);
    let peer_3 = network.peer_id(3);

    // All spoke nodes subscribe
    network.node_mut(1).behaviour_mut().subscribe(topic);
    network.node_mut(2).behaviour_mut().subscribe(topic);
    network.node_mut(3).behaviour_mut().subscribe(topic);

    // Wait for hub (node 0) to see all subscriptions
    network
        .wait_for_events(|events| {
            let sub_count = events
                .iter()
                .filter(|(idx, e)| *idx == 0 && matches!(e, Event::Subscribed(_, t) if *t == topic))
                .count();
            sub_count >= 3
        })
        .await;

    // Verify hub tracks all peers
    {
        let peers: Vec<_> = network.node(0).behaviour().peers(topic).collect();
        assert_eq!(peers.len(), 3);
        assert!(peers.contains(&peer_1));
        assert!(peers.contains(&peer_2));
        assert!(peers.contains(&peer_3));
    }

    // Disconnect only node 2
    network.disconnect_node(2).await;
    network.drive().await;

    // Verify hub still tracks nodes 1 and 3, but not node 2
    {
        let peers: Vec<_> = network.node(0).behaviour().peers(topic).collect();
        assert_eq!(peers.len(), 2, "Should have 2 peers after disconnecting one");
        assert!(peers.contains(&peer_1), "Should still track peer 1");
        assert!(!peers.contains(&peer_2), "Should not track disconnected peer 2");
        assert!(peers.contains(&peer_3), "Should still track peer 3");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_local_subscriptions_persist_through_disconnect() {
    let mut network = TestNetwork::fully_connected(2).await;

    let topic = Topic::new(b"persist-topic");

    // Node 0 subscribes
    network.node_mut(0).behaviour_mut().subscribe(topic);

    // Verify local subscription
    assert!(
        network.node(0).behaviour().subscribed().any(|t| t == topic),
        "Node 0 should be subscribed locally"
    );

    // Wait for node 1 to see the subscription
    network
        .wait_for_event_on(1, |e| matches!(e, Event::Subscribed(_, _)))
        .await;

    // Disconnect
    network.disconnect_node(0).await;
    network.drive().await;

    // Verify local subscription persists after disconnect
    assert!(
        network.node(0).behaviour().subscribed().any(|t| t == topic),
        "Local subscription should persist after disconnect"
    );
}
