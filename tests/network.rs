//! Tests for the TestNetwork infrastructure itself.

mod common;

use crate::common::TestNetwork;

#[tokio::test]
async fn test_network_star_topology() {
    let network = TestNetwork::star(4).await;

    // Node 0 should be connected to all others
    let map = network.connection_map();
    assert_eq!(map[&0].len(), 3);
    for i in 1..4 {
        assert_eq!(map[&i].len(), 1);
        assert!(map[&i].contains(&0));
    }
}

#[tokio::test]
async fn test_network_linear_topology() {
    let network = TestNetwork::linear(4).await;

    let map = network.connection_map();
    // 0 connects to 1, 1 connects to 0 and 2, etc.
    assert_eq!(map[&0].len(), 1);
    assert!(map[&0].contains(&1));
    assert_eq!(map[&1].len(), 2);
    assert_eq!(map[&2].len(), 2);
    assert_eq!(map[&3].len(), 1);
    assert!(map[&3].contains(&2));
}

#[tokio::test]
async fn test_network_ring_topology() {
    let network = TestNetwork::ring(4).await;

    let map = network.connection_map();
    // Each node should have exactly 2 connections
    for i in 0..4 {
        assert_eq!(
            map[&i].len(),
            2,
            "Node {} has {} connections",
            i,
            map[&i].len()
        );
    }
}

#[tokio::test]
async fn test_network_fully_connected() {
    let network = TestNetwork::fully_connected(4).await;

    let map = network.connection_map();
    // Each node should be connected to all others
    for i in 0..4 {
        assert_eq!(
            map[&i].len(),
            3,
            "Node {} has {} connections",
            i,
            map[&i].len()
        );
    }
}
