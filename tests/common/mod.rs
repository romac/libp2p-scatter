#![allow(dead_code)]

//! Common test utilities for integration tests.

use std::time::Duration;

use futures::StreamExt;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{noise, tcp, yamux, Multiaddr, SwarmBuilder};
use libp2p_scatter::{Behaviour, Config, Event};
use tokio::time::timeout;

/// Default timeout for waiting on events.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

/// Creates a new Swarm with the scatter Behaviour using TCP transport.
pub fn create_swarm() -> Swarm<Behaviour> {
    SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .expect("Failed to create TCP transport")
        .with_behaviour(|_| Behaviour::new(Config::default()))
        .expect("Failed to create behaviour")
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build()
}

/// Creates a swarm and starts listening on a random local port.
/// Returns the swarm and its listen address.
pub async fn create_listening_swarm() -> (Swarm<Behaviour>, Multiaddr) {
    let mut swarm = create_swarm();
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .expect("Failed to start listening");

    // Wait for the listen address to be assigned
    loop {
        if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
            return (swarm, address);
        }
    }
}

/// Creates two connected swarms.
/// Returns (swarm_a, swarm_b) where B has dialed A.
pub async fn create_connected_pair() -> (Swarm<Behaviour>, Swarm<Behaviour>) {
    let (mut swarm_a, addr_a) = create_listening_swarm().await;
    let (mut swarm_b, _addr_b) = create_listening_swarm().await;

    // B dials A
    swarm_b.dial(addr_a).expect("Failed to dial");

    // Wait for connection to be established on both sides
    let mut a_connected = false;
    let mut b_connected = false;

    let result = timeout(DEFAULT_TIMEOUT, async {
        loop {
            tokio::select! {
                event = swarm_a.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { .. } = event {
                        a_connected = true;
                    }
                }
                event = swarm_b.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { .. } = event {
                        b_connected = true;
                    }
                }
            }

            if a_connected && b_connected {
                break;
            }
        }
    })
    .await;

    result.expect("Timeout waiting for connection");
    (swarm_a, swarm_b)
}

/// Creates three connected swarms in a star topology.
/// A is the hub, B and C connect to A.
/// Returns (swarm_a, swarm_b, swarm_c).
pub async fn create_connected_trio() -> (Swarm<Behaviour>, Swarm<Behaviour>, Swarm<Behaviour>) {
    let (mut swarm_a, addr_a) = create_listening_swarm().await;
    let (mut swarm_b, _) = create_listening_swarm().await;
    let (mut swarm_c, _) = create_listening_swarm().await;

    // B and C dial A
    swarm_b.dial(addr_a.clone()).expect("Failed to dial");
    swarm_c.dial(addr_a).expect("Failed to dial");

    // Wait for all connections
    let mut connections = 0;
    let target_connections = 4; // A gets 2 inbound, B and C each get 1 outbound

    let result = timeout(DEFAULT_TIMEOUT, async {
        loop {
            tokio::select! {
                event = swarm_a.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { .. } = event {
                        connections += 1;
                    }
                }
                event = swarm_b.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { .. } = event {
                        connections += 1;
                    }
                }
                event = swarm_c.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { .. } = event {
                        connections += 1;
                    }
                }
            }

            if connections >= target_connections {
                break;
            }
        }
    })
    .await;

    result.expect("Timeout waiting for connections");
    (swarm_a, swarm_b, swarm_c)
}

/// Drives two swarms until a specific event is received on the target swarm.
/// Returns the matching event.
/// Note: Both swarms are driven continuously to ensure message flow.
pub async fn wait_for_scatter_event<F>(
    swarm_a: &mut Swarm<Behaviour>,
    swarm_b: &mut Swarm<Behaviour>,
    target: Target,
    mut predicate: F,
) -> Option<Event>
where
    F: FnMut(&Event) -> bool,
{
    let result = timeout(DEFAULT_TIMEOUT, async {
        loop {
            tokio::select! {
                event = swarm_a.select_next_some() => {
                    tracing::debug!(?event, "Swarm A event");

                    // Always process A's events to drive the protocol
                    if target == Target::A {
                        if let SwarmEvent::Behaviour(e) = event {
                            if predicate(&e) {
                                return e;
                            }
                        }
                    }
                }
                event = swarm_b.select_next_some() => {
                    tracing::debug!(?event, "Swarm B event");

                    // Always process B's events to drive the protocol
                    if target == Target::B {
                        if let SwarmEvent::Behaviour(e) = event {
                            if predicate(&e) {
                                return e;
                            }
                        }
                    }
                }
            }
        }
    })
    .await;

    result.ok()
}

/// Which swarm to wait for events on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    A,
    B,
}

/// Drives three swarms until a specific event is received on the target swarm.
pub async fn wait_for_scatter_event_trio<F>(
    swarm_a: &mut Swarm<Behaviour>,
    swarm_b: &mut Swarm<Behaviour>,
    swarm_c: &mut Swarm<Behaviour>,
    target: TargetTrio,
    predicate: F,
) -> Event
where
    F: Fn(&Event) -> bool,
{
    let result = timeout(DEFAULT_TIMEOUT, async {
        loop {
            tokio::select! {
                event = swarm_a.select_next_some() => {
                    if target == TargetTrio::A {
                        if let SwarmEvent::Behaviour(e) = event {
                            if predicate(&e) {
                                return e;
                            }
                        }
                    }
                }
                event = swarm_b.select_next_some() => {
                    if target == TargetTrio::B {
                        if let SwarmEvent::Behaviour(e) = event {
                            if predicate(&e) {
                                return e;
                            }
                        }
                    }
                }
                event = swarm_c.select_next_some() => {
                    if target == TargetTrio::C {
                        if let SwarmEvent::Behaviour(e) = event {
                            if predicate(&e) {
                                return e;
                            }
                        }
                    }
                }
            }
        }
    })
    .await;

    result.expect("Timeout waiting for scatter event")
}

/// Which swarm to wait for events on (trio version).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetTrio {
    A,
    B,
    C,
}

/// Collects scatter events from two swarms until a timeout or max count is reached.
/// Returns events collected from each swarm.
pub async fn collect_events_with_timeout(
    swarm_a: &mut Swarm<Behaviour>,
    swarm_b: &mut Swarm<Behaviour>,
    duration: Duration,
) -> (Vec<Event>, Vec<Event>) {
    let mut events_a = Vec::new();
    let mut events_b = Vec::new();

    let _ = timeout(duration, async {
        loop {
            tokio::select! {
                event = swarm_a.select_next_some() => {
                    if let SwarmEvent::Behaviour(e) = event {
                        events_a.push(e);
                    }
                }
                event = swarm_b.select_next_some() => {
                    if let SwarmEvent::Behaviour(e) = event {
                        events_b.push(e);
                    }
                }
            }
        }
    })
    .await;

    (events_a, events_b)
}
