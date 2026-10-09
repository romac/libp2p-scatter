//! Throughput and latency of broadcasts between two nodes on localhost,
//! for this version and libp2p-scatter 0.3, with and without the legacy protocol.
//!
//! Run all scenarios with `cargo bench --bench throughput`, or only the scenarios
//! whose name contains a filter with `cargo bench --bench throughput -- <filter>`.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use libp2p::identity::Keypair;
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{Multiaddr, SwarmBuilder, noise, tcp, yamux};
use tokio::sync::mpsc;

const TOPIC: &[u8] = b"bench";
const WINDOW: usize = 256;
const RUNS: usize = 5;

trait Impl: Clone + Send + 'static {
    type B: NetworkBehaviour<ToSwarm: Send + std::fmt::Debug> + Send;
    fn behaviour(&self) -> Self::B;
    fn subscribe(b: &mut Self::B);
    fn broadcast(b: &mut Self::B, payload: Bytes);
    fn is_received(e: &<Self::B as NetworkBehaviour>::ToSwarm) -> bool;
    fn is_subscribed(e: &<Self::B as NetworkBehaviour>::ToSwarm) -> bool;
}

#[derive(Clone)]
struct Latest {
    legacy: bool,
}

impl Impl for Latest {
    type B = libp2p_scatter::Behaviour;
    fn behaviour(&self) -> Self::B {
        libp2p_scatter::Behaviour::new(
            libp2p_scatter::Config::default().legacy_protocol(self.legacy),
        )
    }
    fn subscribe(b: &mut Self::B) {
        b.subscribe(libp2p_scatter::Topic::new(TOPIC));
    }
    fn broadcast(b: &mut Self::B, payload: Bytes) {
        b.broadcast(libp2p_scatter::Topic::new(TOPIC), payload);
    }
    fn is_received(e: &libp2p_scatter::Event) -> bool {
        matches!(e, libp2p_scatter::Event::Received(..))
    }
    fn is_subscribed(e: &libp2p_scatter::Event) -> bool {
        matches!(e, libp2p_scatter::Event::Subscribed(..))
    }
}

#[derive(Clone)]
struct V03;

impl Impl for V03 {
    type B = libp2p_scatter_v03::Behaviour;
    fn behaviour(&self) -> Self::B {
        libp2p_scatter_v03::Behaviour::new(libp2p_scatter_v03::Config::default())
    }
    fn subscribe(b: &mut Self::B) {
        b.subscribe(libp2p_scatter_v03::Topic::new(TOPIC));
    }
    fn broadcast(b: &mut Self::B, payload: Bytes) {
        b.broadcast(&libp2p_scatter_v03::Topic::new(TOPIC), payload);
    }
    fn is_received(e: &libp2p_scatter_v03::Event) -> bool {
        matches!(e, libp2p_scatter_v03::Event::Received(..))
    }
    fn is_subscribed(e: &libp2p_scatter_v03::Event) -> bool {
        matches!(e, libp2p_scatter_v03::Event::Subscribed(..))
    }
}

fn build_swarm<B: NetworkBehaviour>(behaviour: B) -> Swarm<B> {
    SwarmBuilder::with_existing_identity(Keypair::generate_ed25519())
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )
        .unwrap()
        .with_behaviour(|_| behaviour)
        .unwrap()
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(600)))
        .build()
}

/// A connected sender/receiver pair. The receiver runs in its own task and
/// reports each received message on `rx`.
struct Pair<S: Impl> {
    sender: Swarm<S::B>,
    rx: mpsc::UnboundedReceiver<Instant>,
}

impl<S: Impl> Pair<S> {
    async fn new<R: Impl>(s: S, r: R) -> Self {
        let mut receiver = build_swarm(r.behaviour());
        receiver
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr: Multiaddr = loop {
            if let SwarmEvent::NewListenAddr { address, .. } = receiver.select_next_some().await {
                break address;
            }
        };
        R::subscribe(receiver.behaviour_mut());

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                if let SwarmEvent::Behaviour(e) = receiver.select_next_some().await
                    && R::is_received(&e)
                    && tx.send(Instant::now()).is_err()
                {
                    return;
                }
            }
        });

        let mut sender = build_swarm(s.behaviour());
        S::subscribe(sender.behaviour_mut());
        sender.dial(addr).unwrap();
        loop {
            match sender.select_next_some().await {
                SwarmEvent::Behaviour(e) if S::is_subscribed(&e) => break,
                SwarmEvent::ConnectionClosed { cause, .. } => panic!("closed: {cause:?}"),
                _ => {}
            }
        }
        // Let the connection settle (protocol negotiation, legacy fallback, ...).
        let settle = tokio::time::sleep(Duration::from_millis(200));
        tokio::pin!(settle);
        loop {
            tokio::select! {
                _ = &mut settle => break,
                _ = sender.select_next_some() => {}
            }
        }
        Self { sender, rx }
    }

    /// Sends `n` messages with at most `window` in flight; returns elapsed time
    /// and per-message latencies (only meaningful when `window == 1`).
    async fn run(&mut self, payload: &Bytes, n: usize, window: usize) -> (Duration, Vec<Duration>) {
        let mut sent = 0;
        let mut received = 0;
        let mut send_times = std::collections::VecDeque::new();
        let mut latencies = Vec::with_capacity(n);
        let start = Instant::now();
        let deadline = tokio::time::sleep(Duration::from_secs(120));
        tokio::pin!(deadline);
        while received < n {
            while sent < n && sent - received < window {
                S::broadcast(self.sender.behaviour_mut(), payload.clone());
                send_times.push_back(Instant::now());
                sent += 1;
            }
            tokio::select! {
                ev = self.sender.select_next_some() => {
                    if let SwarmEvent::ConnectionClosed { cause, .. } = ev {
                        panic!("closed: {cause:?}");
                    }
                }
                Some(at) = self.rx.recv() => {
                    received += 1;
                    let t0 = send_times.pop_front().unwrap();
                    latencies.push(at - t0);
                }
                _ = &mut deadline => panic!("timeout: received {received}/{n}"),
            }
        }
        (start.elapsed(), latencies)
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn pct(v: &[Duration], p: f64) -> f64 {
    let mut v: Vec<f64> = v.iter().map(|d| d.as_secs_f64() * 1e6).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p) as usize]
}

async fn scenario<S: Impl, R: Impl>(name: &str, s: S, r: R) {
    let mut pair = Pair::new(s, r).await;

    let sizes: &[(usize, usize)] = &[
        (64, 20_000),
        (1024, 20_000),
        (64 * 1024, 2_000),
        (1024 * 1024, 200),
    ];
    for &(size, n) in sizes {
        let payload = Bytes::from(vec![0xAB; size]);
        pair.run(&payload, n / 10, WINDOW).await; // warmup
        let mut rates = Vec::new();
        for _ in 0..RUNS {
            let (elapsed, _) = pair.run(&payload, n, WINDOW).await;
            rates.push(n as f64 / elapsed.as_secs_f64());
        }
        let rate = median(rates);
        println!(
            "{name:<28} throughput size={size:>8} B  {rate:>10.0} msg/s  {:>9.1} MiB/s",
            rate * size as f64 / (1024.0 * 1024.0)
        );
    }

    let payload = Bytes::from(vec![0xAB; 64]);
    pair.run(&payload, 200, 1).await; // warmup
    let (_, lat) = pair.run(&payload, 2_000, 1).await;
    println!(
        "{name:<28} latency    size=      64 B  p50={:>6.0} us  p99={:>6.0} us",
        pct(&lat, 0.5),
        pct(&lat, 0.99)
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    // `cargo bench` passes `--bench`, which is not a filter.
    let filter = std::env::args()
        .skip(1)
        .find(|arg| !arg.starts_with("--"))
        .unwrap_or_default();
    macro_rules! run {
        ($name:expr, $s:expr, $r:expr) => {
            if $name.contains(&filter) {
                scenario($name, $s, $r).await;
            }
        };
    }
    run!("v0.3 -> v0.3", V03, V03);
    run!(
        "latest -> latest",
        Latest { legacy: false },
        Latest { legacy: false }
    );
    run!(
        "latest+legacy -> latest+legacy",
        Latest { legacy: true },
        Latest { legacy: true }
    );
    run!("latest+legacy -> v0.3", Latest { legacy: true }, V03);
    run!("v0.3 -> latest+legacy", V03, Latest { legacy: true });
}
