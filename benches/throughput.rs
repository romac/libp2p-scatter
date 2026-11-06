//! Throughput and latency of broadcasts between two nodes on localhost,
//! for this version and libp2p-scatter 0.3, with and without the legacy protocol.
//!
//! Run all scenarios with `cargo bench --bench throughput`, or only the scenarios
//! whose name contains a filter with `cargo bench --bench throughput -- <filter>`.

use std::future::Future;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use libp2p_v056 as libp2p_v03;
use tokio::sync::mpsc;

const TOPIC: &[u8] = b"bench";
const WINDOW: usize = 256;
const RUNS: usize = 5;

/// The events of a swarm that the benchmark uses, for any version of libp2p.
enum NodeEvent {
    Listening(String),
    Received,
    Subscribed,
    Closed(String),
    Other,
}

trait Impl: Clone + Send + 'static {
    type B;
    type Swarm: Send + 'static;
    fn behaviour(&self) -> Self::B;
    fn subscribe(b: &mut Self::B);
    fn broadcast(b: &mut Self::B, payload: Bytes);
    fn swarm(&self) -> Self::Swarm;
    fn behaviour_mut(swarm: &mut Self::Swarm) -> &mut Self::B;
    fn listen(swarm: &mut Self::Swarm);
    fn dial(swarm: &mut Self::Swarm, addr: &str);
    fn next_event(swarm: &mut Self::Swarm) -> impl Future<Output = NodeEvent> + Send;
}

/// Implements the items of `Impl` that have the same code for each version of libp2p.
macro_rules! swarm_items {
    ($libp2p:ident, $scatter:ident) => {
        type B = $scatter::Behaviour;
        type Swarm = $libp2p::Swarm<Self::B>;

        fn swarm(&self) -> Self::Swarm {
            let behaviour = self.behaviour();
            $libp2p::SwarmBuilder::with_existing_identity(
                $libp2p::identity::Keypair::generate_ed25519(),
            )
            .with_tokio()
            .with_tcp(
                $libp2p::tcp::Config::default().nodelay(true),
                $libp2p::noise::Config::new,
                $libp2p::yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| behaviour)
            .unwrap()
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(600)))
            .build()
        }

        fn behaviour_mut(swarm: &mut Self::Swarm) -> &mut Self::B {
            swarm.behaviour_mut()
        }

        fn listen(swarm: &mut Self::Swarm) {
            swarm
                .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
                .unwrap();
        }

        fn dial(swarm: &mut Self::Swarm, addr: &str) {
            swarm
                .dial(addr.parse::<$libp2p::Multiaddr>().unwrap())
                .unwrap();
        }

        async fn next_event(swarm: &mut Self::Swarm) -> NodeEvent {
            use $libp2p::swarm::SwarmEvent;
            match swarm.select_next_some().await {
                SwarmEvent::NewListenAddr { address, .. } => {
                    NodeEvent::Listening(address.to_string())
                }
                SwarmEvent::Behaviour($scatter::Event::Received(..)) => NodeEvent::Received,
                SwarmEvent::Behaviour($scatter::Event::Subscribed(..)) => NodeEvent::Subscribed,
                SwarmEvent::ConnectionClosed { cause, .. } => {
                    NodeEvent::Closed(format!("{cause:?}"))
                }
                _ => NodeEvent::Other,
            }
        }
    };
}

#[derive(Clone)]
struct Latest {
    legacy: bool,
}

impl Impl for Latest {
    swarm_items!(libp2p, libp2p_scatter);

    fn behaviour(&self) -> Self::B {
        libp2p_scatter::Behaviour::new(
            libp2p_scatter::Config::default().legacy_protocol(self.legacy),
        )
    }
    fn subscribe(b: &mut Self::B) {
        b.subscribe(libp2p_scatter::Topic::new(TOPIC));
    }
    fn broadcast(b: &mut Self::B, payload: Bytes) {
        b.broadcast(libp2p_scatter::Topic::new(TOPIC), payload)
            .unwrap();
    }
}

#[derive(Clone)]
struct V03;

impl Impl for V03 {
    swarm_items!(libp2p_v03, libp2p_scatter_v03);

    fn behaviour(&self) -> Self::B {
        libp2p_scatter_v03::Behaviour::new(libp2p_scatter_v03::Config::default())
    }
    fn subscribe(b: &mut Self::B) {
        b.subscribe(libp2p_scatter_v03::Topic::new(TOPIC));
    }
    fn broadcast(b: &mut Self::B, payload: Bytes) {
        b.broadcast(&libp2p_scatter_v03::Topic::new(TOPIC), payload);
    }
}

/// A connected sender/receiver pair. The receiver runs in its own task and
/// reports each received message on `rx`.
struct Pair<S: Impl> {
    sender: S::Swarm,
    rx: mpsc::UnboundedReceiver<Instant>,
}

impl<S: Impl> Pair<S> {
    async fn new<R: Impl>(s: S, r: R) -> Self {
        let mut receiver = r.swarm();
        R::listen(&mut receiver);
        let addr = loop {
            if let NodeEvent::Listening(address) = R::next_event(&mut receiver).await {
                break address;
            }
        };
        R::subscribe(R::behaviour_mut(&mut receiver));

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                if matches!(R::next_event(&mut receiver).await, NodeEvent::Received)
                    && tx.send(Instant::now()).is_err()
                {
                    return;
                }
            }
        });

        let mut sender = s.swarm();
        S::subscribe(S::behaviour_mut(&mut sender));
        S::dial(&mut sender, &addr);
        loop {
            match S::next_event(&mut sender).await {
                NodeEvent::Subscribed => break,
                NodeEvent::Closed(cause) => panic!("closed: {cause}"),
                _ => {}
            }
        }
        // Let the connection settle (protocol negotiation, legacy fallback, ...).
        let settle = tokio::time::sleep(Duration::from_millis(200));
        tokio::pin!(settle);
        loop {
            tokio::select! {
                _ = &mut settle => break,
                _ = S::next_event(&mut sender) => {}
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
                S::broadcast(S::behaviour_mut(&mut self.sender), payload.clone());
                send_times.push_back(Instant::now());
                sent += 1;
            }
            tokio::select! {
                ev = S::next_event(&mut self.sender) => {
                    if let NodeEvent::Closed(cause) = ev {
                        panic!("closed: {cause}");
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
