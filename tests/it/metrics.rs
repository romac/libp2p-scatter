use libp2p_scatter::metrics::Registry;
use libp2p_scatter::{Behaviour, Config};

/// An application must be able to register the metrics of libp2p and libp2p-scatter in the same
/// registry. This test does not compile if the two use different versions of `prometheus-client`.
#[test]
fn test_libp2p_and_scatter_metrics_share_a_registry() {
    let mut registry = Registry::default();

    let _libp2p = libp2p::metrics::Metrics::new(&mut registry);
    let _scatter = Behaviour::new_with_metrics(Config::default(), &mut registry);

    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry)
        .expect("Failed to encode metrics");

    assert!(text.contains("libp2p_"), "unexpected output: {text}");
    assert!(
        text.contains("legacy_connections"),
        "unexpected output: {text}"
    );
}
