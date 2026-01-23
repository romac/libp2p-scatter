use core::fmt;

use fnv::FnvHashMap as HashMap;

use prometheus_client::encoding::{EncodeLabelSet, LabelSetEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;

pub use prometheus_client::registry::Registry;

use crate::Topic;

pub struct Metrics {
    /// Information needed to decide if a topic is allowed or not.
    topic_info: HashMap<Topic, EverSubscribed>,
    /// Status of our subscription to this topic. This metric allows analyzing other topic metrics
    /// filtered by our current subscription status.
    topic_subscription_status: Family<Topic, Gauge>,
    /// Number of peers subscribed to each topic. This allows us to analyze a topic's behaviour
    /// regardless of our subscription status.
    topic_peers_count: Family<Topic, Gauge>,

    /// Number of messages sent to each topic.
    topic_msg_sent_counts: Family<Topic, Counter>,
    /// Amount of data sent to each topic, in bytes.
    topic_msg_sent_bytes: Family<Topic, Counter>,
    /// Number of messages published to each topic.
    topic_msg_published: Family<Topic, Counter>,

    /// Number of messages received on each topic
    topic_msg_recv_counts: Family<Topic, Counter>,
    /// Amount of data received for each topic, in bytes.
    topic_msg_recv_bytes: Family<Topic, Counter>,
}

type EverSubscribed = bool;

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        macro_rules! register_family {
            ($name:expr, $help:expr) => {{
                let fam = Family::default();
                registry.register($name, $help, fam.clone());
                fam
            }};
        }

        let topic_subscription_status = register_family!(
            "topic_subscription_status",
            "Subscription status per known topic"
        );

        let topic_peers_count = register_family!(
            "topic_peers_counts",
            "Number of peers subscribed to each topic"
        );

        let topic_msg_sent_counts = register_family!(
            "topic_msg_sent_counts",
            "Number of messages sent to each topic"
        );
        let topic_msg_published = register_family!(
            "topic_msg_published",
            "Number of messages published to each topic"
        );
        let topic_msg_sent_bytes = register_family!(
            "topic_msg_sent_bytes",
            "Amount of data sent to each topic, in bytes"
        );

        let topic_msg_recv_counts = register_family!(
            "topic_msg_recv_counts",
            "Number of messages received on each topic"
        );
        let topic_msg_recv_bytes = register_family!(
            "topic_msg_recv_bytes",
            "Amount of data received for each topic, in bytes"
        );

        Self {
            topic_info: HashMap::default(),
            topic_subscription_status,
            topic_peers_count,
            topic_msg_sent_counts,
            topic_msg_published,
            topic_msg_sent_bytes,
            topic_msg_recv_counts,
            topic_msg_recv_bytes,
        }
    }

    /// Registers a topic if not already known
    fn register_topic(&mut self, topic: &Topic) {
        if !self.topic_info.contains_key(topic) {
            self.topic_info.entry(*topic).or_insert(false);
            self.topic_subscription_status.get_or_create(topic).set(0);
        }
    }

    pub(crate) fn subscribe(&mut self, topic: &Topic) {
        self.register_topic(topic);
        *self.topic_info.entry(*topic).or_default() = true;
        self.topic_subscription_status.get_or_create(topic).set(1);
    }

    pub(crate) fn unsubscribe(&mut self, topic: &Topic) {
        self.register_topic(topic);
        self.topic_subscription_status.get_or_create(topic).set(0);
    }

    /// Increase the number of peers that are subscribed to this topic.
    pub(crate) fn inc_topic_peers(&mut self, topic: &Topic) {
        self.register_topic(topic);
        self.topic_peers_count.get_or_create(topic).inc();
    }

    /// Decrease the number of peers that are subscribed to this topic.
    pub(crate) fn dec_topic_peers(&mut self, topic: &Topic) {
        self.register_topic(topic);

        let gauge = self.topic_peers_count.get_or_create(topic);
        if gauge.get() > 0 {
            gauge.dec();
        }
    }

    pub(crate) fn register_published_message(&mut self, topic: &Topic) {
        self.register_topic(topic);
        self.topic_msg_published.get_or_create(topic).inc();
    }

    /// Register sending a message over a topic.
    pub(crate) fn msg_sent(&mut self, topic: &Topic, bytes: usize) {
        self.register_topic(topic);
        self.topic_msg_sent_counts.get_or_create(topic).inc();
        self.topic_msg_sent_bytes
            .get_or_create(topic)
            .inc_by(bytes as u64);
    }

    /// Register that a message was received .
    pub(crate) fn msg_received(&mut self, topic: &Topic, bytes: usize) {
        self.register_topic(topic);
        self.topic_msg_recv_counts.get_or_create(topic).inc();
        self.topic_msg_recv_bytes
            .get_or_create(topic)
            .inc_by(bytes as u64);
    }
}

impl EncodeLabelSet for Topic {
    fn encode(&self, mut encoder: LabelSetEncoder) -> fmt::Result {
        use prometheus_client::encoding::{EncodeLabelKey, EncodeLabelValue};

        let mut label_encoder = encoder.encode_label();
        let mut key_encoder = label_encoder.encode_label_key()?;
        EncodeLabelKey::encode(&"topic", &mut key_encoder)?;
        let mut value_encoder = key_encoder.encode_label_value()?;
        let value = String::from_utf8_lossy(self.as_ref());
        EncodeLabelValue::encode(&value, &mut value_encoder)?;
        value_encoder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metrics() -> Metrics {
        let mut registry = Registry::default();
        Metrics::new(&mut registry)
    }

    // ==================== Subscription Status Tests ====================

    #[test]
    fn test_subscribe_sets_status() {
        let mut metrics = make_metrics();
        let topic = Topic::new(b"test-topic");

        metrics.subscribe(&topic);

        let status = metrics.topic_subscription_status.get_or_create(&topic);
        assert_eq!(status.get(), 1);
    }

    #[test]
    fn test_unsubscribe_clears_status() {
        let mut metrics = make_metrics();
        let topic = Topic::new(b"test-topic");

        metrics.subscribe(&topic);
        metrics.unsubscribe(&topic);

        let status = metrics.topic_subscription_status.get_or_create(&topic);
        assert_eq!(status.get(), 0);
    }

    #[test]
    fn test_subscribe_unsubscribe_cycle() {
        let mut metrics = make_metrics();
        let topic = Topic::new(b"test-topic");

        // Subscribe
        metrics.subscribe(&topic);
        assert_eq!(
            metrics
                .topic_subscription_status
                .get_or_create(&topic)
                .get(),
            1
        );

        // Unsubscribe
        metrics.unsubscribe(&topic);
        assert_eq!(
            metrics
                .topic_subscription_status
                .get_or_create(&topic)
                .get(),
            0
        );

        // Re-subscribe
        metrics.subscribe(&topic);
        assert_eq!(
            metrics
                .topic_subscription_status
                .get_or_create(&topic)
                .get(),
            1
        );
    }

    // ==================== Peer Count Tests ====================

    #[test]
    fn test_inc_topic_peers() {
        let mut metrics = make_metrics();
        let topic = Topic::new(b"test-topic");

        metrics.inc_topic_peers(&topic);
        metrics.inc_topic_peers(&topic);
        metrics.inc_topic_peers(&topic);

        let count = metrics.topic_peers_count.get_or_create(&topic);
        assert_eq!(count.get(), 3);
    }

    #[test]
    fn test_dec_topic_peers() {
        let mut metrics = make_metrics();
        let topic = Topic::new(b"test-topic");

        metrics.inc_topic_peers(&topic);
        metrics.inc_topic_peers(&topic);
        metrics.dec_topic_peers(&topic);

        let count = metrics.topic_peers_count.get_or_create(&topic);
        assert_eq!(count.get(), 1);
    }

    // ==================== Message Sent Tests ====================

    #[test]
    fn test_msg_sent_counts() {
        let mut metrics = make_metrics();
        let topic = Topic::new(b"test-topic");

        metrics.msg_sent(&topic, 100);
        metrics.msg_sent(&topic, 200);

        let count = metrics.topic_msg_sent_counts.get_or_create(&topic);
        assert_eq!(count.get(), 2);

        let bytes = metrics.topic_msg_sent_bytes.get_or_create(&topic);
        assert_eq!(bytes.get(), 300);
    }

    #[test]
    fn test_register_published_message() {
        let mut metrics = make_metrics();
        let topic = Topic::new(b"test-topic");

        metrics.register_published_message(&topic);
        metrics.register_published_message(&topic);

        let count = metrics.topic_msg_published.get_or_create(&topic);
        assert_eq!(count.get(), 2);
    }

    // ==================== Message Received Tests ====================

    #[test]
    fn test_msg_received_counts() {
        let mut metrics = make_metrics();
        let topic = Topic::new(b"test-topic");

        metrics.msg_received(&topic, 50);
        metrics.msg_received(&topic, 150);
        metrics.msg_received(&topic, 100);

        let count = metrics.topic_msg_recv_counts.get_or_create(&topic);
        assert_eq!(count.get(), 3);

        let bytes = metrics.topic_msg_recv_bytes.get_or_create(&topic);
        assert_eq!(bytes.get(), 300);
    }

    // ==================== Multiple Topics Tests ====================

    #[test]
    fn test_multiple_topics_independent() {
        let mut metrics = make_metrics();
        let topic1 = Topic::new(b"topic1");
        let topic2 = Topic::new(b"topic2");

        metrics.subscribe(&topic1);
        metrics.msg_sent(&topic1, 100);
        metrics.inc_topic_peers(&topic1);

        metrics.subscribe(&topic2);
        metrics.msg_sent(&topic2, 200);
        metrics.inc_topic_peers(&topic2);
        metrics.inc_topic_peers(&topic2);

        // Verify topic1
        assert_eq!(
            metrics.topic_msg_sent_bytes.get_or_create(&topic1).get(),
            100
        );
        assert_eq!(metrics.topic_peers_count.get_or_create(&topic1).get(), 1);

        // Verify topic2
        assert_eq!(
            metrics.topic_msg_sent_bytes.get_or_create(&topic2).get(),
            200
        );
        assert_eq!(metrics.topic_peers_count.get_or_create(&topic2).get(), 2);
    }

    // ==================== Topic Registration Tests ====================

    #[test]
    fn test_register_topic_idempotent() {
        let mut metrics = make_metrics();
        let topic = Topic::new(b"test-topic");

        // Multiple operations should not cause issues
        metrics.subscribe(&topic);
        metrics.subscribe(&topic);
        metrics.unsubscribe(&topic);
        metrics.subscribe(&topic);

        // Should still work correctly
        assert_eq!(
            metrics
                .topic_subscription_status
                .get_or_create(&topic)
                .get(),
            1
        );
    }
}
