# Changelog

## v0.4.0

### Breaking Changes

#### Protocol

* Change protocol name to `/me.romac/scatter/1.0.0`.

#### Configuration

* Rename `Config::max_buf_size` to `Config::max_message_size`.

#### Metrics

* Put metrics behind the `metrics` feature, which is off by default. `Behaviour::new_with_metrics` is only available with this feature.
* Move `Metrics` from the crate root to the `metrics` module.

### Added

#### Compatibility with libp2p-scatter 0.3

* Add support for peers that use libp2p-scatter 0.3 (protocol `/ax/broadcast/1.0.0`). With these peers, each message uses a new substream, and messages can arrive in a different order. The node does not send messages with a topic of more than 63 bytes to these peers.
* Add configuration option `legacy_protocol` to turn on support for peers that use libp2p-scatter 0.3. It is off by default.
* Add metric `legacy_connections`, the number of connections that fell back to the libp2p-scatter 0.3 protocol.

#### Subscriptions

* Emit `Unsubscribed` events when a peer disconnects.
* Improve pubsub subscription management.
* Add `Behaviour::announce` to send a subscription to a connected peer again. A node sends its subscriptions only when the first connection to a peer opens, and `subscribe` does nothing for a topic that the node is already subscribed to.

#### Outbound queue

* Add configuration option for max size of outbound queue: `max_outbound_queue_size`. When the queue is full, the node drops broadcast messages, but not subscription messages. The queue keeps only the latest subscription message for each topic, which can take the queue above `max_outbound_queue_size`.

### Changed

#### Substreams

* Refactor internal message handling by replacing the `OneShotHandler` with a custom `Handler` that implements dedicated inbound and outbound state machines. This provides more robust state management and better control over message flow.
* Significantly improve performance by reusing long-lived bidirectional substreams for message exchange, rather than opening new ones for each message. This reduces connection setup overhead and improves latency.
* Neither inbound substream errors nor the remote closing the substream close the connection, since other protocols may be using it. The handler resets its inbound state and the remote can open a new substream.
* When a send fails, open a new outbound substream for the remaining messages. After 5 failed sends in a row on a connection, the behaviour closes the connection. A failed legacy substream counts as a failed send.

#### Subscriptions

* Send the subscriptions again on a connection when outbound messages on it can be lost: when a send fails on the outbound substream or on a legacy substream. When the behaviour closes a connection after too many failed sends, the new connection sends the subscriptions again.

#### Codec

* Encode each message directly into the write buffer of the substream, without a temporary buffer.

#### Metrics

* Improve metrics description, remove mentions of gossip.

### Performance

* Compare this version with libp2p-scatter 0.3.0, with two nodes on localhost (TCP, noise, yamux), at most 256 messages in flight. Run the benchmark with `cargo bench --bench throughput`:
  * With messages of 64 B to 1 KiB, the throughput is about 15 to 25 times higher: 0.5 to 0.9 million messages per second, instead of about 36,000 for 0.3.0. The 0.3.0 protocol opens a new substream for each message.
  * With messages of 64 KiB, the throughput is about 30% higher: about 970 MiB/s instead of 740 MiB/s. With messages of 1 MiB, both versions reach about 1 GiB/s.
  * The median latency for a 64 B message is about 20 µs, instead of 60 µs for 0.3.0.
  * When both nodes use this version, `legacy_protocol` has no measurable cost.
  * Between this version, with `legacy_protocol` on, and a 0.3.0 node, throughput and latency are the same as between two 0.3.0 nodes, in both directions.

### Fixed

#### Protocol

* Remove redundant and unused implementations of upgrade-related traits, cleaning up the codebase and potentially reducing compiled binary size.

#### Metrics

* Keep `topic_peers_counts` correct. A subscription that the node already has does not increase it, and an unsubscription from a topic that the peer is not subscribed to does not decrease it. The gauge cannot go below zero.
* Fix bug in metrics where `topic_info` was not updated on subscribe.
* Report only the payload bytes, not the encoded message size including header and topic.

### Tests

* Add unit tests for pending queue overflow, substream recovery, and connection keep-alive.
* Add integration tests for connection lifecycle, disconnections, and state cleanup.

## v0.3.0

### Changed

* **chore**: Update `libp2p` dependency to `v0.56.x`.

## v0.2.0

### Changed

* **chore**: Update `libp2p` dependency to `v0.55.x`.

## v0.1.1

### Fixed

* **fix**: Prevent application panic when a `StreamUpgradeError` occurs within the `on_connection_handler_event` callback.

## v0.1.0

### Added

* Initial release of the `scatter` library.

