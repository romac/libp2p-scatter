# Changelog

## Unreleased

### Breaking Changes

* **config**: Rename `Config::max_buf_size` to `Config::max_message_size`.
* **protocol**: Change protocol name to `/me.romac/scatter/1.0.0`.

### Added

* **config**: Add configuration option for max size of outbound queue: `max_outbound_queue_size`.
* **protocol**: Emit `Unsubscribed` events when a peer disconnects.
* **protocol**: Improve pubsub subscription management.

### Changed

* **protocol**: Refactor internal message handling by replacing the `OneShotHandler` with a custom `Handler` that implements dedicated inbound and outbound state machines. This provides more robust state management and better control over message flow.
* **protocol**: Significantly improve performance by reusing long-lived bidirectional substreams for message exchange, rather than opening new ones for each message. This reduces connection setup overhead and improves latency.
* **metrics**: Improve metrics description, remove mentions of gossip.

### Fixed

* **protocol**: Remove redundant and unused implementations of upgrade-related traits, cleaning up the codebase and potentially reducing compiled binary size.
* **metrics**: Fix underflow in `topic_peers_count` metric.
* **metrics**: Fix bug in metrics where `topic_info` was not updated on subscribe.
* **metrics**: Report only the payload bytes, not the encoded message size including header and topic.

### Tests

* **unit**: Add unit tests for pending queue overflow, substream recovery, and connection keep-alive.
* **integration**: Add integration tests for connection lifecycle, disconnections, and state cleanup.

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

