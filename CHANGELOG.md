# Changelog

## Unreleased

### Fixed

* **behaviour**: Send the subscriptions again on a connection when outbound messages on it can be lost: when a legacy substream fails, or when a send on the outbound substream fails. Before, the peer did not get a lost subscription until a new connection opened.
* **handler**: Count failed sends on the outbound substream and failed legacy substreams as outbound failures. Only a sent message resets the count, not a new substream. After 5 failures in a row, the behaviour closes the connection, and the new connection sends the subscriptions again. Before, the connection stayed open and the node dropped all messages to it.
* **handler**: Do not drop subscription messages when the outbound queue is full. The queue keeps only the latest subscription message for each topic, which can take the queue above `max_outbound_queue_size`.
* **metrics**: Decrease `topic_peers_counts` only when a peer unsubscribes from a topic that it was subscribed to.
* **handler**: When a send fails, open a new substream for the remaining messages and notify the behaviour immediately. Before, the handler waited for other activity on the connection.

## v0.4.0-rc.5

### Added

* **behaviour**: Add `Behaviour::announce` to send a subscription to a connected peer again. A node sends its subscriptions only when the first connection to a peer opens, and `subscribe` does nothing for a topic that the node is already subscribed to.

### Fixed

* **metrics**: Do not count a peer again in `topic_peers_counts` when it sends a subscription that the node already has.

## v0.4.0-rc.4

### Changed

* **codec**: Encode each message directly into the write buffer. Each payload is now copied once instead of twice, and the space for the message is reserved once.

### Fixed

* **handler**: Flush the outbound substream after the queue is empty. Before, the last messages of a burst could stay in the write buffer until the node sent another message.

## v0.4.0-rc.3

### Added

* **protocol**: Add support for peers that use libp2p-scatter 0.3 (protocol `/ax/broadcast/1.0.0`). With these peers, each message uses a new substream, and messages can arrive in a different order. The node does not send messages with a topic of more than 63 bytes to these peers.
* **config**: Add configuration option `legacy_protocol` to turn on support for peers that use libp2p-scatter 0.3. It is off by default.
* **metrics**: Add counter `legacy_connections`, the number of connections that fell back to the libp2p-scatter 0.3 protocol.

## v0.4.0-rc.2

### Breaking Changes

* **handler**: Split `HandlerEvent::Error(io::Error)` into `InboundError(io::Error)` and `InboundClosed` to distinguish between transient I/O errors and the remote peer closing the inbound substream.

### Changed

* **behaviour**: Neither inbound substream errors nor the remote closing the substream close the connection, since other protocols may be using it. The handler resets its inbound state and the remote can open a new substream.

## v0.4.0-rc.1

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

