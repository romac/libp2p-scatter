# Changelog

## Unreleased

### Breaking Changes

* **feat!**: Change protocol name to `/me.romac/scatter/1.0.0`.
* **chore!**: Rename `Config::max_buf_size` to `Config::max_message_size`.

### Changed

* **perf**: Significantly improve performance by reusing long-lived bidirectional substreams for message exchange, rather than opening new ones for each message. This reduces connection setup overhead and improves latency.
* **refactor**: Refactor internal message handling by replacing the `OneShotHandler` with a custom `Handler` that implements dedicated inbound and outbound state machines. This provides more robust state management and better control over message flow.

### Fixed

* **fix**: Remove redundant and unused implementations of upgrade-related traits, cleaning up the codebase and potentially reducing compiled binary size.

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

