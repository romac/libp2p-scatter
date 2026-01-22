# Changelog

## Unreleased

- perf: Reuse long-lived bidirectional substreams instead of opening new ones for each message ([`a9d2c8a`](https://github.com/romac/libp2p-scatter/commit/a9d2c8a))
- refactor: Replace `OneShotHandler` with custom `Handler` implementing inbound/outbound state machines ([`a9d2c8a`](https://github.com/romac/libp2p-scatter/commit/a9d2c8a))
- fix: Fix handler poll loop to properly handle state transitions when sending multiple messages ([`2247875`](https://github.com/romac/libp2p-scatter/commit/2247875))
- test: Add `TestNetwork` infrastructure for multi-node integration testing with configurable topologies ([`ce937b5`](https://github.com/romac/libp2p-scatter/commit/ce937b5))
- test: Reorganize tests into separate modules: `connectivity`, `broadcast`, `topology`, `edge_cases`, `network` ([`9636f8e`](https://github.com/romac/libp2p-scatter/commit/9636f8e))
- test: Add topology tests for star, ring, linear, and fully connected networks ([`7eab38b`](https://github.com/romac/libp2p-scatter/commit/7eab38b))

## v0.3.0

- chore: Update libp2p to v0.56.x ([#4](https://github.com/romac/libp2p-scatter/pull/6))

## v0.2.0

- chore: Update libp2p to v0.55.x ([#4](https://github.com/romac/libp2p-scatter/pull/4))

## v0.1.1

- fix: Do not panic on `StreamUpgradeError` in `on_connection_handler_event` ([#2](https://github.com/romac/libp2p-scatter/issues/2))

## v0.1.0

- Initial release
