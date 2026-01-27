mod behaviour;
mod codec;
mod handler;
mod protocol;
mod util;

pub use behaviour::{Behaviour, Event};
pub use codec::Codec;
pub use protocol::Topic;

use libp2p::swarm::StreamProtocol;

#[cfg(feature = "metrics")]
pub mod metrics;

/// Default protocol name for the scatter protocol.
const DEFAULT_PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/me.romac/scatter/1.0.0");

/// Configuration options for the scatter protocol.
#[derive(Clone, Debug)]
pub struct Config {
    /// The protocol name used for substream negotiation.
    pub protocol_name: StreamProtocol,

    /// Maximum allowed size for messages, in bytes.
    pub max_message_size: usize,

    /// Maximum number of pending messages in the outbound queue.
    pub max_outbound_queue_size: usize,
}

impl Config {
    /// Sets the protocol name used for substream negotiation.
    pub fn protocol_name(mut self, protocol_name: StreamProtocol) -> Self {
        self.protocol_name = protocol_name;
        self
    }

    /// Sets the maximum allowed size for messages, in bytes.
    pub fn max_message_size(mut self, max_message_size: usize) -> Self {
        self.max_message_size = max_message_size;
        self
    }

    /// Sets the maximum number of pending messages in the outbound queue.
    pub fn max_outbound_queue_size(mut self, max_outbound_queue_size: usize) -> Self {
        self.max_outbound_queue_size = max_outbound_queue_size;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            protocol_name: DEFAULT_PROTOCOL_NAME,
            max_message_size: 1024 * 1024 * 4, // 4 MiB
            max_outbound_queue_size: 1024,     // 1024 messages
        }
    }
}
