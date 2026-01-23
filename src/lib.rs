mod behaviour;
mod codec;
mod handler;
mod protocol;
mod util;

pub use behaviour::{Behaviour, Event};
pub use codec::Codec;
pub use protocol::Topic;

#[cfg(feature = "metrics")]
pub mod metrics;

/// Configuration options for the scatter protocol.
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum allowed size for messages, in bytes.
    pub max_message_size: usize,
}

impl Config {
    /// Sets the maximum allowed size for messages, in bytes.
    pub fn max_message_size(mut self, max_message_size: usize) -> Self {
        self.max_message_size = max_message_size;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024 * 4, // 4 MiB
        }
    }
}
