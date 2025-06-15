//! Constants for EventStore subscription system

use std::time::Duration;

/// Maximum number of pending notifications in a subscription channel
pub const SUBSCRIPTION_CHANNEL_SIZE: usize = 10000;

/// Maximum events to return in a single batch
pub const MAX_BATCH_SIZE: usize = 100;

/// How often to clean up dead subscriptions
pub const SUBSCRIPTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum time to wait for a complete batch before returning partial
pub const BATCH_TIMEOUT: Duration = Duration::from_millis(10);

/// Sleep duration for sources without subscriptions
pub const SOURCE_IDLE_SLEEP: Duration = Duration::from_millis(10);

/// Maximum subscriptions per stage (prevent resource exhaustion)
pub const MAX_SUBSCRIPTIONS_PER_STAGE: usize = 1000;

/// Initial buffer capacity for pending events
pub const PENDING_BUFFER_CAPACITY: usize = 16;