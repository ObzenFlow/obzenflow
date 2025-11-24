//! Unified subscription polling trait for FSM-controlled event consumption
//!
//! This trait provides a consistent polling interface for all subscription types,
//! ensuring that FSMs control sleep timing and preventing busy loops.

use obzenflow_core::event::JournalEvent;
use std::fmt::Debug;

/// Result of polling a subscription for events
#[derive(Debug)]
pub enum PollResult<T: JournalEvent> {
    /// An event is available
    Event(obzenflow_core::EventEnvelope<T>),

    /// No events currently available (would block)
    NoEvents,

    /// Error occurred while polling
    Error(Box<dyn std::error::Error + Send + Sync>),
}

/// Trait for non-blocking subscription polling
///
/// This trait unifies the polling interface for different subscription types,
/// ensuring consistent behavior across the system:
/// - Non-blocking polling (returns immediately)
/// - Clear semantics for each result variant
/// - FSM controls all sleep/retry timing
#[async_trait::async_trait]
pub trait SubscriptionPoller: Send + Sync {
    /// The type of events this subscription produces
    type Event: JournalEvent;

    /// Poll for the next event without blocking
    ///
    /// Returns immediately with one of:
    /// - `Event`: An event is ready
    /// - `NoEvents`: No events available right now
    /// - `Error`: An error occurred
    async fn poll_next(&mut self) -> PollResult<Self::Event>;

    /// Get the name/identifier of this subscription for logging
    fn name(&self) -> &str;
}
