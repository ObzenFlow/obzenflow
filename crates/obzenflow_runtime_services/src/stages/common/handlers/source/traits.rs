//! Source handler traits for both finite and infinite sources

use obzenflow_core::ChainEvent;

/// Handler for sources that eventually complete (files, bounded collections)
///
/// Finite sources:
/// - Pull events using `next()`
/// - Eventually complete when data is exhausted
/// - Automatically send EOF when `is_complete()` returns true
///
/// # Example
/// ```ignore
/// use obzenflow_runtime_services::stages::common::handlers::FiniteSourceHandler;
/// use obzenflow_core::{ChainEvent, EventId, WriterId};
/// use serde_json::json;
///
/// struct ListSource {
///     items: Vec<String>,
///     index: usize,
///     writer_id: WriterId,
/// }
///
/// impl FiniteSourceHandler for ListSource {
///     fn next(&mut self) -> Option<ChainEvent> {
///         if self.index < self.items.len() {
///             let item = self.items[self.index].clone();
///             self.index += 1;
///             Some(ChainEvent::data(
///                 EventId::new(),
///                 self.writer_id,
///                 "data",
///                 json!({"item": item}),
///             ))
///         } else {
///             None
///         }
///     }
///     
///     fn is_complete(&self) -> bool {
///         self.index >= self.items.len()
///     }
/// }
/// ```
pub trait FiniteSourceHandler: Send + Sync {
    /// Pull the next event from the source
    ///
    /// Returns None when temporarily out of events.
    /// Check `is_complete()` to know if source is exhausted.
    fn next(&mut self) -> Option<ChainEvent>;

    /// Check if this source has completed
    ///
    /// When true, supervisor will send EOF and terminate
    fn is_complete(&self) -> bool;
}

/// Handler for sources that run indefinitely
///
/// Infinite sources:
/// - Never complete naturally
/// - Only stop on shutdown signal or error
/// - Examples: Kafka consumers, WebSocket streams, system monitors
///
/// # Example
/// ```ignore
/// use obzenflow_runtime_services::stages::common::handlers::InfiniteSourceHandler;
/// use obzenflow_core::{ChainEvent, EventId, WriterId};
/// use serde_json::json;
/// use std::time::{Duration, Instant};
///
/// struct HeartbeatSource {
///     writer_id: WriterId,
///     last_beat: Instant,
///     interval: Duration,
/// }
///
/// impl InfiniteSourceHandler for HeartbeatSource {
///     fn next(&mut self) -> Option<ChainEvent> {
///         let now = Instant::now();
///         if now.duration_since(self.last_beat) >= self.interval {
///             self.last_beat = now;
///             Some(ChainEvent::data(
///                 EventId::new(),
///                 self.writer_id,
///                 "heartbeat",
///                 json!({"heartbeat": true, "timestamp": chrono::Utc::now().to_rfc3339()}),
///             ))
///         } else {
///             None
///         }
///     }
/// }
/// ```
pub trait InfiniteSourceHandler: Send + Sync {
    /// Pull the next event from the source
    ///
    /// Returns None when temporarily out of events.
    /// Infinite sources never complete - they run until shutdown.
    fn next(&mut self) -> Option<ChainEvent>;
}
