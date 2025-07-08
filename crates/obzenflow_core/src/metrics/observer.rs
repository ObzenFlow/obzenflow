//! Metrics observer trait for event observation
//!
//! This trait allows the runtime layer to notify observers about events
//! without knowing their implementation details, maintaining clean
//! architectural boundaries.

use crate::event::event_envelope::EventEnvelope;

/// Trait for observing events for metrics collection
/// 
/// This trait follows the Observer pattern and allows the runtime
/// to notify observers without knowing their implementation.
/// Implementations should be thread-safe as they may be called
/// from multiple threads concurrently.
pub trait MetricsObserver: Send + Sync {
    /// Called when a data event is written to the journal
    fn on_event(&self, envelope: &EventEnvelope);
    
    /// Called when a control event is written to the journal
    fn on_control_event(&self, envelope: &EventEnvelope);
    
    /// Called periodically to allow time-based aggregations
    fn on_tick(&self);
}

/// A no-op implementation for when metrics are disabled
/// 
/// This implementation does nothing and incurs minimal overhead,
/// allowing the system to run without metrics collection.
#[derive(Debug, Default)]
pub struct NoOpMetricsObserver;

impl NoOpMetricsObserver {
    /// Create a new no-op metrics observer
    pub fn new() -> Self {
        Self
    }
}

impl MetricsObserver for NoOpMetricsObserver {
    fn on_event(&self, _envelope: &EventEnvelope) {
        // No-op: metrics disabled
    }
    
    fn on_control_event(&self, _envelope: &EventEnvelope) {
        // No-op: metrics disabled
    }
    
    fn on_tick(&self) {
        // No-op: metrics disabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChainEvent, EventId, WriterId};
    use std::sync::Arc;
    
    #[test]
    fn test_noop_observer_does_nothing() {
        let observer = NoOpMetricsObserver::new();
        
        // Create the event with its own writer_id (who created the event)
        let event_writer = WriterId::new();
        let event = ChainEvent::new(
            EventId::new(),
            event_writer,
            "test.event",
            serde_json::json!({}),
        );
        
        // Create the envelope with potentially different writer_id (who wrote to journal)
        let journal_writer = WriterId::new();
        let envelope = EventEnvelope::new(journal_writer, event);
        
        // These should complete without panicking
        observer.on_event(&envelope);
        observer.on_control_event(&envelope);
        observer.on_tick();
    }
    
    #[test]
    fn test_observer_trait_is_object_safe() {
        // Verify the trait can be used as a trait object
        let observer: Arc<dyn MetricsObserver> = Arc::new(NoOpMetricsObserver::new());
        
        let event_writer = WriterId::new();
        let event = ChainEvent::new(
            EventId::new(),
            event_writer,
            "test.event",
            serde_json::json!({}),
        );
        
        let journal_writer = WriterId::new();
        let envelope = EventEnvelope::new(journal_writer, event);
        
        observer.on_event(&envelope);
        observer.on_control_event(&envelope);
        observer.on_tick();
    }
}