//! Flow observer implementation using EventHandler trait
//! 
//! This replaces FlowEventSubscriber with a cleaner design that separates
//! the observation logic (EventHandler) from the subscription management (GenericEventProcessor).

use crate::chain_event::ChainEvent;
use crate::lifecycle::{EventHandler, ProcessingMode};
use crate::step::Result;

/// FlowObserver is an EventHandler that observes events without modifying them.
/// 
/// This is the core logic component that observes flow events. It should be:
/// 1. Wrapped with MiddlewareEventHandler to add cross-cutting concerns
/// 2. Wrapped with GenericEventProcessor to handle subscription and lifecycle
/// 
/// ## Example
/// 
/// ```rust
/// // Create the observer
/// let observer = FlowObserver::new("my_flow");
/// 
/// // Add middleware
/// let with_middleware = observer
///     .middleware()
///     .with(MonitoringMiddleware::new(GoldenSignals))
///     .with(LoggingMiddleware::new())
///     .build();
/// 
/// // Wrap in processor for subscription management
/// let processor = GenericEventProcessor::new(
///     "flow_observer",
///     with_middleware,
///     event_store,
///     filter,
///     ComponentType::FlowObserver,
/// );
/// ```
#[derive(Debug, Clone)]
pub struct FlowObserver {
    flow_name: String,
}

impl FlowObserver {
    /// Create a new flow observer for the given flow
    pub fn new(flow_name: impl Into<String>) -> Self {
        Self {
            flow_name: flow_name.into(),
        }
    }
    
    /// Get the flow name this observer is monitoring
    pub fn flow_name(&self) -> &str {
        &self.flow_name
    }
}

impl EventHandler for FlowObserver {
    /// Observe events without modification
    fn observe(&self, event: &ChainEvent) -> Result<()> {
        // Core observation logic - just trace the event
        // The actual monitoring/logging is done by middleware
        tracing::trace!(
            flow = %self.flow_name,
            event_id = %event.ulid,
            event_type = %event.event_type,
            "Flow observer processing event"
        );
        Ok(())
    }
    
    /// FlowObserver uses the observe processing mode
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Observe
    }
    
}