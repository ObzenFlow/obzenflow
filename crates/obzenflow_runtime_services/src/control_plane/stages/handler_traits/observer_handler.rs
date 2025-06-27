//! Handler trait for observation stages that monitor without modifying
//!
//! Examples: Loggers, metrics collectors, debuggers

use obzenflow_core::{ChainEvent, Result};

/// Handler for observation stages that monitor events without transformation
/// 
/// Observers are used for:
/// - Logging and debugging
/// - Metrics collection
/// - Monitoring and alerting
/// 
/// They see events but don't modify the data flow
/// 
/// # Example
/// ```rust
/// struct EventLogger {
///     log_level: LogLevel,
/// }
/// 
/// impl ObserverHandler for EventLogger {
///     fn observe(&self, event: &ChainEvent) -> Result<()> {
///         match self.log_level {
///             LogLevel::Debug => debug!("Event: {:?}", event),
///             LogLevel::Info => info!("Event type: {}", event.event_type),
///             _ => {}
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait ObserverHandler: Send + Sync {
    /// Observe an event without modifying it
    /// 
    /// The event is passed by reference since observers don't consume it
    fn observe(&self, event: &ChainEvent) -> Result<()>;
    
    /// Check if the observer is healthy
    fn is_healthy(&self) -> bool {
        true
    }
}