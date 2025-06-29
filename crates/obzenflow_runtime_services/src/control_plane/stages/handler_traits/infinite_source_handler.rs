//! Handler trait for sources that run indefinitely
//!
//! Examples: Kafka consumers, network streams, file watchers

use obzenflow_core::ChainEvent;

/// Handler for sources that run indefinitely (network streams, queues)
/// 
/// Infinite sources:
/// - Never naturally complete
/// - Only stop on explicit shutdown
/// 
/// # Example
/// ```rust
/// use obzenflow_runtime_services::control_plane::stages::handler_traits::InfiniteSourceHandler;
/// use obzenflow_core::{ChainEvent, EventId, WriterId};
/// use std::time::Duration;
/// use serde_json::json;
/// 
/// struct NetworkSource {
///     reader: std::sync::Mutex<Vec<String>>,
///     writer_id: WriterId,
/// }
/// 
/// impl InfiniteSourceHandler for NetworkSource {
///     fn next(&mut self) -> Option<ChainEvent> {
///         // Simulate polling from a network source
///         if let Ok(mut messages) = self.reader.lock() {
///             if let Some(msg) = messages.pop() {
///                 return Some(ChainEvent::new(
///                     EventId::new(),
///                     self.writer_id.clone(),
///                     "network_message",
///                     json!({ "data": msg })
///                 ));
///             }
///         }
///         None // No message available right now
///     }
/// }
/// ```
pub trait InfiniteSourceHandler: Send + Sync {
    /// Get the next event from this source
    /// 
    /// Returns:
    /// - `Some(event)` - Next event available  
    /// - `None` - No event available right now
    /// 
    /// Only sends EOF on explicit shutdown
    fn next(&mut self) -> Option<ChainEvent>;
}