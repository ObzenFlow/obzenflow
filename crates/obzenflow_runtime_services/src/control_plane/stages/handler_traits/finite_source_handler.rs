//! Handler trait for sources that eventually complete
//!
//! Examples: File readers, bounded collections, API paginators

use obzenflow_core::ChainEvent;

/// Handler for sources that eventually complete (files, bounded collections)
/// 
/// Finite sources:
/// - Pull events using `next()` 
/// - Eventually complete when data is exhausted
/// - Automatically send EOF when `is_complete()` returns true
/// 
/// # Example
/// ```rust
/// use obzenflow_runtime_services::control_plane::stages::handler_traits::FiniteSourceHandler;
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
///             Some(ChainEvent::new(
///                 EventId::new(),
///                 self.writer_id.clone(),
///                 "list_item",
///                 json!({ "value": item, "index": self.index - 1 })
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
    /// Get the next event from this source
    /// 
    /// Returns:
    /// - `Some(event)` - Next event available
    /// - `None` - No event available right now (but source isn't complete)
    /// 
    /// The framework will automatically send EOF when `is_complete()` returns true
    fn next(&mut self) -> Option<ChainEvent>;
    
    /// Check if this source has naturally completed its work
    /// 
    /// Returns true when the source has no more events to produce
    /// and has reached a natural completion state (not due to shutdown).
    fn is_complete(&self) -> bool;
}