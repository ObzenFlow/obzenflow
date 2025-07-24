//! Handler trait for stateless transform stages
//!
//! Examples: Data enrichers, filters, mappers, routers

use obzenflow_core::{ChainEvent, Result};
use obzenflow_core::event::ChainEventContent;
use async_trait::async_trait;

/// Handler for stateless transform stages
/// 
/// Transforms are the workhorses of the pipeline - they:
/// - Start processing immediately (no waiting)
/// - Process events one at a time
/// - Can filter (0 outputs), pass through (1 output), or expand (N outputs)
/// 
/// # Example
/// ```rust
/// use obzenflow_runtime_services::stages::common::handlers::TransformHandler;
/// use obzenflow_core::{ChainEvent, Result};
/// use obzenflow_core::event::ChainEventContent;
/// use std::collections::HashMap;
/// use serde_json::{json, Value};
/// use async_trait::async_trait;
/// 
/// struct DataEnricher {
///     cache: HashMap<String, Value>,
/// }
/// 
/// #[async_trait]
/// impl TransformHandler for DataEnricher {
///     fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
///         // Enrich event with cached metadata
///         if let Some(metadata) = self.cache.get(&event.event_type()) {
///             if let ChainEventContent::Data { ref mut payload, .. } = event.content {
///                 payload["metadata"] = metadata.clone();
///             }
///         }
///         vec![event]
///     }
///     
///     // Stateless transform has no special drain logic
///     async fn drain(&mut self) -> Result<()> { 
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait TransformHandler: Send + Sync {
    /// Process an event, potentially producing multiple outputs
    /// 
    /// This is a pure function - same input always produces same output
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent>;
    
    /// Perform any cleanup during shutdown
    /// 
    /// For stateless transforms, this is typically a no-op.
    /// For stateful transforms, this might flush caches or close connections.
    async fn drain(&mut self) -> Result<()>;
}