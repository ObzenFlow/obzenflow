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
/// struct KafkaSource {
///     consumer: KafkaConsumer,
/// }
/// 
/// impl InfiniteSourceHandler for KafkaSource {
///     fn next(&mut self) -> Option<ChainEvent> {
///         match self.consumer.poll(Duration::from_millis(100)) {
///             Some(msg) => Some(ChainEvent::new(...)),
///             None => None, // No message available right now
///         }
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