//! Handler trait for sink stages that consume events
//!
//! Examples: Database writers, file outputs, API clients

use obzenflow_core::{ChainEvent, Result};
use async_trait::async_trait;

/// Handler for sink stages that consume events
/// 
/// Sinks are the endpoints of the pipeline - they:
/// - Write to databases, files, or external systems
/// - Have special flushing semantics for data durability
/// - Consume events without producing outputs
/// 
/// # Example
/// ```rust
/// use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
/// use obzenflow_core::{ChainEvent, Result};
/// use async_trait::async_trait;
/// 
/// struct FileSink {
///     file: std::sync::Mutex<Vec<String>>,
///     buffer: Vec<String>,
/// }
/// 
/// #[async_trait]
/// impl SinkHandler for FileSink {
///     fn consume(&mut self, event: ChainEvent) -> Result<()> {
///         // Extract data from event and buffer it
///         if let Some(data) = event.payload.get("data").and_then(|v| v.as_str()) {
///             self.buffer.push(data.to_string());
///             if self.buffer.len() >= 100 {
///                 self.flush()?;
///             }
///         }
///         Ok(())
///     }
///     
///     fn flush(&mut self) -> Result<()> {
///         // Write buffered data to file
///         if let Ok(mut file) = self.file.lock() {
///             file.extend(self.buffer.drain(..));
///         }
///         Ok(())
///     }
/// }
/// 
/// // Drain is handled by the flush() method
/// ```
#[async_trait]
pub trait SinkHandler: Send + Sync {
    /// Consume an event
    /// 
    /// Returns an error if the event cannot be processed
    fn consume(&mut self, event: ChainEvent) -> Result<()>;
    
    /// Flush any buffered data
    /// 
    /// Called during shutdown to ensure no data loss:
    /// - Database sinks: commit transactions
    /// - File sinks: fsync to disk
    /// - Network sinks: flush buffers
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    
    /// Perform graceful shutdown and cleanup
    /// 
    /// This is called during draining to ensure all data is persisted
    /// and resources are properly released. Default implementation
    /// just calls flush().
    async fn drain(&mut self) -> Result<()> {
        self.flush()
    }
}