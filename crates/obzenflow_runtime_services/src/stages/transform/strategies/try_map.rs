//! TryMap helper for trait-based fallible transformations
//!
//! The TryMap helper is a fallible variant of Map that enables type-safe transformations
//! using Rust's `TryFrom` and `Into` traits. This is ideal for domain types that implement
//! standard conversion traits.
//!
//! # Composable Error Handling
//!
//! TryMap supports multiple error handling strategies through a builder pattern:
//!
//! ```ignore
//! use obzenflow_runtime_services::stages::transform::strategies::TryMap;
//!
//! // Route errors to error journal (default)
//! let try_mapper = TryMap::<Order>::new()
//!     .on_error_journal();
//!
//! // Emit errors as custom event type (for cycle-based error handling)
//! let try_mapper = TryMap::<Order>::new()
//!     .on_error_emit("Order.invalid");
//!
//! // Drop failed conversions silently
//! let try_mapper = TryMap::<Order>::new()
//!     .on_error_drop();
//! ```

use crate::stages::common::handlers::TransformHandler;
use async_trait::async_trait;
use obzenflow_core::event::{status::processing_status::ProcessingStatus, ChainEventFactory};
use obzenflow_core::ChainEvent;
use serde_json::json;
use std::marker::PhantomData;
use std::sync::Arc;

// Re-use ErrorStrategy from try_map_with
use super::try_map_with::ErrorStrategy;

/// TryMap helper for trait-based fallible transformations
///
/// This helper enables type-safe conversions between ChainEvent and domain types
/// using Rust's `TryFrom` and `Into` traits. Conversion failures are handled
/// according to the configured ErrorStrategy.
///
/// # Type Parameters
///
/// * `T` - The target type that implements `TryFrom<ChainEvent>` and `Into<ChainEvent>`
///
/// # Error Handling Strategies
///
/// By default, errors are routed to the error journal (FLOWIP-082e). This can
/// be changed using builder methods:
///
/// - `.on_error_journal()` - Route to error journal (default)
/// - `.on_error_emit(event_type)` - Emit as custom event type
/// - `.on_error_drop()` - Silently drop failures
/// - `.on_error_with(handler)` - Custom error handling
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::transform::strategies::TryMap;
/// use obzenflow_core::ChainEvent;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct Order {
///     id: String,
///     amount: f64,
/// }
///
/// impl TryFrom<ChainEvent> for Order {
///     type Error = String;
///
///     fn try_from(event: ChainEvent) -> Result<Self, Self::Error> {
///         serde_json::from_value(event.payload().clone())
///             .map_err(|e| format!("Failed to parse order: {}", e))
///     }
/// }
///
/// impl Into<ChainEvent> for Order {
///     fn into(self) -> ChainEvent {
///         // Convert back to ChainEvent
///         ChainEvent::data(/* ... */)
///     }
/// }
///
/// // Errors to journal (default)
/// let try_mapper = TryMap::<Order>::new();
///
/// // Emit invalid conversions for cycle-based handling
/// let validator = TryMap::<Order>::new()
///     .on_error_emit("Order.invalid");
/// ```
#[derive(Clone)]
pub struct TryMap<T> {
    _phantom: PhantomData<T>,
    error_strategy: ErrorStrategy,
}

impl<T> TryMap<T> {
    /// Create a new TryMap helper for type T
    ///
    /// By default, errors are routed to the error journal (FLOWIP-082e).
    /// Use builder methods to configure different error handling strategies.
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime_services::stages::transform::strategies::TryMap;
    ///
    /// let try_mapper = TryMap::<Order>::new()
    ///     .on_error_journal();  // Explicit (this is the default)
    /// ```
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
            error_strategy: ErrorStrategy::ToErrorJournal, // Default
        }
    }

    /// Route failed conversions to the error journal (default behavior)
    ///
    /// Events that fail conversion are marked with ProcessingStatus::Error
    /// and routed to the error journal by the supervisor (FLOWIP-082e).
    ///
    /// # Use Case
    /// - Terminal errors that should be logged and monitored
    /// - Unrecoverable parsing/deserialization failures
    /// - Errors that should not retry
    pub fn on_error_journal(mut self) -> Self {
        self.error_strategy = ErrorStrategy::ToErrorJournal;
        self
    }

    /// Emit failed conversions as a custom event type
    ///
    /// Creates a derived event with the specified event_type, preserving
    /// the original payload with an added "validation_error" field containing
    /// the error message.
    ///
    /// # Use Case
    /// - Cycle-based error handling (validator → fixer → validator)
    /// - Invalid events that can be repaired and retried
    /// - Separating valid/invalid streams for different processing
    ///
    /// # Arguments
    /// * `event_type` - The event type for failed conversions (e.g., "Order.invalid")
    pub fn on_error_emit(mut self, event_type: impl Into<String>) -> Self {
        self.error_strategy = ErrorStrategy::ToEventType(event_type.into());
        self
    }

    /// Emit failed conversions with structured error payload
    ///
    /// Like `on_error_emit`, but allows customizing how error info is added to the payload.
    /// The closure receives `(original_event, error_message)` and returns a `serde_json::Value`
    /// that will be merged into the payload.
    ///
    /// # Use Case
    /// - Downstream stages expect specific error field structures
    /// - Need to parse/transform error messages before adding to payload
    ///
    /// # Arguments
    /// * `event_type` - The event type for failed conversions
    /// * `payload_fn` - Function `(event, error) -> serde_json::Value` to create error payload
    pub fn on_error_emit_with<P>(mut self, event_type: impl Into<String>, payload_fn: P) -> Self
    where
        P: Fn(&ChainEvent, &str) -> serde_json::Value + Send + Sync + 'static,
    {
        self.error_strategy =
            ErrorStrategy::ToEventTypeWith(event_type.into(), Arc::new(payload_fn));
        self
    }

    /// Drop failed conversions silently
    ///
    /// Events that fail conversion produce no output. Use this when failures
    /// are expected and should simply be filtered out of the pipeline.
    ///
    /// # Use Case
    /// - Expected parsing failures that should be ignored
    /// - Filtering patterns where invalid events are discarded
    /// - Performance-sensitive pipelines where error logging is unnecessary
    pub fn on_error_drop(mut self) -> Self {
        self.error_strategy = ErrorStrategy::Drop;
        self
    }

    /// Use a custom error handler for failed conversions
    ///
    /// Provides full control over error handling. The handler receives
    /// the original event and error message, returning Option<ChainEvent>
    /// (Some to emit, None to drop).
    ///
    /// # Arguments
    /// * `handler` - Function (ChainEvent, String) -> Option<ChainEvent>
    pub fn on_error_with<H>(mut self, handler: H) -> Self
    where
        H: Fn(ChainEvent, String) -> Option<ChainEvent> + Send + Sync + 'static,
    {
        self.error_strategy = ErrorStrategy::Custom(Arc::new(handler));
        self
    }
}

impl<T> Default for TryMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> std::fmt::Debug for TryMap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TryMap")
            .field("target_type", &std::any::type_name::<T>())
            .field("error_strategy", &self.error_strategy)
            .finish()
    }
}

#[async_trait]
impl<T> TransformHandler for TryMap<T>
where
    T: TryFrom<ChainEvent> + Into<ChainEvent> + Send + Sync + 'static,
    <T as TryFrom<ChainEvent>>::Error: std::fmt::Display,
{
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        match T::try_from(event.clone()) {
            Ok(typed_value) => {
                // Successful conversion - convert back to ChainEvent
                vec![typed_value.into()]
            }
            Err(e) => {
                let error_msg = format!(
                    "Type conversion to {} failed: {}",
                    std::any::type_name::<T>(),
                    e
                );

                // Handle conversion failure according to error strategy
                match &self.error_strategy {
                    ErrorStrategy::Drop => {
                        // Silently drop failed conversions
                        vec![]
                    }
                    ErrorStrategy::ToErrorJournal => {
                        // Mark event with error status for error journal routing
                        let mut error_event = event;
                        error_event.processing_info.status = ProcessingStatus::error(error_msg);
                        vec![error_event]
                    }
                    ErrorStrategy::ToEventType(event_type) => {
                        // Emit as custom event type with validation_error field
                        let mut payload = event.payload();
                        payload["validation_error"] = json!(error_msg);

                        vec![ChainEventFactory::derived_data_event(
                            event.writer_id.clone(),
                            &event,
                            event_type,
                            payload,
                        )]
                    }
                    ErrorStrategy::ToEventTypeWith(event_type, payload_fn) => {
                        // Emit as custom event type with structured error payload
                        let mut payload = event.payload();

                        // Call user's function to create error payload structure
                        let error_value = payload_fn(&event, &error_msg);

                        // Merge the error value into the payload
                        if let serde_json::Value::Object(error_map) = error_value {
                            for (key, value) in error_map {
                                payload[key] = value;
                            }
                        }

                        vec![ChainEventFactory::derived_data_event(
                            event.writer_id.clone(),
                            &event,
                            event_type,
                            payload,
                        )]
                    }
                    ErrorStrategy::Custom(handler) => {
                        // Use custom error handler
                        handler(event, error_msg).into_iter().collect()
                    }
                }
            }
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use obzenflow_core::{EventId, WriterId};
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestOrder {
        id: String,
        amount: f64,
    }

    impl TryFrom<ChainEvent> for TestOrder {
        type Error = String;

        fn try_from(event: ChainEvent) -> Result<Self, Self::Error> {
            serde_json::from_value(event.payload().clone())
                .map_err(|e| format!("Failed to parse TestOrder: {}", e))
        }
    }

    impl From<TestOrder> for ChainEvent {
        fn from(order: TestOrder) -> Self {
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "order", json!(order))
        }
    }

    #[tokio::test]
    async fn test_try_map_success() {
        let try_mapper = TryMap::<TestOrder>::new();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "raw_order",
            json!({"id": "123", "amount": 99.99}),
        );

        let result = try_mapper.process(event);
        assert_eq!(result.len(), 1);

        // Event should be successfully converted and back
        let converted_event = &result[0];
        assert_eq!(converted_event.event_type(), "order");
        assert_eq!(converted_event.payload()["id"], json!("123"));
        assert_eq!(converted_event.payload()["amount"], json!(99.99));
    }

    #[tokio::test]
    async fn test_try_map_failure_returns_error_event() {
        let try_mapper = TryMap::<TestOrder>::new();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "invalid",
            json!({"wrong_field": "value"}),
        );

        let result = try_mapper.process(event);
        assert_eq!(result.len(), 1);

        // Event should be marked with error status
        let error_event = &result[0];
        assert!(matches!(
            error_event.processing_info.status,
            ProcessingStatus::Error { .. }
        ));

        if let ProcessingStatus::Error { message: msg, .. } = &error_event.processing_info.status {
            assert!(msg.contains("Type conversion"));
            assert!(msg.contains("TestOrder"));
        }
    }

    #[tokio::test]
    async fn test_try_map_clone() {
        let try_mapper = TryMap::<TestOrder>::new();
        let _cloned = try_mapper.clone();
        // Should compile and not panic
    }

    #[tokio::test]
    async fn test_try_map_debug() {
        let try_mapper = TryMap::<TestOrder>::new();
        let debug_str = format!("{:?}", try_mapper);
        assert!(debug_str.contains("TryMap"));
    }
}
