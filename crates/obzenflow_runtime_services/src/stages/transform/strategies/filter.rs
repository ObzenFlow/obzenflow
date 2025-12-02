//! Filter helper for transform stages
//!
//! The Filter helper wraps a predicate function to create a transform handler
//! that filters events. Events matching the predicate pass through, others are dropped.
//!
//! # Type-Safe Filtering
//!
//! Filter supports two modes:
//!
//! 1. **Typed Mode** (recommended): Work directly with domain types using serde
//! 2. **ChainEvent Mode**: Work directly with ChainEvent for maximum flexibility
//!
//! ## Typed Mode (Type-Safe)
//!
//! ```ignore
//! use obzenflow_runtime_services::stages::transform::strategies::FilterTyped;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct FlightRecord {
//!     validation_status: String,
//!     delay_minutes: u32,
//! }
//!
//! // Type-safe filtering - work with domain types!
//! let valid_filter = FilterTyped::new(|flight: &FlightRecord| {
//!     flight.validation_status == "valid"
//! });
//!
//! let delayed_filter = FilterTyped::new(|flight: &FlightRecord| {
//!     flight.delay_minutes > 0
//! });
//! ```
//!
//! ## ChainEvent Mode (Maximum Flexibility)
//!
//! ```ignore
//! use obzenflow_runtime_services::stages::transform::strategies::Filter;
//!
//! // For metadata-based filtering (not payload data)
//! let error_filter = Filter::new(|event| {
//!     event.payload()["level"].as_str() == Some("error")
//! });
//! ```

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::TransformHandler;
use async_trait::async_trait;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::ChainEvent;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

/// Filter helper for transform stages
///
/// Wraps a predicate function to create a transform handler that filters events.
/// Events matching the predicate pass through, others are dropped.
///
/// # Type Parameters
///
/// * `F` - The predicate function type: `Fn(&ChainEvent) -> bool`
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::transform::strategies::Filter;
///
/// // Filter by log level
/// let filter = Filter::new(|event| {
///     event.payload()["level"].as_str() == Some("error")
/// });
///
/// // Filter by numeric threshold
/// let high_value = Filter::new(|event| {
///     event.payload()["amount"].as_f64().unwrap_or(0.0) > 1000.0
/// });
/// ```
#[derive(Clone)]
pub struct Filter<F>
where
    F: Fn(&ChainEvent) -> bool + Send + Sync + Clone,
{
    predicate: F,
}

impl<F> Filter<F>
where
    F: Fn(&ChainEvent) -> bool + Send + Sync + Clone,
{
    /// Create a new Filter with the given predicate
    ///
    /// # Arguments
    ///
    /// * `predicate` - Function that returns `true` to keep the event, `false` to drop it
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime_services::stages::transform::strategies::Filter;
    ///
    /// let filter = Filter::new(|event| {
    ///     event.payload()["amount"].as_f64().unwrap_or(0.0) > 100.0
    /// });
    /// ```
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> std::fmt::Debug for Filter<F>
where
    F: Fn(&ChainEvent) -> bool + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Filter")
            .field("predicate", &"<closure>")
            .finish()
    }
}

#[async_trait]
impl<F> TransformHandler for Filter<F>
where
    F: Fn(&ChainEvent) -> bool + Send + Sync + Clone + 'static,
{
    fn process(
        &self,
        event: ChainEvent,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if (self.predicate)(&event) {
            Ok(vec![event])
        } else {
            Ok(vec![])
        }
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

// ============================================================================
// Typed Filter - Type-safe filtering with automatic serde
// ============================================================================

/// Typed Filter for type-safe filtering with automatic deserialization
///
/// This version of Filter works directly with domain types instead of ChainEvent,
/// eliminating the need for manual deserialization boilerplate.
/// Events that can't be deserialized to type T are dropped.
///
/// # Type Parameters
///
/// * `T` - Input type (must implement `DeserializeOwned`)
/// * `F` - Predicate function: `Fn(&T) -> bool`
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::transform::strategies::FilterTyped;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order { amount: f64, status: String }
///
/// // Filter by business logic on domain type
/// let high_value = FilterTyped::new(|order: &Order| {
///     order.amount > 1000.0
/// });
///
/// let pending = FilterTyped::new(|order: &Order| {
///     order.status == "pending"
/// });
/// ```
#[derive(Clone)]
pub struct FilterTyped<T, F>
where
    T: DeserializeOwned + Send + Sync,
    F: Fn(&T) -> bool + Send + Sync + Clone,
{
    predicate: F,
    _phantom: PhantomData<T>,
}

impl<T, F> FilterTyped<T, F>
where
    T: DeserializeOwned + Send + Sync,
    F: Fn(&T) -> bool + Send + Sync + Clone,
{
    /// Create a new typed Filter helper
    ///
    /// # Arguments
    ///
    /// * `predicate` - Function that returns `true` to keep the event, `false` to drop it
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime_services::stages::transform::strategies::FilterTyped;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Data { value: i32 }
    ///
    /// let filter = FilterTyped::new(|data: &Data| {
    ///     data.value > 100
    /// });
    /// ```
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> std::fmt::Debug for FilterTyped<T, F>
where
    T: DeserializeOwned + Send + Sync,
    F: Fn(&T) -> bool + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("predicate", &"<closure>")
            .finish()
    }
}

#[async_trait]
impl<T, F> TransformHandler for FilterTyped<T, F>
where
    T: DeserializeOwned + Send + Sync + 'static,
    F: Fn(&T) -> bool + Send + Sync + Clone + 'static,
{
    fn process(
        &self,
        event: ChainEvent,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Step 1: Deserialize ChainEvent payload → T
        let input_value: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(_) => {
                // Deserialization failed - event doesn't match type T, so drop it
                return Ok(vec![]);
            }
        };

        // Step 2: Apply predicate
        if (self.predicate)(&input_value) {
            Ok(vec![event])
        } else {
            Ok(vec![])
        }
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

// ============================================================================
// Add .typed() constructor to original Filter for convenience
// ============================================================================

impl<F> Filter<F>
where
    F: Fn(&ChainEvent) -> bool + Send + Sync + Clone,
{
    /// Create a typed Filter that works with domain types instead of ChainEvent
    ///
    /// This is a convenience method that delegates to FilterTyped::new().
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime_services::stages::transform::strategies::Filter;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Data { valid: bool }
    ///
    /// let filter = Filter::typed(|data: &Data| data.valid);
    /// ```
    pub fn typed<T, G>(predicate: G) -> FilterTyped<T, G>
    where
        T: DeserializeOwned + Send + Sync,
        G: Fn(&T) -> bool + Send + Sync + Clone,
    {
        FilterTyped::new(predicate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use obzenflow_core::{EventId, WriterId};
    use serde_json::json;

    #[tokio::test]
    async fn test_filter_passes_matching_events() {
        let filter = Filter::new(|event| event.payload()["level"].as_str() == Some("error"));

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "log",
            json!({"level": "error", "msg": "test"}),
        );

        let result = filter
            .process(event)
            .expect("Filter::process should succeed in test_filter_passes_matching_events");
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_filter_drops_non_matching_events() {
        let filter = Filter::new(|event| event.payload()["level"].as_str() == Some("error"));

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "log",
            json!({"level": "info", "msg": "test"}),
        );

        let result = filter
            .process(event)
            .expect("Filter::process should succeed in test_filter_drops_non_matching_events");
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_numeric_threshold() {
        let filter = Filter::new(|event| event.payload()["amount"].as_f64().unwrap_or(0.0) > 100.0);

        let high = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "order",
            json!({"amount": 150.0}),
        );
        let high_result = filter
            .process(high)
            .expect("Filter::process should succeed for high amount");
        assert_eq!(high_result.len(), 1);

        let low = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "order",
            json!({"amount": 50.0}),
        );
        let low_result = filter
            .process(low)
            .expect("Filter::process should succeed for low amount");
        assert_eq!(low_result.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_clone() {
        let filter = Filter::new(|event| event.payload()["valid"].as_bool().unwrap_or(false));

        let _cloned = filter.clone();
        // Should compile and not panic
    }
}
