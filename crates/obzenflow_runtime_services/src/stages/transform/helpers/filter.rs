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
//! ```rust
//! use obzenflow_runtime_services::stages::transform::helpers::FilterTyped;
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
//! ```rust
//! use obzenflow_runtime_services::stages::transform::helpers::Filter;
//!
//! // For metadata-based filtering (not payload data)
//! let error_filter = Filter::new(|event| {
//!     event.payload()["level"].as_str() == Some("error")
//! });
//! ```

use obzenflow_core::ChainEvent;
use obzenflow_core::event::ChainEventFactory;
use crate::stages::common::handlers::TransformHandler;
use async_trait::async_trait;
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
/// ```rust
/// use obzenflow_runtime_services::stages::transform::helpers::Filter;
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
    /// ```rust
    /// use obzenflow_runtime_services::stages::transform::helpers::Filter;
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
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if (self.predicate)(&event) {
            vec![event]
        } else {
            vec![]
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
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
/// ```rust
/// use obzenflow_runtime_services::stages::transform::helpers::FilterTyped;
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
    /// ```rust
    /// use obzenflow_runtime_services::stages::transform::helpers::FilterTyped;
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
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Step 1: Deserialize ChainEvent payload → T
        let input_value: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(_) => {
                // Deserialization failed - event doesn't match type T, so drop it
                return vec![];
            }
        };

        // Step 2: Apply predicate
        if (self.predicate)(&input_value) {
            vec![event]
        } else {
            vec![]
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
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
    /// ```rust
    /// use obzenflow_runtime_services::stages::transform::helpers::Filter;
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
    use obzenflow_core::{WriterId, EventId};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use serde_json::json;

    #[tokio::test]
    async fn test_filter_passes_matching_events() {
        let filter = Filter::new(|event| {
            event.payload()["level"].as_str() == Some("error")
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "log",
            json!({"level": "error", "msg": "test"}),
        );

        let result = filter.process(event);
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_filter_drops_non_matching_events() {
        let filter = Filter::new(|event| {
            event.payload()["level"].as_str() == Some("error")
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "log",
            json!({"level": "info", "msg": "test"}),
        );

        let result = filter.process(event);
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_numeric_threshold() {
        let filter = Filter::new(|event| {
            event.payload()["amount"].as_f64().unwrap_or(0.0) > 100.0
        });

        let high = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "order",
            json!({"amount": 150.0}),
        );
        assert_eq!(filter.process(high).len(), 1);

        let low = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "order",
            json!({"amount": 50.0}),
        );
        assert_eq!(filter.process(low).len(), 0);
    }

    #[tokio::test]
    async fn test_filter_clone() {
        let filter = Filter::new(|event| {
            event.payload()["valid"].as_bool().unwrap_or(false)
        });

        let _cloned = filter.clone();
        // Should compile and not panic
    }
}
