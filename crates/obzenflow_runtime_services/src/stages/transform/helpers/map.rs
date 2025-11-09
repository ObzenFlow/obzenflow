//! Map helper for transform stages
//!
//! The Map helper wraps a transformation function to create a transform handler
//! that applies a 1-to-1 mapping to events.
//!
//! # Type-Safe Transformations
//!
//! Map supports two modes:
//!
//! 1. **Typed Mode** (recommended): Work directly with domain types using serde
//! 2. **ChainEvent Mode**: Work directly with ChainEvent for maximum flexibility
//!
//! ## Typed Mode (Type-Safe)
//!
//! ```rust
//! use obzenflow_runtime_services::stages::transform::helpers::MapTyped;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize)]
//! struct InvalidFlight { carrier: String, validation_errors: serde_json::Value }
//!
//! #[derive(Serialize)]
//! struct FixedFlight { carrier: String }
//!
//! // Type-safe transformation - no ChainEvent boilerplate!
//! let fixer = MapTyped::new(
//!     |flight: InvalidFlight| FixedFlight {
//!         carrier: if flight.carrier.is_empty() { "UNK".to_string() } else { flight.carrier }
//!     },
//!     "FlightRecord"  // Output event type
//! );
//! ```
//!
//! ## ChainEvent Mode (Maximum Flexibility)
//!
//! ```rust
//! use obzenflow_runtime_services::stages::transform::helpers::Map;
//!
//! let enricher = Map::new(|mut event| {
//!     event.payload_mut()["timestamp"] = json!(Utc::now().to_rfc3339());
//!     event
//! });
//! ```

use obzenflow_core::ChainEvent;
use obzenflow_core::event::ChainEventFactory;
use crate::stages::common::handlers::TransformHandler;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;

/// Map helper for transform stages
///
/// Wraps a transformation function to create a transform handler that applies
/// a 1-to-1 mapping to events.
///
/// # Type Parameters
///
/// * `F` - The mapper function type: `Fn(ChainEvent) -> ChainEvent`
///
/// # Examples
///
/// ```rust
/// use obzenflow_runtime_services::stages::transform::helpers::Map;
/// use serde_json::json;
///
/// // Add computed field
/// let enricher = Map::new(|mut event| {
///     let price = event.payload()["price"].as_f64().unwrap_or(0.0);
///     let qty = event.payload()["quantity"].as_f64().unwrap_or(0.0);
///     event.payload_mut()["total"] = json!(price * qty);
///     event
/// });
///
/// // Modify event type
/// let renamer = Map::new(|event| {
///     event.with_event_type("processed_event")
/// });
/// ```
#[derive(Clone)]
pub struct Map<F>
where
    F: Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone,
{
    mapper: F,
}

impl<F> Map<F>
where
    F: Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone,
{
    /// Create a new Map with the given transformation function
    ///
    /// # Arguments
    ///
    /// * `mapper` - Function that transforms a ChainEvent into another ChainEvent
    ///
    /// # Example
    /// ```rust
    /// use obzenflow_runtime_services::stages::transform::helpers::Map;
    /// use serde_json::json;
    ///
    /// let map = Map::new(|mut event| {
    ///     event.payload_mut()["processed"] = json!(true);
    ///     event
    /// });
    /// ```
    pub fn new(mapper: F) -> Self {
        Self { mapper }
    }
}

impl<F> std::fmt::Debug for Map<F>
where
    F: Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Map")
            .field("mapper", &"<closure>")
            .finish()
    }
}

#[async_trait]
impl<F> TransformHandler for Map<F>
where
    F: Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone + 'static,
{
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![(self.mapper)(event)]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

// ============================================================================
// Typed Map - Type-safe transformations with automatic serde
// ============================================================================

/// Typed Map for type-safe transformations with automatic serialization
///
/// This version of Map works directly with domain types instead of ChainEvent,
/// eliminating the need for manual serialization/deserialization boilerplate.
/// Unlike TryMapWith, Map transformations always succeed - there's no error handling.
///
/// The output event type is automatically derived from the output type's name.
///
/// # Type Parameters
///
/// * `T` - Input type (must implement `DeserializeOwned`)
/// * `O` - Output type (must implement `Serialize`)
/// * `F` - Mapper function: `Fn(T) -> O`
///
/// # Examples
///
/// ```rust
/// use obzenflow_runtime_services::stages::transform::helpers::MapTyped;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct RawData { value: i32 }
///
/// #[derive(Serialize)]
/// struct ProcessedData { value: i32, doubled: i32 }
///
/// let processor = MapTyped::new(|data: RawData| ProcessedData {
///     value: data.value,
///     doubled: data.value * 2,
/// });
/// ```
#[derive(Clone)]
pub struct MapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync,
    F: Fn(T) -> O + Send + Sync + Clone,
{
    mapper: F,
    _phantom: PhantomData<(T, O)>,
}

impl<T, O, F> MapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync,
    F: Fn(T) -> O + Send + Sync + Clone,
{
    /// Create a new typed Map helper
    ///
    /// The output event type is automatically derived from the output type name.
    ///
    /// # Arguments
    ///
    /// * `mapper` - Function that transforms T into O
    ///
    /// # Example
    /// ```rust
    /// use obzenflow_runtime_services::stages::transform::helpers::MapTyped;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Input { x: i32 }
    ///
    /// #[derive(Serialize)]
    /// struct Output { x: i32, squared: i32 }
    ///
    /// let mapper = MapTyped::new(|input: Input| Output {
    ///     x: input.x,
    ///     squared: input.x * input.x
    /// });
    /// ```
    pub fn new(mapper: F) -> Self {
        Self {
            mapper,
            _phantom: PhantomData,
        }
    }
}

impl<T, O, F> std::fmt::Debug for MapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync,
    F: Fn(T) -> O + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("output_type", &std::any::type_name::<O>())
            .finish()
    }
}

#[async_trait]
impl<T, O, F> TransformHandler for MapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(T) -> O + Send + Sync + Clone + 'static,
{
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Step 1: Deserialize ChainEvent payload → T
        let input_value: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(e) => {
                // Deserialization failed - this is a panic situation for Map
                // (unlike TryMapWith which has error handling)
                panic!(
                    "MapTyped: Failed to deserialize payload into {}: {}. \
                     Use TryMapWithTyped if deserialization can fail.",
                    std::any::type_name::<T>(),
                    e
                );
            }
        };

        // Step 2: Apply user's transformation T → O
        let output_value = (self.mapper)(input_value);

        // Step 3: Serialize O → ChainEvent payload
        let payload = match serde_json::to_value(&output_value) {
            Ok(p) => p,
            Err(e) => {
                panic!(
                    "MapTyped: Failed to serialize {} into payload: {}",
                    std::any::type_name::<O>(),
                    e
                );
            }
        };

        // TODO(FLOWIP-082a): Replace with TypedPayload::EVENT_TYPE when schemas are implemented
        // Current approach: Extract type name at runtime as temporary solution
        // Future: Prefer O::EVENT_TYPE (compile-time) if O implements TypedPayload trait
        // Fallback: Keep type_name() for ad-hoc types without schemas
        let event_type = std::any::type_name::<O>()
            .split("::")
            .last()
            .unwrap_or(std::any::type_name::<O>());

        vec![ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            event_type,
            payload,
        )]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

// ============================================================================
// Add .typed() constructor to original Map for convenience
// ============================================================================

impl<F> Map<F>
where
    F: Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone,
{
    /// Create a typed Map that works with domain types instead of ChainEvent
    ///
    /// This is a convenience method that delegates to MapTyped::new().
    ///
    /// # Example
    /// ```rust
    /// use obzenflow_runtime_services::stages::transform::helpers::Map;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Input { value: i32 }
    ///
    /// #[derive(Serialize)]
    /// struct Output { value: i32, processed: bool }
    ///
    /// let mapper = Map::typed(|input: Input| Output {
    ///     value: input.value,
    ///     processed: true
    /// });
    /// ```
    pub fn typed<T, O, G>(mapper: G) -> MapTyped<T, O, G>
    where
        T: DeserializeOwned + Send + Sync,
        O: Serialize + Send + Sync,
        G: Fn(T) -> O + Send + Sync + Clone,
    {
        MapTyped::new(mapper)
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
    async fn test_map_transforms_event() {
        let map = Map::new(|event| {
            let mut payload = event.payload();
            payload["processed"] = json!(true);

            ChainEventFactory::data_event(
                event.writer_id.clone(),
                event.event_type(),
                payload,
            )
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({"value": 42}),
        );

        let result = map.process(event);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].payload()["processed"], json!(true));
        assert_eq!(result[0].payload()["value"], json!(42));
    }

    #[tokio::test]
    async fn test_map_computed_field() {
        let map = Map::new(|event| {
            let price = event.payload()["price"].as_f64().unwrap_or(0.0);
            let qty = event.payload()["quantity"].as_f64().unwrap_or(0.0);
            let mut payload = event.payload();
            payload["total"] = json!(price * qty);

            ChainEventFactory::data_event(
                event.writer_id.clone(),
                event.event_type(),
                payload,
            )
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "order",
            json!({"price": 10.0, "quantity": 5.0}),
        );

        let result = map.process(event);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].payload()["total"], json!(50.0));
    }

    #[tokio::test]
    async fn test_map_event_type_change() {
        let map = Map::new(|event| {
            ChainEventFactory::data_event(
                event.writer_id.clone(),
                "processed",
                event.payload(),
            )
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "original",
            json!({}),
        );

        let result = map.process(event);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_type(), "processed");
    }

    #[tokio::test]
    async fn test_map_clone() {
        let map = Map::new(|event| {
            let mut payload = event.payload();
            payload["cloned"] = json!(true);

            ChainEventFactory::data_event(
                event.writer_id.clone(),
                event.event_type(),
                payload,
            )
        });

        let _cloned = map.clone();
        // Should compile and not panic
    }

    // ========================================================================
    // Typed Map Tests
    // ========================================================================

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Deserialize, PartialEq)]
    struct TestInput {
        value: i32,
        name: String,
    }

    #[derive(Debug, Clone, Serialize, PartialEq)]
    struct TestOutput {
        value: i32,
        name: String,
        doubled: i32,
    }

    #[tokio::test]
    async fn test_map_typed_success() {
        use super::MapTyped;

        let mapper = MapTyped::new(|input: TestInput| TestOutput {
            value: input.value,
            name: input.name.clone(),
            doubled: input.value * 2,
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "TestInput",
            json!({"value": 21, "name": "test"}),
        );

        let result = mapper.process(event);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_type(), "TestOutput");
        assert_eq!(result[0].payload()["value"], json!(21));
        assert_eq!(result[0].payload()["name"], json!("test"));
        assert_eq!(result[0].payload()["doubled"], json!(42));
    }

    #[tokio::test]
    async fn test_map_typed_transformation() {
        use super::MapTyped;

        let mapper = MapTyped::new(|input: TestInput| TestOutput {
            value: input.value * 10,
            name: input.name.to_uppercase(),
            doubled: input.value * 2,
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "RawInput",
            json!({"value": 5, "name": "hello"}),
        );

        let result = mapper.process(event);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_type(), "TestOutput");  // Derived from output type name
        assert_eq!(result[0].payload()["value"], json!(50));
        assert_eq!(result[0].payload()["name"], json!("HELLO"));
        assert_eq!(result[0].payload()["doubled"], json!(10));
    }

    #[tokio::test]
    async fn test_map_typed_clone() {
        use super::MapTyped;

        let mapper = MapTyped::new(|input: TestInput| TestOutput {
            value: input.value,
            name: input.name,
            doubled: input.value * 2,
        });

        let _cloned = mapper.clone();
        // Should compile and not panic
    }

    #[tokio::test]
    async fn test_map_typed_debug() {
        use super::MapTyped;

        let mapper = MapTyped::new(|input: TestInput| TestOutput {
            value: input.value,
            name: input.name,
            doubled: input.value * 2,
        });

        let debug_str = format!("{:?}", mapper);
        assert!(debug_str.contains("MapTyped"));
        assert!(debug_str.contains("TestInput"));
        assert!(debug_str.contains("TestOutput"));
        assert!(debug_str.contains("TestOutput")); // output_event_type
    }

    #[tokio::test]
    #[should_panic(expected = "MapTyped: Failed to deserialize")]
    async fn test_map_typed_deserialization_failure_panics() {
        use super::MapTyped;

        let mapper = MapTyped::new(|input: TestInput| TestOutput {
            value: input.value,
            name: input.name,
            doubled: input.value * 2,
        });

        // Missing required fields - should panic
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "TestInput",
            json!({"wrong_field": "value"}),
        );

        mapper.process(event); // Should panic
    }
}
