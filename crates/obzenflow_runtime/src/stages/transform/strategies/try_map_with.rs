// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! TryMapWith helper for closure-based fallible transformations
//!
//! The TryMapWith helper is a fallible variant of Map that enables ad-hoc transformations
//! using closures, without requiring trait implementations. This is ideal for one-off
//! transformations or quick prototyping with validation.
//!
//! # Type-Safe Transformations
//!
//! TryMapWith supports two modes:
//!
//! 1. **Typed Mode** (recommended): Work directly with domain types using serde
//! 2. **ChainEvent Mode**: Work directly with ChainEvent for maximum flexibility
//!
//! ## Typed Mode (Type-Safe)
//!
//! ```ignore
//! use obzenflow_runtime::stages::transform::strategies::TryMapWith;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize)]
//! struct FlightRecord { carrier: String, delay_minutes: u32 }
//!
//! #[derive(Serialize)]
//! struct ValidatedFlight { carrier: String, delay_minutes: u32, validated: bool }
//!
//! // Type-safe transformation - no ChainEvent boilerplate!
//! let validator = TryMapWith::typed(
//!     |flight: FlightRecord| -> Result<ValidatedFlight, String> {
//!         if flight.carrier.is_empty() {
//!             return Err("Missing carrier".to_string());
//!         }
//!         Ok(ValidatedFlight {
//!             carrier: flight.carrier,
//!             delay_minutes: flight.delay_minutes,
//!             validated: true,
//!         })
//!     },
//!     "FlightRecord.valid"  // Output event type for successful conversions
//! )
//! .on_error_emit("FlightRecord.invalid");  // Composable error handling!
//! ```
//!
//! ## ChainEvent Mode (Maximum Flexibility)
//!
//! ```ignore
//! use obzenflow_runtime::stages::transform::strategies::TryMapWith;
//!
//! let validator = TryMapWith::new(|event| {
//!     // Work directly with ChainEvent
//!     let amount = event.payload()["amount"].as_f64()
//!         .ok_or("Missing amount")?;
//!     Ok(event)
//! });
//! ```
//!
//! # Composable Error Handling
//!
//! Both modes support the same error handling strategies:
//!
//! - `.on_error_journal()` - Route to error journal (default)
//! - `.on_error_emit(type)` - Emit as custom event type
//! - `.on_error_emit_with(type, fn)` - Emit with structured error payload
//! - `.on_error_drop()` - Silently drop failures
//! - `.on_error_with(handler)` - Custom error handling

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::TransformHandler;
use async_trait::async_trait;
use obzenflow_core::event::{status::processing_status::ProcessingStatus, ChainEventFactory};
use obzenflow_core::ChainEvent;
use obzenflow_core::TypedPayload;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::marker::PhantomData;
use std::sync::Arc;

type ErrorPayloadFn = Arc<dyn Fn(&ChainEvent, &str) -> serde_json::Value + Send + Sync>;

/// Strategy for handling conversion errors in TryMapWith
///
/// This enum defines how TryMapWith should handle events that fail conversion.
/// The strategy can be configured using builder methods on TryMapWith.
#[derive(Clone)]
pub enum ErrorStrategy {
    /// Route failed conversions to error journal (default)
    ///
    /// Events are marked with ProcessingStatus::Error and routed to the error
    /// journal by the supervisor (FLOWIP-082e). Use this for terminal errors
    /// that should be logged and monitored but not retried.
    ToErrorJournal,

    /// Drop failed conversions silently
    ///
    /// Failed conversions produce no output events. Use this when failures
    /// are expected and should be filtered out of the pipeline.
    Drop,

    /// Emit failed conversions as a custom event type
    ///
    /// Creates a new event with the specified event_type, preserving the
    /// original payload with an added "validation_error" field. Use this
    /// for cycle-based error handling where invalid events need to flow
    /// through the pipeline for fixing/retry.
    ToEventType(String),

    /// Emit failed conversions with structured error payload
    ///
    /// Like ToEventType, but allows customizing how error info is added to the payload.
    /// The closure receives (original_event, error_message) and returns a serde_json::Value
    /// that will be merged into the payload. This is useful when downstream stages expect
    /// specific error field structures.
    ///
    /// # Example
    /// ```text
    /// // .on_error_emit_with("Record.invalid", |_event, error| {
    /// //     json!({"validation_errors": {"missing_fields": parse_error(error)}})
    /// // })
    /// ```
    ToEventTypeWith(String, ErrorPayloadFn),

    /// Custom error handler function
    ///
    /// Calls the provided closure with (original_event, error_message).
    /// The closure returns `Option<ChainEvent>` - Some to emit an event,
    /// None to drop it. Use this for complete custom error handling logic
    /// (e.g., creating entirely new events, circuit breakers, etc.).
    Custom(Arc<dyn Fn(ChainEvent, String) -> Option<ChainEvent> + Send + Sync>),
}

impl std::fmt::Debug for ErrorStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorStrategy::ToErrorJournal => write!(f, "ToErrorJournal"),
            ErrorStrategy::Drop => write!(f, "Drop"),
            ErrorStrategy::ToEventType(event_type) => write!(f, "ToEventType({event_type})"),
            ErrorStrategy::ToEventTypeWith(event_type, _) => {
                write!(f, "ToEventTypeWith({event_type}, <closure>)")
            }
            ErrorStrategy::Custom(_) => write!(f, "Custom(<closure>)"),
        }
    }
}

/// TryMapWith helper for closure-based fallible transformations
///
/// This helper enables inline conversions using closures, without requiring
/// trait implementations. The closure receives a ChainEvent and returns
/// `Result<ChainEvent, String>`. Conversion failures are handled according
/// to the configured ErrorStrategy.
///
/// # Type Parameters
///
/// * `F` - The converter function type: `Fn(ChainEvent) -> Result<ChainEvent, String>`
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
/// use obzenflow_runtime::stages::transform::strategies::TryMapWith;
/// use obzenflow_core::ChainEvent;
/// use obzenflow_core::event::ChainEventFactory;
/// use serde_json::json;
///
/// // Extract and validate a specific field (errors to journal)
/// let try_mapper = TryMapWith::new(|event| {
///     let user_id = event.payload()["user_id"]
///         .as_str()
///         .ok_or_else(|| "Missing user_id field".to_string())?;
///
///     if user_id.is_empty() {
///         return Err("Empty user_id".to_string());
///     }
///
///     Ok(event)
/// })
/// .on_error_journal();  // Explicit (this is the default)
///
/// // Validation with cycle-based error handling
/// let validator = TryMapWith::new(|event| {
///     validate_event(&event)?;
///     Ok(event)
/// })
/// .on_error_emit("Record.invalid");  // Invalid events flow to fixer
/// ```
#[derive(Clone)]
pub struct TryMapWith<F>
where
    F: Fn(ChainEvent) -> Result<ChainEvent, String> + Send + Sync + Clone,
{
    converter: F,
    error_strategy: ErrorStrategy,
}

impl<F> TryMapWith<F>
where
    F: Fn(ChainEvent) -> Result<ChainEvent, String> + Send + Sync + Clone,
{
    /// Create a new TryMapWith helper with the given fallible mapping function
    ///
    /// By default, errors are routed to the error journal (FLOWIP-082e).
    /// Use builder methods to configure different error handling strategies.
    ///
    /// # Arguments
    ///
    /// * `converter` - Function that converts ChainEvent, returning Result<ChainEvent, String>
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime::stages::transform::strategies::TryMapWith;
    ///
    /// let try_mapper = TryMapWith::new(|event| {
    ///     // Validation logic
    ///     let value = event.payload()["amount"]
    ///         .as_f64()
    ///         .ok_or("Missing amount field")?;
    ///
    ///     if value < 0.0 {
    ///         return Err("Negative amount not allowed".to_string());
    ///     }
    ///
    ///     Ok(event)
    /// })
    /// .on_error_journal();  // Route errors to error journal (default)
    /// ```
    pub fn new(converter: F) -> Self {
        Self {
            converter,
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
    /// - Unrecoverable validation failures
    /// - Errors that should not retry
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime::stages::transform::strategies::TryMapWith;
    ///
    /// let validator = TryMapWith::new(|event| {
    ///     validate_critical_field(&event)?;
    ///     Ok(event)
    /// })
    /// .on_error_journal();  // Failures go to error journal
    /// ```
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
    /// * `event_type` - The event type for failed conversions (e.g., "Record.invalid")
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime::stages::transform::strategies::TryMapWith;
    ///
    /// let validator = TryMapWith::new(|event| {
    ///     validate_record(&event)?;
    ///     Ok(event)
    /// })
    /// .on_error_emit("FlightRecord.invalid");  // Invalid events can be fixed
    ///
    /// // Topology:
    /// // validator → invalid_filter → fixer → validator (cycle)
    /// ```
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
    /// - Want to add multiple error-related fields at once
    ///
    /// # Arguments
    /// * `event_type` - The event type for failed conversions
    /// * `payload_fn` - Function `(event, error) -> serde_json::Value` to create error payload
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime::stages::transform::strategies::TryMapWith;
    /// use serde_json::json;
    ///
    /// let validator = TryMapWith::new(|event| {
    ///     let missing = find_missing_fields(&event);
    ///     if !missing.is_empty() {
    ///         return Err(
    ///             serde_json::to_string(&missing)
    ///                 .expect("serializing missing field list should not fail"),
    ///         );
    ///     }
    ///     Ok(event)
    /// })
    /// .on_error_emit_with("Record.invalid", |_event, error_json| {
    ///     let fields: Vec<String> = serde_json::from_str(error_json)
    ///         .expect("parsing error_json into Vec<String> should not fail");
    ///     json!({"validation_errors": {"missing_fields": fields}})
    /// });
    ///
    /// // Fixer can now access: payload["validation_errors"]["missing_fields"]
    /// ```
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
    /// - Expected validation failures that should be ignored
    /// - Filtering patterns where invalid events are discarded
    /// - Performance-sensitive pipelines where error logging is unnecessary
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime::stages::transform::strategies::TryMapWith;
    ///
    /// let filter_valid = TryMapWith::new(|event| {
    ///     if is_valid(&event) {
    ///         Ok(event)
    ///     } else {
    ///         Err("Invalid event".to_string())
    ///     }
    /// })
    /// .on_error_drop();  // Invalid events are silently discarded
    /// ```
    pub fn on_error_drop(mut self) -> Self {
        self.error_strategy = ErrorStrategy::Drop;
        self
    }

    /// Use a custom error handler for failed conversions
    ///
    /// Provides full control over error handling. The handler receives
    /// the original event and error message, returning `Option<ChainEvent>`
    /// (Some to emit, None to drop).
    ///
    /// # Arguments
    /// * `handler` - Function `(ChainEvent, String) -> Option<ChainEvent>`
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime::stages::transform::strategies::TryMapWith;
    /// use obzenflow_core::event::ChainEventFactory;
    /// use serde_json::json;
    ///
    /// let validator = TryMapWith::new(|event| {
    ///     validate(&event)?;
    ///     Ok(event)
    /// })
    /// .on_error_with(|event, error| {
    ///     // Log error and emit a custom event
    ///     println!("Validation failed: {}", error);
    ///     let mut payload = event.payload();
    ///     payload["error"] = json!(error);
    ///     payload["retry_count"] = json!(payload["retry_count"].as_u64().unwrap_or(0) + 1);
    ///
    ///     Some(ChainEventFactory::derived_data_event(
    ///         event.writer_id.clone(),
    ///         &event,
    ///         "Record.failed",
    ///         payload,
    ///     ))
    /// });
    /// ```
    pub fn on_error_with<H>(mut self, handler: H) -> Self
    where
        H: Fn(ChainEvent, String) -> Option<ChainEvent> + Send + Sync + 'static,
    {
        self.error_strategy = ErrorStrategy::Custom(Arc::new(handler));
        self
    }
}

impl<F> std::fmt::Debug for TryMapWith<F>
where
    F: Fn(ChainEvent) -> Result<ChainEvent, String> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TryMapWith")
            .field("converter", &"<closure>")
            .field("error_strategy", &self.error_strategy)
            .finish()
    }
}

#[async_trait]
impl<F> TransformHandler for TryMapWith<F>
where
    F: Fn(ChainEvent) -> Result<ChainEvent, String> + Send + Sync + Clone + 'static,
{
    fn process(
        &self,
        event: ChainEvent,
    ) -> Result<Vec<ChainEvent>, crate::stages::common::handler_error::HandlerError> {
        match (self.converter)(event.clone()) {
            Ok(converted) => Ok(vec![converted]),
            Err(error_msg) => {
                // Handle conversion failure according to error strategy
                let events = match &self.error_strategy {
                    ErrorStrategy::Drop => {
                        // Silently drop failed conversions
                        vec![]
                    }
                    ErrorStrategy::ToErrorJournal => {
                        // Mark event with error status for error journal routing
                        let mut error_event = event;
                        error_event.processing_info.status =
                            ProcessingStatus::error(format!("Conversion failed: {error_msg}"));
                        vec![error_event]
                    }
                    ErrorStrategy::ToEventType(event_type) => {
                        // Emit as custom event type with validation_error field
                        let mut payload = event.payload();
                        payload["validation_error"] = json!(error_msg);

                        vec![ChainEventFactory::derived_data_event(
                            event.writer_id,
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
                            event.writer_id,
                            &event,
                            event_type,
                            payload,
                        )]
                    }
                    ErrorStrategy::Custom(handler) => {
                        // Use custom error handler
                        handler(event, error_msg).into_iter().collect()
                    }
                };
                Ok(events)
            }
        }
    }

    async fn drain(&mut self) -> Result<(), crate::stages::common::handler_error::HandlerError> {
        Ok(())
    }
}

// ============================================================================
// Typed TryMapWith - Type-safe transformations with automatic serde
// ============================================================================

/// Typed TryMapWith for type-safe transformations with automatic serialization
///
/// This version of TryMapWith works directly with domain types instead of ChainEvent,
/// eliminating the need for manual serialization/deserialization boilerplate.
///
/// # Type Parameters
///
/// * `T` - Input type (must implement `DeserializeOwned`)
/// * `O` - Output type (must implement `Serialize`)
/// * `F` - Converter function: `Fn(T) -> Result<O, String>`
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::transform::strategies::TryMapWithTyped;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order { id: String, amount: f64 }
///
/// #[derive(Serialize)]
/// struct ValidatedOrder { id: String, amount: f64, validated: bool }
///
/// let validator = TryMapWithTyped::new(|order: Order| -> Result<ValidatedOrder, String> {
///     if order.amount < 0.0 {
///         return Err("Negative amount".to_string());
///     }
///     Ok(ValidatedOrder {
///         id: order.id,
///         amount: order.amount,
///         validated: true,
///     })
/// });
/// ```
#[derive(Clone)]
pub struct TryMapWithTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Result<O, String> + Send + Sync + Clone,
{
    converter: F,
    error_strategy: ErrorStrategy,
    _phantom: PhantomData<(T, O)>,
}

impl<T, O, F> TryMapWithTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Result<O, String> + Send + Sync + Clone,
{
    /// Create a new typed TryMapWith helper
    ///
    /// The output type O must implement TypedPayload to provide the event type name.
    ///
    /// # Arguments
    ///
    /// * `converter` - Function that transforms T into Result<O, String>
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime::stages::transform::strategies::TryMapWithTyped;
    /// use obzenflow_core::TypedPayload;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Flight { carrier: String }
    ///
    /// #[derive(Serialize)]
    /// struct ValidatedFlight { carrier: String, validated: bool }
    ///
    /// impl TypedPayload for ValidatedFlight {
    ///     const EVENT_TYPE: &'static str = "flight.validated.v1";
    /// }
    ///
    /// let validator = TryMapWithTyped::new(|flight: Flight| {
    ///     if flight.carrier.is_empty() {
    ///         return Err("Missing carrier".to_string());
    ///     }
    ///     Ok(ValidatedFlight { carrier: flight.carrier, validated: true })
    /// });
    /// ```
    pub fn new(converter: F) -> Self {
        Self {
            converter,
            error_strategy: ErrorStrategy::ToErrorJournal,
            _phantom: PhantomData,
        }
    }

    /// Route failed conversions to the error journal (default behavior)
    pub fn on_error_journal(mut self) -> Self {
        self.error_strategy = ErrorStrategy::ToErrorJournal;
        self
    }

    /// Emit failed conversions as a custom event type
    pub fn on_error_emit(mut self, event_type: impl Into<String>) -> Self {
        self.error_strategy = ErrorStrategy::ToEventType(event_type.into());
        self
    }

    /// Emit failed conversions with structured error payload
    pub fn on_error_emit_with<P>(mut self, event_type: impl Into<String>, payload_fn: P) -> Self
    where
        P: Fn(&ChainEvent, &str) -> serde_json::Value + Send + Sync + 'static,
    {
        self.error_strategy =
            ErrorStrategy::ToEventTypeWith(event_type.into(), Arc::new(payload_fn));
        self
    }

    /// Drop failed conversions silently
    pub fn on_error_drop(mut self) -> Self {
        self.error_strategy = ErrorStrategy::Drop;
        self
    }

    /// Use a custom error handler for failed conversions
    pub fn on_error_with<H>(mut self, handler: H) -> Self
    where
        H: Fn(ChainEvent, String) -> Option<ChainEvent> + Send + Sync + 'static,
    {
        self.error_strategy = ErrorStrategy::Custom(Arc::new(handler));
        self
    }
}

impl<T, O, F> std::fmt::Debug for TryMapWithTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Result<O, String> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TryMapWithTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("output_type", &std::any::type_name::<O>())
            .field("error_strategy", &self.error_strategy)
            .finish()
    }
}

#[async_trait]
impl<T, O, F> TransformHandler for TryMapWithTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync + 'static,
    O: Serialize + Send + Sync + TypedPayload + 'static,
    F: Fn(T) -> Result<O, String> + Send + Sync + Clone + 'static,
{
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        // Step 1: Deserialize ChainEvent payload → T
        let input_value: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(e) => {
                let error_msg = format!(
                    "Failed to deserialize payload into {}: {}",
                    std::any::type_name::<T>(),
                    e
                );
                // Deserialization failed - handle according to error strategy
                return Ok(self.handle_error(event, error_msg));
            }
        };

        // Step 2: Apply user's transformation T → Result<O, String>
        match (self.converter)(input_value) {
            Ok(output_value) => {
                // Step 3: Serialize O → ChainEvent payload
                match serde_json::to_value(&output_value) {
                    Ok(payload) => {
                        // FLOWIP-082a: Use TypedPayload::EVENT_TYPE (compile-time constant)
                        let event_type = O::versioned_event_type();
                        Ok(vec![ChainEventFactory::derived_data_event(
                            event.writer_id,
                            &event,
                            &event_type,
                            payload,
                        )])
                    }
                    Err(e) => {
                        let error_msg = format!(
                            "Failed to serialize {} into payload: {}",
                            std::any::type_name::<O>(),
                            e
                        );
                        Ok(self.handle_error(event, error_msg))
                    }
                }
            }
            Err(error_msg) => {
                // Transformation failed - handle according to error strategy
                Ok(self.handle_error(event, error_msg))
            }
        }
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

impl<T, O, F> TryMapWithTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Result<O, String> + Send + Sync + Clone,
{
    /// Handle errors according to the configured error strategy
    fn handle_error(&self, event: ChainEvent, error_msg: String) -> Vec<ChainEvent> {
        match &self.error_strategy {
            ErrorStrategy::Drop => vec![],
            ErrorStrategy::ToErrorJournal => {
                let mut error_event = event;
                error_event.processing_info.status = ProcessingStatus::error(error_msg);
                vec![error_event]
            }
            ErrorStrategy::ToEventType(event_type) => {
                let mut payload = event.payload();
                payload["validation_error"] = json!(error_msg);
                vec![ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    event_type,
                    payload,
                )]
            }
            ErrorStrategy::ToEventTypeWith(event_type, payload_fn) => {
                let mut payload = event.payload();
                let error_value = payload_fn(&event, &error_msg);
                if let serde_json::Value::Object(error_map) = error_value {
                    for (key, value) in error_map {
                        payload[key] = value;
                    }
                }
                vec![ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    event_type,
                    payload,
                )]
            }
            ErrorStrategy::Custom(handler) => handler(event, error_msg).into_iter().collect(),
        }
    }
}

// ============================================================================
// Add .typed() constructor to original TryMapWith for convenience
// ============================================================================

impl<F> TryMapWith<F>
where
    F: Fn(ChainEvent) -> Result<ChainEvent, String> + Send + Sync + Clone,
{
    /// Create a typed TryMapWith that works with domain types instead of ChainEvent
    ///
    /// This is a convenience method that delegates to TryMapWithTyped::new().
    ///
    /// # Example
    /// ```ignore
    /// use obzenflow_runtime::stages::transform::strategies::TryMapWith;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Input { value: i32 }
    ///
    /// #[derive(Serialize)]
    /// struct Output { value: i32, validated: bool }
    ///
    /// let validator = TryMapWith::typed(
    ///     |input: Input| Ok(Output { value: input.value, validated: true }),
    ///     "Output"
    /// );
    /// ```
    pub fn typed<T, O, G>(converter: G) -> TryMapWithTyped<T, O, G>
    where
        T: DeserializeOwned + Send + Sync,
        O: Serialize + Send + Sync + TypedPayload,
        G: Fn(T) -> Result<O, String> + Send + Sync + Clone,
    {
        TryMapWithTyped::new(converter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use obzenflow_core::WriterId;
    use serde_json::json;

    #[tokio::test]
    async fn test_try_map_with_success() {
        let try_mapper = TryMapWith::new(|event| {
            let payload = event.payload();
            let user_id = payload["user_id"]
                .as_str()
                .ok_or_else(|| "Missing user_id".to_string())?
                .to_string();

            Ok(ChainEventFactory::data_event(
                event.writer_id,
                "user_id_extracted",
                json!({"user_id": user_id}),
            ))
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "user_event",
            json!({"user_id": "user123", "action": "login"}),
        );

        let result = try_mapper
            .process(event)
            .expect("TryMapWith basic success case should not fail in tests");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_type(), "user_id_extracted");
        assert_eq!(result[0].payload()["user_id"], json!("user123"));
    }

    #[tokio::test]
    async fn test_try_map_with_validation_error() {
        let try_mapper = TryMapWith::new(|event| {
            let amount = event.payload()["amount"]
                .as_f64()
                .ok_or_else(|| "Missing amount field".to_string())?;

            if amount < 0.0 {
                return Err("Negative amount not allowed".to_string());
            }

            Ok(event)
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({"amount": -100.0}),
        );

        let result = try_mapper
            .process(event)
            .expect("TryMapWith validation_error should not fail handler itself");
        assert_eq!(result.len(), 1);

        // Event should be marked with error status
        assert!(matches!(
            result[0].processing_info.status,
            ProcessingStatus::Error { .. }
        ));

        if let ProcessingStatus::Error { message: msg, .. } = &result[0].processing_info.status {
            assert!(msg.contains("Negative amount"));
        }
    }

    #[tokio::test]
    async fn test_try_map_with_missing_field() {
        let try_mapper = TryMapWith::new(|event| {
            let _user_id = event.payload()["user_id"]
                .as_str()
                .ok_or_else(|| "Missing user_id field".to_string())?;

            Ok(event)
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "event",
            json!({"other_field": "value"}),
        );

        let result = try_mapper
            .process(event)
            .expect("TryMapWith missing_field should not fail handler itself");
        assert_eq!(result.len(), 1);

        assert!(matches!(
            result[0].processing_info.status,
            ProcessingStatus::Error { .. }
        ));

        if let ProcessingStatus::Error { message: msg, .. } = &result[0].processing_info.status {
            assert!(msg.contains("Missing user_id"));
        }
    }

    #[tokio::test]
    async fn test_try_map_with_passthrough() {
        let try_mapper = TryMapWith::new(|event| {
            // Add a validation flag
            let mut payload = event.payload();
            payload["validated"] = json!(true);

            Ok(ChainEventFactory::data_event(
                event.writer_id,
                event.event_type(),
                payload,
            ))
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({"value": 42}),
        );

        let result = try_mapper
            .process(event)
            .expect("TryMapWith passthrough should not fail handler itself");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].payload()["validated"], json!(true));
        assert_eq!(result[0].payload()["value"], json!(42));
    }

    #[tokio::test]
    async fn test_try_map_with_clone() {
        let try_mapper = TryMapWith::new(Ok);
        let _cloned = try_mapper.clone();
        // Should compile and not panic
    }

    // ========================================================================
    // Error Strategy Tests
    // ========================================================================

    #[tokio::test]
    async fn test_on_error_drop_strategy() {
        let try_mapper = TryMapWith::new(|event| {
            let amount = event.payload()["amount"].as_f64().ok_or("Missing amount")?;

            if amount < 0.0 {
                return Err("Negative amount".to_string());
            }

            Ok(event)
        })
        .on_error_drop();

        // Valid event should pass through
        let valid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({"amount": 100.0}),
        );
        let result = try_mapper
            .process(valid_event)
            .expect("TryMapWith on_error_drop valid case should not fail");
        assert_eq!(result.len(), 1);

        // Invalid event should be dropped
        let invalid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({"amount": -50.0}),
        );
        let result = try_mapper
            .process(invalid_event)
            .expect("TryMapWith on_error_drop invalid case should not fail handler itself");
        assert_eq!(result.len(), 0); // Dropped!
    }

    #[tokio::test]
    async fn test_on_error_emit_strategy() {
        let try_mapper = TryMapWith::new(|event| {
            let amount = event.payload()["amount"].as_f64().ok_or("Missing amount")?;

            if amount < 0.0 {
                return Err("Negative amount".to_string());
            }

            Ok(event)
        })
        .on_error_emit("Transaction.invalid");

        // Invalid event should be emitted as custom event type
        let invalid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({"amount": -50.0}),
        );

        let result = try_mapper
            .process(invalid_event)
            .expect("TryMapWith ToErrorJournal should not fail in tests");
        assert_eq!(result.len(), 1);

        // Check event type
        assert_eq!(result[0].event_type(), "Transaction.invalid");

        // Check validation_error field was added
        assert!(result[0].payload().get("validation_error").is_some());
        assert_eq!(
            result[0].payload()["validation_error"]
                .as_str()
                .expect("validation_error should be a string"),
            "Negative amount"
        );

        // Original payload should be preserved
        assert_eq!(result[0].payload()["amount"], json!(-50.0));

        // Processing status should still be Success (not Error!)
        assert!(matches!(
            result[0].processing_info.status,
            ProcessingStatus::Success
        ));
    }

    #[tokio::test]
    async fn test_on_error_journal_strategy_explicit() {
        let try_mapper = TryMapWith::new(|event| {
            let amount = event.payload()["amount"].as_f64().ok_or("Missing amount")?;

            if amount < 0.0 {
                return Err("Negative amount".to_string());
            }

            Ok(event)
        })
        .on_error_journal(); // Explicit (same as default)

        // Invalid event should be marked with error status
        let invalid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({"amount": -50.0}),
        );

        let result = try_mapper
            .process(invalid_event)
            .expect("TryMapWith on_error_journal should not fail in tests");
        assert_eq!(result.len(), 1);

        // Should have error status
        assert!(matches!(
            result[0].processing_info.status,
            ProcessingStatus::Error { .. }
        ));
    }

    #[tokio::test]
    async fn test_on_error_with_custom_handler() {
        let try_mapper = TryMapWith::new(|event| {
            let amount = event.payload()["amount"].as_f64().ok_or("Missing amount")?;

            if amount < 0.0 {
                return Err("Negative amount".to_string());
            }

            Ok(event)
        })
        .on_error_with(|event, error| {
            // Custom handler: add retry_count and emit as "Transaction.failed"
            let mut payload = event.payload();
            payload["error"] = json!(error);
            payload["retry_count"] = json!(payload["retry_count"].as_u64().unwrap_or(0) + 1);

            Some(ChainEventFactory::derived_data_event(
                event.writer_id,
                &event,
                "Transaction.failed",
                payload,
            ))
        });

        // Invalid event should be processed by custom handler
        let invalid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({"amount": -50.0}),
        );

        let result = try_mapper
            .process(invalid_event)
            .expect("TryMapWith custom handler should not fail in tests");
        assert_eq!(result.len(), 1);

        // Check custom event type
        assert_eq!(result[0].event_type(), "Transaction.failed");

        // Check custom fields added
        assert_eq!(result[0].payload()["error"], json!("Negative amount"));
        assert_eq!(result[0].payload()["retry_count"], json!(1));

        // Original payload should be preserved
        assert_eq!(result[0].payload()["amount"], json!(-50.0));
    }

    #[tokio::test]
    async fn test_on_error_with_custom_handler_can_drop() {
        let try_mapper = TryMapWith::new(|_event| Err("Always fail".to_string())).on_error_with(
            |_event, _error| {
                None // Drop the event
            },
        );

        let event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}));

        let result = try_mapper
            .process(event)
            .expect("TryMapWith custom drop handler should not fail in tests");
        assert_eq!(result.len(), 0); // Dropped by custom handler
    }

    // ========================================================================
    // Typed TryMapWith Tests
    // ========================================================================

    #[derive(Debug, Clone, serde::Deserialize, PartialEq)]
    struct TestInput {
        value: i32,
        name: String,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    struct TestOutput {
        value: i32,
        name: String,
        validated: bool,
    }

    impl obzenflow_core::TypedPayload for TestOutput {
        const EVENT_TYPE: &'static str = "test.output.validated";
    }

    #[tokio::test]
    async fn test_typed_try_map_with_success() {
        use super::TryMapWithTyped;

        let validator = TryMapWithTyped::new(|input: TestInput| -> Result<TestOutput, String> {
            if input.value < 0 {
                return Err("Negative value".to_string());
            }
            Ok(TestOutput {
                value: input.value,
                name: input.name,
                validated: true,
            })
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "TestInput",
            json!({"value": 42, "name": "test"}),
        );

        let result = validator
            .process(event)
            .expect("TryMapWithTyped success case should not fail in tests");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_type(), "test.output.validated.v1");
        assert_eq!(result[0].payload()["value"], json!(42));
        assert_eq!(result[0].payload()["name"], json!("test"));
        assert_eq!(result[0].payload()["validated"], json!(true));
    }

    #[tokio::test]
    async fn test_typed_try_map_with_validation_failure() {
        use super::TryMapWithTyped;

        let validator = TryMapWithTyped::new(|input: TestInput| -> Result<TestOutput, String> {
            if input.value < 0 {
                return Err("Negative value".to_string());
            }
            Ok(TestOutput {
                value: input.value,
                name: input.name,
                validated: true,
            })
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "TestInput",
            json!({"value": -10, "name": "test"}),
        );

        let result = validator
            .process(event)
            .expect("TryMapWithTyped validation_failure should not fail handler itself");
        assert_eq!(result.len(), 1);

        // Should be marked with error status (default strategy)
        assert!(matches!(
            result[0].processing_info.status,
            ProcessingStatus::Error { .. }
        ));

        if let ProcessingStatus::Error { message: msg, .. } = &result[0].processing_info.status {
            assert!(msg.contains("Negative value"));
        }
    }

    #[tokio::test]
    async fn test_typed_try_map_with_deserialization_failure() {
        use super::TryMapWithTyped;

        let validator = TryMapWithTyped::new(|input: TestInput| -> Result<TestOutput, String> {
            Ok(TestOutput {
                value: input.value,
                name: input.name,
                validated: true,
            })
        });

        // Missing required fields
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "TestInput",
            json!({"wrong_field": "value"}),
        );

        let result = validator
            .process(event)
            .expect("TryMapWithTyped deserialization_failure should not fail handler itself");
        assert_eq!(result.len(), 1);

        // Should be marked with error status
        assert!(matches!(
            result[0].processing_info.status,
            ProcessingStatus::Error { .. }
        ));

        if let ProcessingStatus::Error { message: msg, .. } = &result[0].processing_info.status {
            assert!(msg.contains("Failed to deserialize"));
            assert!(msg.contains("TestInput"));
        }
    }

    #[tokio::test]
    async fn test_typed_try_map_with_on_error_emit() {
        use super::TryMapWithTyped;

        let validator = TryMapWithTyped::new(|input: TestInput| -> Result<TestOutput, String> {
            if input.value < 0 {
                return Err("Negative value".to_string());
            }
            Ok(TestOutput {
                value: input.value,
                name: input.name,
                validated: true,
            })
        })
        .on_error_emit("TestInput.invalid");

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "TestInput",
            json!({"value": -10, "name": "test"}),
        );

        let result = validator
            .process(event)
            .expect("TryMapWithTyped on_error_emit should not fail handler itself");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_type(), "TestInput.invalid");
        assert!(result[0].payload().get("validation_error").is_some());

        // Original payload should be preserved
        assert_eq!(result[0].payload()["value"], json!(-10));
    }

    #[tokio::test]
    async fn test_typed_try_map_with_on_error_emit_with() {
        use super::TryMapWithTyped;

        let validator = TryMapWithTyped::new(|input: TestInput| -> Result<TestOutput, String> {
            if input.value < 0 {
                return Err("Negative value".to_string());
            }
            Ok(TestOutput {
                value: input.value,
                name: input.name,
                validated: true,
            })
        })
        .on_error_emit_with(
            "TestInput.invalid",
            |_event, error| json!({"error_details": {"message": error, "severity": "high"}}),
        );

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "TestInput",
            json!({"value": -10, "name": "test"}),
        );

        let result = validator
            .process(event)
            .expect("TryMapWithTyped on_error_emit_with should not fail handler itself");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_type(), "TestInput.invalid");

        // Check structured error payload
        assert_eq!(
            result[0].payload()["error_details"]["message"],
            json!("Negative value")
        );
        assert_eq!(
            result[0].payload()["error_details"]["severity"],
            json!("high")
        );

        // Original payload should be preserved
        assert_eq!(result[0].payload()["value"], json!(-10));
    }

    #[tokio::test]
    async fn test_typed_try_map_with_on_error_drop() {
        use super::TryMapWithTyped;

        let validator = TryMapWithTyped::new(|input: TestInput| -> Result<TestOutput, String> {
            if input.value < 0 {
                return Err("Negative value".to_string());
            }
            Ok(TestOutput {
                value: input.value,
                name: input.name,
                validated: true,
            })
        })
        .on_error_drop();

        // Valid event should pass through
        let valid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "TestInput",
            json!({"value": 42, "name": "test"}),
        );
        let result = validator
            .process(valid_event)
            .expect("TryMapWithTyped on_error_drop valid case should not fail");
        assert_eq!(result.len(), 1);

        // Invalid event should be dropped
        let invalid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "TestInput",
            json!({"value": -10, "name": "test"}),
        );
        let result = validator
            .process(invalid_event)
            .expect("TryMapWithTyped on_error_drop invalid case should not fail handler itself");
        assert_eq!(result.len(), 0); // Dropped!
    }

    #[tokio::test]
    async fn test_typed_try_map_with_clone() {
        use super::TryMapWithTyped;

        let validator = TryMapWithTyped::new(|input: TestInput| -> Result<TestOutput, String> {
            Ok(TestOutput {
                value: input.value,
                name: input.name,
                validated: true,
            })
        });

        let _cloned = validator.clone();
        // Should compile and not panic
    }

    #[tokio::test]
    async fn test_typed_try_map_with_debug() {
        use super::TryMapWithTyped;

        let validator = TryMapWithTyped::new(|input: TestInput| -> Result<TestOutput, String> {
            Ok(TestOutput {
                value: input.value,
                name: input.name,
                validated: true,
            })
        });

        let debug_str = format!("{validator:?}");
        assert!(debug_str.contains("TryMapWithTyped"));
        assert!(debug_str.contains("TestInput"));
        assert!(debug_str.contains("TestOutput"));
    }
}
