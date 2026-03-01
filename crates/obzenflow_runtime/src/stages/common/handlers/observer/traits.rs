// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler trait for observation stages that monitor without modifying
//!
//! Examples: Loggers, metrics collectors, debuggers

use obzenflow_core::{ChainEvent, Result};

/// Handler for observation stages that monitor events without transformation
///
/// Observers are used for:
/// - Logging and debugging
/// - Metrics collection
/// - Monitoring and alerting
///
/// They see events but don't modify the data flow
///
/// # Example
/// ```ignore
/// use obzenflow_runtime::stages::common::handlers::ObserverHandler;
/// use obzenflow_core::{ChainEvent, Result};
/// use tracing::{debug, info};
///
/// #[derive(Debug)]
/// enum LogLevel {
///     Debug,
///     Info,
/// }
///
/// struct EventLogger {
///     log_level: LogLevel,
/// }
///
/// impl ObserverHandler for EventLogger {
///     fn observe(&self, event: &ChainEvent) -> Result<()> {
///         match self.log_level {
///             LogLevel::Debug => debug!("Event: {:?}", event),
///             LogLevel::Info => info!("Event type: {}", event.event_type()),
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait ObserverHandler: Send + Sync {
    /// Observe an event without modifying it
    ///
    /// The event is passed by reference since observers don't consume it
    fn observe(&self, event: &ChainEvent) -> Result<()>;

    /// Check if the observer is healthy
    fn is_healthy(&self) -> bool {
        true
    }
}
