// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source handler traits for both finite and infinite sources
//!
//! 051b refinement:
//! - Finite sources now use Result<Option<Vec<ChainEvent>>, SourceError> so EOF
//!   and infra failures are explicit in the type system.
//! - Infinite sources use Result<Vec<ChainEvent>, SourceError>.

use async_trait::async_trait;
use obzenflow_core::ChainEvent;
use obzenflow_core::WriterId;
use std::fmt;

/// Errors that can occur while polling a source.
///
/// This is intentionally small for now; 082h will own any cross-stage
/// unification with a broader StageError taxonomy.
#[derive(Debug)]
pub enum SourceError {
    /// The underlying transport or dependency timed out.
    Timeout(String),
    /// The underlying transport or dependency failed (e.g. network error).
    Transport(String),
    /// The source encountered malformed data it could not deserialize.
    Deserialization(String),
    /// Catch-all for other source-specific failures.
    Other(String),
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceError::Timeout(msg) => write!(f, "source timeout: {msg}"),
            SourceError::Transport(msg) => write!(f, "source transport error: {msg}"),
            SourceError::Deserialization(msg) => {
                write!(f, "source deserialization error: {msg}")
            }
            SourceError::Other(msg) => write!(f, "source error: {msg}"),
        }
    }
}

impl std::error::Error for SourceError {}

/// Handler for sources that eventually complete (files, bounded collections)
///
/// Finite sources:
/// - Pull events using `next()`
/// - Eventually complete when data is exhausted (signalled via `Ok(None)`)
/// - Supervisor sends EOF when `next()` returns `Ok(None)`
///
/// # Example
/// ```ignore
/// use obzenflow_runtime::stages::common::handlers::FiniteSourceHandler;
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
///     fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
///         if self.index < self.items.len() {
///             let item = self.items[self.index].clone();
///             self.index += 1;
///             Ok(Some(vec![ChainEvent::data(
///                 EventId::new(),
///                 self.writer_id,
///                 "data",
///                 json!({"item": item}),
///             )]))
///         } else {
///             Ok(None)
///         }
///     }
/// }
/// ```
pub trait FiniteSourceHandler: Send + Sync {
    /// Called by the runtime before the first `next()` to inject the stage `WriterId`.
    ///
    /// Default is a no-op for existing handlers that manage their own `WriterId`.
    fn bind_writer_id(&mut self, _id: WriterId) {}

    /// Pull zero or more events from the source.
    ///
    /// - `Ok(Some(events))` means the source advanced; `events` may be empty
    ///   (no new data right now) or contain one or more events.
    /// - `Ok(None)` means the source is exhausted; supervisor will send EOF.
    /// - `Err(SourceError)` means polling the source failed (timeout,
    ///   transport error, deserialization error, etc.).
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError>;
}

/// Async handler for finite sources.
///
/// This is intended for IO-bound sources (HTTP, DB reads, scraping) that need to `await`
/// without blocking the runtime.
///
/// Semantics mirror `FiniteSourceHandler`:
/// - `Ok(Some(events))` → source advanced; events may be empty or non-empty
/// - `Ok(None)` → source exhausted; supervisor emits EOF and completes
/// - `Err(SourceError)` → polling failed; middleware converts to error-marked event
#[async_trait]
pub trait AsyncFiniteSourceHandler: Send + Sync {
    /// Called by the runtime before the first `next()` to inject the stage `WriterId`.
    ///
    /// Default is a no-op for existing handlers that manage their own `WriterId`.
    fn bind_writer_id(&mut self, _id: WriterId) {}

    /// Pull zero or more events from the source asynchronously.
    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError>;

    /// Perform any cleanup during shutdown.
    ///
    /// Default is a no-op. Errors are logged by the supervisor but do not block shutdown.
    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

/// Async handler for infinite sources.
///
/// This is intended for push/streaming sources that should await incoming data efficiently
/// (e.g. `recv().await` on an internal channel) instead of polling with `try_recv()`.
///
/// Semantics mirror `InfiniteSourceHandler`:
/// - `Ok(events)` → source advanced; `events` may be empty or non-empty
/// - `Err(SourceError)` → polling failed; middleware converts to error-marked event
///
/// Infinite sources never complete naturally; shutdown is signaled externally (e.g. BeginDrain)
/// and handlers can optionally implement `drain()` for best-effort cleanup.
#[async_trait]
pub trait AsyncInfiniteSourceHandler: Send + Sync {
    /// Called by the runtime before the first `next()` to inject the stage `WriterId`.
    ///
    /// Default is a no-op for existing handlers that manage their own `WriterId`.
    fn bind_writer_id(&mut self, _id: WriterId) {}

    /// Pull zero or more events from the source asynchronously.
    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError>;

    /// Perform any cleanup during shutdown.
    ///
    /// Default is a no-op. Errors are logged by the supervisor but do not block shutdown.
    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

/// Handler for sources that run indefinitely
///
/// Infinite sources:
/// - Never complete naturally
/// - Only stop on shutdown signal or error
/// - Examples: Kafka consumers, WebSocket streams, system monitors
///
/// # Example
/// ```ignore
/// use obzenflow_runtime::stages::common::handlers::InfiniteSourceHandler;
/// use obzenflow_core::{ChainEvent, EventId, WriterId};
/// use serde_json::json;
/// use std::time::{Duration, Instant};
///
/// struct HeartbeatSource {
///     writer_id: WriterId,
///     last_beat: Instant,
///     interval: Duration,
/// }
///
/// impl InfiniteSourceHandler for HeartbeatSource {
///     fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
///         let now = Instant::now();
///         if now.duration_since(self.last_beat) >= self.interval {
///             self.last_beat = now;
///             Ok(vec![ChainEvent::data(
///                 EventId::new(),
///                 self.writer_id,
///                 "heartbeat",
///                 json!({"heartbeat": true, "timestamp": chrono::Utc::now().to_rfc3339()}),
///             )])
///         } else {
///             Ok(Vec::new())
///         }
///     }
/// }
/// ```
pub trait InfiniteSourceHandler: Send + Sync {
    /// Called by the runtime before the first `next()` to inject the stage `WriterId`.
    ///
    /// Default is a no-op for existing handlers that manage their own `WriterId`.
    fn bind_writer_id(&mut self, _id: WriterId) {}

    /// Pull zero or more events from the source.
    ///
    /// - `Ok(events)` means the source advanced; `events` may be empty or non-empty.
    /// - `Err(SourceError)` means polling the source failed.
    ///
    /// Infinite sources never complete naturally - they run until shutdown.
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError>;
}
