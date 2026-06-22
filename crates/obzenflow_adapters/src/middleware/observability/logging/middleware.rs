// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::chain_event::ChainEvent;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Logs event processing from observe-only middleware hooks.
pub struct LoggingMiddleware {
    prefix: Option<String>,
    events_processed: Arc<AtomicUsize>,
    level: tracing::Level,
}

impl LoggingMiddleware {
    /// Create a new logging middleware with default INFO level.
    pub fn new() -> Self {
        Self {
            prefix: None,
            events_processed: Arc::new(AtomicUsize::new(0)),
            level: tracing::Level::INFO,
        }
    }

    /// Create with a custom prefix.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: Some(prefix.into()),
            events_processed: Arc::new(AtomicUsize::new(0)),
            level: tracing::Level::INFO,
        }
    }

    /// Set the log level.
    pub fn with_level(mut self, level: tracing::Level) -> Self {
        self.level = level;
        self
    }

    /// Get the count of events processed.
    pub fn events_processed(&self) -> usize {
        self.events_processed.load(Ordering::Relaxed)
    }

    pub(super) fn add_processed(&self, count: usize) {
        self.events_processed.fetch_add(count, Ordering::Relaxed);
    }

    pub(super) fn log_processing(&self, event: &ChainEvent) {
        let count = self.events_processed.fetch_add(1, Ordering::Relaxed) + 1;

        let message = if let Some(prefix) = &self.prefix {
            format!(
                "{} - Processing event #{}: {} ({})",
                prefix,
                count,
                event.id,
                event.event_type()
            )
        } else {
            format!(
                "Processing event #{}: {} ({})",
                count,
                event.id,
                event.event_type()
            )
        };

        self.emit(message);
    }

    pub(super) fn log_completed(&self, event: &ChainEvent, result_count: usize) {
        let message = if let Some(prefix) = &self.prefix {
            format!(
                "{} - Completed processing {}, produced {} results",
                prefix, event.id, result_count
            )
        } else {
            format!(
                "Completed processing {}, produced {} results",
                event.id, result_count
            )
        };

        self.emit(message);
    }

    pub(super) fn emit(&self, message: String) {
        match self.level {
            tracing::Level::TRACE => tracing::trace!("{}", message),
            tracing::Level::DEBUG => tracing::debug!("{}", message),
            tracing::Level::INFO => tracing::info!("{}", message),
            tracing::Level::WARN => tracing::warn!("{}", message),
            tracing::Level::ERROR => tracing::error!("{}", message),
        }
    }
}

impl Default for LoggingMiddleware {
    fn default() -> Self {
        Self::new()
    }
}
