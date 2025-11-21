//! Concrete logging middleware implementation for testing and demonstration
//!
//! This provides a simple but real LoggingMiddleware that can be used to verify
//! that our middleware adapters work correctly.

use crate::middleware::{
    ErrorAction, Middleware, MiddlewareAction, MiddlewareContext, MiddlewareSink,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_runtime_services::stages::SinkHandler;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A concrete logging middleware that logs event processing
pub struct LoggingMiddleware {
    /// Optional prefix for log messages
    prefix: Option<String>,
    /// Counter for events processed
    events_processed: Arc<AtomicUsize>,
    /// Log level to use
    level: tracing::Level,
}

impl LoggingMiddleware {
    /// Create a new logging middleware with default INFO level
    pub fn new() -> Self {
        Self {
            prefix: None,
            events_processed: Arc::new(AtomicUsize::new(0)),
            level: tracing::Level::INFO,
        }
    }

    /// Create with a custom prefix (like "SINK WAZ HERE!")
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: Some(prefix.into()),
            events_processed: Arc::new(AtomicUsize::new(0)),
            level: tracing::Level::INFO,
        }
    }

    /// Set the log level
    pub fn with_level(mut self, level: tracing::Level) -> Self {
        self.level = level;
        self
    }

    /// Get the count of events processed
    pub fn events_processed(&self) -> usize {
        self.events_processed.load(Ordering::Relaxed)
    }
}

impl Default for LoggingMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

impl Middleware for LoggingMiddleware {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
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

        match self.level {
            tracing::Level::TRACE => tracing::trace!("{}", message),
            tracing::Level::DEBUG => tracing::debug!("{}", message),
            tracing::Level::INFO => tracing::info!("{}", message),
            tracing::Level::WARN => tracing::warn!("{}", message),
            tracing::Level::ERROR => tracing::error!("{}", message),
        }

        // Emit a logging event
        ctx.emit_event(
            "logging",
            "event_processed",
            serde_json::json!({
                "event_id": event.id.as_str(),
                "event_type": event.event_type(),
                "count": count
            }),
        );

        MiddlewareAction::Continue
    }

    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        let message = if let Some(prefix) = &self.prefix {
            format!(
                "{} - Completed processing {}, produced {} results",
                prefix,
                event.id,
                results.len()
            )
        } else {
            format!(
                "Completed processing {}, produced {} results",
                event.id,
                results.len()
            )
        };

        match self.level {
            tracing::Level::TRACE => tracing::trace!("{}", message),
            tracing::Level::DEBUG => tracing::debug!("{}", message),
            tracing::Level::INFO => tracing::info!("{}", message),
            tracing::Level::WARN => tracing::warn!("{}", message),
            tracing::Level::ERROR => tracing::error!("{}", message),
        }

        // Emit completion event
        ctx.emit_event(
            "logging",
            "processing_completed",
            serde_json::json!({
                "event_id": event.id.as_str(),
                "results_count": results.len()
            }),
        );
    }

    fn on_error(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> ErrorAction {
        let message = if let Some(prefix) = &self.prefix {
            format!("{} - Error processing {}", prefix, event.id)
        } else {
            format!("Error processing {}", event.id)
        };

        tracing::error!("{}", message);

        // Emit error event
        ctx.emit_event(
            "logging",
            "processing_error",
            serde_json::json!({
                "event_id": event.id.as_str()
            }),
        );

        // Just log and propagate - don't interfere with error handling
        ErrorAction::Propagate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
    use serde_json::json;

    #[test]
    fn test_logging_middleware_counts_events() {
        let middleware = LoggingMiddleware::with_prefix("TEST");

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test.event",
            json!({ "data": "test" }),
        );

        assert_eq!(middleware.events_processed(), 0);

        let mut ctx = MiddlewareContext::new();
        middleware.pre_handle(&event, &mut ctx);
        assert_eq!(middleware.events_processed(), 1);

        middleware.pre_handle(&event, &mut ctx);
        assert_eq!(middleware.events_processed(), 2);
    }

    #[tokio::test] // use Tokio or any async‑test runtime
    async fn test_logging_middleware_with_sink() {
        use crate::middleware::SinkHandlerExt;

        /// Simple in‑memory sink for testing
        struct TestSink {
            consumed: Vec<ChainEvent>,
        }

        impl TestSink {
            fn consume_count(&self) -> usize {
                self.consumed.len()
            }
        }

        #[async_trait]
        impl SinkHandler for TestSink {
            async fn consume(
                &mut self,
                event: ChainEvent,
            ) -> obzenflow_core::Result<DeliveryPayload> {
                self.consumed.push(event);
                Ok(DeliveryPayload::success(
                    "test_sink",          // destination
                    DeliveryMethod::Noop, // no real I/O
                    None,                 // bytes_processed
                ))
            }
        }

        // Build sink + logging middleware
        let mut sink = TestSink { consumed: vec![] }
            .middleware()
            .with(LoggingMiddleware::with_prefix("SINK WAZ HERE!"))
            .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test.event",
            json!({ "data": "test" }),
        );

        // Should log and succeed
        sink.consume(event).await.unwrap();
        assert_eq!(sink.inner().consume_count(), 1);
    }
}
