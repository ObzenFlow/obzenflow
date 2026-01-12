//! Timing middleware for measuring stage processing time
//!
//! This middleware implements the wide events pattern by enriching events
//! with processing duration before they're written to the journal.

use crate::middleware::{
    ErrorAction, Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::time::MetricsDuration;
use obzenflow_runtime_services::pipeline::config::StageConfig;
use std::time::{SystemTime, UNIX_EPOCH};

/// Middleware that measures processing time and adds it to events
///
/// This is a core system middleware that should be added to all stages
/// to provide automatic timing instrumentation.
#[derive(Debug, Clone)]
pub struct TimingMiddleware {
    stage_name: String,
}

impl TimingMiddleware {
    /// Create a new timing middleware for a specific stage
    pub fn new(stage_name: impl Into<String>) -> Self {
        Self {
            stage_name: stage_name.into(),
        }
    }
}

impl Middleware for TimingMiddleware {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Record the start time in the context as nanoseconds since epoch
        let start_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        ctx.set_baggage("processing_start_nanos", serde_json::json!(start_nanos));

        tracing::trace!(
            "TimingMiddleware[{}]: pre_handle for event {} at {}ns",
            self.stage_name,
            event.id,
            start_nanos
        );

        MiddlewareAction::Continue
    }

    fn post_handle(
        &self,
        _event: &ChainEvent,
        _results: &[ChainEvent],
        _ctx: &mut MiddlewareContext,
    ) {
        // Post-handle doesn't need to do anything - timing is added in pre_write
    }

    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        // Just propagate errors - timing middleware doesn't handle errors
        ErrorAction::Propagate
    }

    fn pre_write(&self, event: &mut ChainEvent, ctx: &MiddlewareContext) {
        // Calculate processing time and add to event
        if let Some(start_value) = ctx.get_baggage("processing_start_nanos") {
            if let Some(start_nanos) = start_value.as_u64() {
                let now_nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let duration_nanos = now_nanos - start_nanos;
                let duration = MetricsDuration::from_nanos(duration_nanos);

                // Store duration
                event.processing_info.processing_time = duration;

                tracing::debug!(
                    "TimingMiddleware[{}]: Set processing_time={} for event {}",
                    self.stage_name,
                    duration,
                    event.id,
                );

                // Log warning if timing seems too low for processor stage
                if self.stage_name.contains("processor") && duration.as_nanos() < 5_000_000 {
                    tracing::debug!(
                        "TimingMiddleware: Processor timing seems too low: {} for event {}",
                        duration,
                        event.id
                    );
                }
            } else {
                tracing::debug!(
                    "TimingMiddleware: Could not parse start time from baggage for event {}",
                    event.id
                );
            }
        } else {
            tracing::debug!(
                "TimingMiddleware: No processing start time found for event {}",
                event.id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{EventId, StageId, WriterId};
    use serde_json::json;
    use std::thread;

    #[test]
    fn test_timing_middleware_adds_processing_time() {
        let middleware = TimingMiddleware::new("test_stage");
        let mut ctx = MiddlewareContext::new();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"data": "test"}),
        );

        // Pre-handle starts the timer
        let action = middleware.pre_handle(&event, &mut ctx);
        assert!(matches!(action, MiddlewareAction::Continue));

        // Verify start time was recorded
        assert!(ctx.get_baggage("processing_start_nanos").is_some());

        // Simulate some processing time
        thread::sleep(MetricsDuration::from_millis(10).to_std());

        // Pre-write should add the timing
        let mut result_event = event.clone();
        middleware.pre_write(&mut result_event, &ctx);

        // Check that processing time was added (in nanoseconds)
        assert!(result_event.processing_info.processing_time.as_nanos() >= 9_000_000);
        // Allow for timing variance
    }

    #[test]
    fn test_timing_middleware_handles_missing_start_time() {
        let middleware = TimingMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new(); // No start time set

        let mut event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"data": "test"}),
        );

        // Pre-write should handle missing start time gracefully
        middleware.pre_write(&mut event, &ctx);

        // Processing time should remain at default (ZERO)
        assert_eq!(event.processing_info.processing_time, MetricsDuration::ZERO);
    }
}

/// Factory for creating TimingMiddleware instances with stage context
pub struct TimingMiddlewareFactory;

impl TimingMiddlewareFactory {
    pub fn new() -> Self {
        Self
    }
}

impl MiddlewareFactory for TimingMiddlewareFactory {
    fn create(
        &self,
        config: &StageConfig,
        _control_middleware: std::sync::Arc<
            crate::middleware::control::ControlMiddlewareAggregator,
        >,
    ) -> Box<dyn Middleware> {
        Box::new(TimingMiddleware::new(&config.name))
    }

    fn name(&self) -> &str {
        "timing"
    }
}

/// Convenience function to create a timing middleware factory
pub fn timing() -> Box<dyn MiddlewareFactory> {
    Box::new(TimingMiddlewareFactory::new())
}
