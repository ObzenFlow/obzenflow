// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concrete logging middleware implementation for testing and demonstration
//!
//! This provides a simple but real LoggingMiddleware that can be used to verify
//! that our middleware adapters work correctly.

use crate::middleware::{
    ErrorAction, Middleware, MiddlewareAction, MiddlewareContext, SourceMiddlewarePhase,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::{
    HandlerMiddlewareObserver, HandlerObserverContext, JoinMiddlewareObserver, JoinObserverContext,
    ObserverDeterminism, ObserverReport, SinkDeliveryObserver, SinkDeliveryObserverContext,
    SinkDeliveryObserverOutcome, SourcePollObserver, SourcePollObserverContext,
    StatefulMiddlewareObserver, StatefulObserverContext,
};
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

    fn log_processing(&self, event: &ChainEvent) {
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

    fn log_completed(&self, event: &ChainEvent, result_count: usize) {
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

    fn emit(&self, message: String) {
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

impl Middleware for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        MiddlewareAction::Continue
    }

    fn post_handle(
        &self,
        _event: &ChainEvent,
        _results: &[ChainEvent],
        _ctx: &mut MiddlewareContext,
    ) {
    }

    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        ErrorAction::Propagate
    }
}

impl HandlerMiddlewareObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) -> ObserverReport {
        self.log_processing(ctx.input);
        ObserverReport::empty()
    }

    fn after_handle(
        &self,
        ctx: &HandlerObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.log_completed(ctx.input, outputs.len());
        ObserverReport::empty()
    }
}

impl StatefulMiddlewareObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_state_accumulate(&self, ctx: &StatefulObserverContext<'_>) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.log_processing(input);
        }
        ObserverReport::empty()
    }

    fn after_state_emit(
        &self,
        ctx: &StatefulObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.log_completed(input, outputs.len());
        }
        ObserverReport::empty()
    }
}

impl JoinMiddlewareObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn before_join_input(&self, ctx: &JoinObserverContext<'_>) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.log_processing(input);
        }
        ObserverReport::empty()
    }

    fn after_join_output(
        &self,
        ctx: &JoinObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        if let Some(input) = ctx.input {
            self.log_completed(input, outputs.len());
        }
        ObserverReport::empty()
    }
}

impl SourcePollObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn after_source_poll(
        &self,
        _ctx: &SourcePollObserverContext<'_>,
        outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        if !outputs.is_empty() {
            self.events_processed.fetch_add(
                outputs.iter().filter(|event| event.is_data()).count(),
                Ordering::Relaxed,
            );
            self.emit(format!("Source poll produced {} events", outputs.len()));
        }
        ObserverReport::empty()
    }
}

impl SinkDeliveryObserver for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::LiveOnly
    }

    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) -> ObserverReport {
        self.log_processing(ctx.input);
        match &ctx.outcome {
            SinkDeliveryObserverOutcome::Delivered => {
                self.emit(format!("Sink delivered {}", ctx.input.id));
            }
            SinkDeliveryObserverOutcome::Failed { message } => {
                self.emit(format!(
                    "Sink delivery failed for {}: {}",
                    ctx.input.id, message
                ));
            }
            SinkDeliveryObserverOutcome::Rejected { reason } => {
                self.emit(format!(
                    "Sink delivery rejected for {}: {}",
                    ctx.input.id, reason
                ));
            }
        }
        ObserverReport::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope, StageType};
    use obzenflow_core::event::ChainEventFactory;
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

        let flow_context = FlowContext {
            flow_name: "test_flow".to_string(),
            flow_id: "flow_1".to_string(),
            stage_name: "test_stage".to_string(),
            stage_id: obzenflow_core::StageId::new(),
            stage_type: StageType::Transform,
        };
        let ctx = HandlerObserverContext {
            stage_id: flow_context.stage_id,
            stage_name: &flow_context.stage_name,
            flow_context: &flow_context,
            scope: MiddlewareExecutionScope::LiveHandler,
            input: &event,
            stage_input_position: Some(1),
        };
        HandlerMiddlewareObserver::before_handle(&middleware, &ctx);
        assert_eq!(middleware.events_processed(), 1);

        HandlerMiddlewareObserver::before_handle(&middleware, &ctx);
        assert_eq!(middleware.events_processed(), 2);
    }

    #[test]
    fn test_logging_middleware_is_live_only() {
        let middleware = LoggingMiddleware::default();
        assert_eq!(
            HandlerMiddlewareObserver::determinism(&middleware),
            ObserverDeterminism::LiveOnly
        );
    }

    #[test]
    fn test_logging_middleware_observes_sink_delivery() {
        let middleware = LoggingMiddleware::with_prefix("SINK WAZ HERE!");
        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test.event",
            json!({ "data": "test" }),
        );

        let ctx = SinkDeliveryObserverContext {
            stage_id: obzenflow_core::StageId::new(),
            stage_name: "test_sink",
            scope: MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
            input: &event,
            stage_input_position: Some(1),
            outcome: SinkDeliveryObserverOutcome::Delivered,
        };

        SinkDeliveryObserver::after_sink_delivery(&middleware, &ctx);
        assert_eq!(middleware.events_processed(), 1);
    }
}
