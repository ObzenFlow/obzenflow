//! Outcome enrichment middleware for setting ProcessingOutcome based on event characteristics.

use crate::middleware::{
    ErrorAction, Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_runtime_services::pipeline::config::StageConfig;
use serde_json::Value;

/// Middleware that detects error conditions and sets ProcessingOutcome.
///
/// Detection rules:
/// - Event type contains "error"/"failed"/"failure" → `Error`
/// - Payload has `_error: true` → `Error` (uses `_error_message` if present)
/// - Payload has `error` (string or object with `message`) → `Error`
/// - Middleware baggage has `retry_attempt` → `Retry{attempt}`
/// - Control events → `Filtered`
/// - Otherwise `Success`
#[derive(Debug, Clone)]
pub struct OutcomeEnrichmentMiddleware {
    stage_name: String,
}

impl OutcomeEnrichmentMiddleware {
    pub fn new(stage_name: impl Into<String>) -> Self {
        Self {
            stage_name: stage_name.into(),
        }
    }

    fn detect_outcome(&self, event: &ChainEvent, ctx: &MiddlewareContext) -> ProcessingStatus {
        let etype = event.event_type().to_lowercase();
        let payload: Value = event.payload(); // clone, fine for middleware

        // 1) event type keywords
        if etype.contains("error") || etype.contains("failed") || etype.contains("failure") {
            if let Some(msg) = payload
                .get("error")
                .and_then(|v| v.as_str())
                .or_else(|| payload.get("message").and_then(|v| v.as_str()))
            {
                return ProcessingStatus::error(msg.to_string());
            }
            return ProcessingStatus::error(format!(
                "Error in {}: {}",
                self.stage_name,
                event.event_type()
            ));
        }

        // 2) payload _error flag
        if payload
            .get("_error")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let msg = payload
                .get("_error_message")
                .and_then(|v| v.as_str())
                .unwrap_or("Processing error");
            return ProcessingStatus::error(msg.to_string());
        }

        // 3) explicit `error` field
        if let Some(err) = payload.get("error") {
            if let Some(s) = err.as_str() {
                return ProcessingStatus::error(s.to_string());
            }
            if let Some(msg) = err.get("message").and_then(|v| v.as_str()) {
                return ProcessingStatus::error(msg.to_string());
            }
        }

        // 4) retry baggage
        if let Some(retry_info) = ctx.get_baggage("retry_attempt") {
            if let Some(attempt) = retry_info.as_u64() {
                return ProcessingStatus::Retry {
                    attempt: attempt as u32,
                };
            }
        }

        // 5) control events → filtered
        if event.is_control() {
            return ProcessingStatus::Filtered;
        }

        ProcessingStatus::Success
    }
}

impl Middleware for OutcomeEnrichmentMiddleware {
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

    fn on_error(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> ErrorAction {
        ctx.set_baggage("processing_failed", serde_json::json!(true));
        ctx.set_baggage("failed_event_type", serde_json::json!(event.event_type()));
        ErrorAction::Propagate
    }

    fn pre_write(&self, event: &mut ChainEvent, ctx: &MiddlewareContext) {
        // Only classify if it's still the default (Success)
        if matches!(event.processing_info.status, ProcessingStatus::Success) {
            let detected = self.detect_outcome(event, ctx);
            event.processing_info.status = detected.clone();

            if matches!(detected, ProcessingStatus::Error { .. }) {
                event.processing_info.processed_by = self.stage_name.clone();
            }

            tracing::trace!(
                stage = %self.stage_name,
                event_id = %event.id,
                event_type = %event.event_type(),
                outcome = ?detected,
                "OutcomeEnrichmentMiddleware set outcome"
            );
        }
    }
}

// ==========================================================================
// Factory
// ==========================================================================
pub struct OutcomeEnrichmentMiddlewareFactory;

impl OutcomeEnrichmentMiddlewareFactory {
    pub fn new() -> Self {
        Self
    }
}

impl MiddlewareFactory for OutcomeEnrichmentMiddlewareFactory {
    fn create(
        &self,
        config: &StageConfig,
        _control_middleware: std::sync::Arc<
            crate::middleware::control::ControlMiddlewareAggregator,
        >,
    ) -> Box<dyn Middleware> {
        Box::new(OutcomeEnrichmentMiddleware::new(&config.name))
    }

    fn name(&self) -> &str {
        "outcome_enrichment"
    }
}

pub fn outcome_enrichment() -> Box<dyn MiddlewareFactory> {
    Box::new(OutcomeEnrichmentMiddlewareFactory::new())
}

// ==========================================================================
// Tests
// ==========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use obzenflow_core::WriterId;
    use serde_json::json;

    fn writer() -> WriterId {
        WriterId::from(StageId::new())
    }

    #[test]
    fn detects_error_from_event_type() {
        let mw = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();

        let mut event =
            ChainEventFactory::data_event(writer(), "validation.error", json!({"data": "invalid"}));
        mw.pre_write(&mut event, &ctx);

        match &event.processing_info.status {
            ProcessingStatus::Error { message: msg, .. } => {
                assert!(msg.contains("test_stage"));
                assert!(msg.contains("validation.error"));
            }
            _ => panic!("Expected Error outcome"),
        }
    }

    #[test]
    fn detects_error_from_payload_flag() {
        let mw = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();

        let mut event = ChainEventFactory::data_event(
            writer(),
            "order.processed",
            json!({"order_id":"12345","_error":true,"_error_message":"Insufficient funds"}),
        );

        mw.pre_write(&mut event, &ctx);

        match &event.processing_info.status {
            ProcessingStatus::Error { message: msg, .. } => assert_eq!(msg, "Insufficient funds"),
            _ => panic!("Expected Error outcome"),
        }
    }

    #[test]
    fn detects_error_from_error_field() {
        let mw = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();

        let mut event = ChainEventFactory::data_event(
            writer(),
            "payment.processed",
            json!({"payment_id":"pay_123","error":"Card declined"}),
        );

        mw.pre_write(&mut event, &ctx);

        match &event.processing_info.status {
            ProcessingStatus::Error { message: msg, .. } => assert_eq!(msg, "Card declined"),
            _ => panic!("Expected Error outcome"),
        }
    }

    #[test]
    fn detects_retry_from_context() {
        let mw = OutcomeEnrichmentMiddleware::new("test_stage");
        let mut ctx = MiddlewareContext::new();
        ctx.set_baggage("retry_attempt", json!(3));

        let mut event = ChainEventFactory::data_event(
            writer(),
            "order.processing",
            json!({"order_id": "12345"}),
        );

        mw.pre_write(&mut event, &ctx);

        match event.processing_info.status {
            ProcessingStatus::Retry { attempt } => assert_eq!(attempt, 3),
            _ => panic!("Expected Retry outcome"),
        }
    }

    #[test]
    fn marks_control_events_as_filtered() {
        let mw = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();

        let mut event = ChainEventFactory::eof_event(writer(), true);

        mw.pre_write(&mut event, &ctx);

        assert!(matches!(
            event.processing_info.status,
            ProcessingStatus::Filtered
        ));
    }

    #[test]
    fn preserves_existing_outcome() {
        let mw = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();

        let mut event =
            ChainEventFactory::data_event(writer(), "error.validation", json!({"data": "invalid"}));

        // Pre-set an outcome
        event.processing_info.status = ProcessingStatus::Retry { attempt: 2 };

        mw.pre_write(&mut event, &ctx);

        match event.processing_info.status {
            ProcessingStatus::Retry { attempt } => assert_eq!(attempt, 2),
            _ => panic!("Expected original Retry outcome to be preserved"),
        }
    }

    #[test]
    fn leaves_success_events_unchanged() {
        let mw = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();

        let mut event = ChainEventFactory::data_event(
            writer(),
            "order.created",
            json!({"order_id": "12345", "amount": 99.99}),
        );

        mw.pre_write(&mut event, &ctx);

        assert!(matches!(
            event.processing_info.status,
            ProcessingStatus::Success
        ));
    }
}
