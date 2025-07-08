//! Outcome enrichment middleware for setting ProcessingOutcome based on event characteristics
//!
//! This middleware implements the wide events pattern by detecting error conditions
//! and setting the appropriate ProcessingOutcome before events are written to the journal.

use super::{Middleware, MiddlewareAction, ErrorAction, MiddlewareContext, MiddlewareFactory};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::processing_outcome::ProcessingOutcome;
use obzenflow_runtime_services::pipeline::config::StageConfig;

/// Middleware that detects error conditions and sets ProcessingOutcome
///
/// This is a core system middleware that should be added to all stages
/// to provide automatic error detection and outcome classification.
/// 
/// Detection rules:
/// - Event type contains "error" → ProcessingOutcome::Error
/// - Event type contains "failed" or "failure" → ProcessingOutcome::Error
/// - Payload contains "_error": true → ProcessingOutcome::Error
/// - Payload contains "error" field with string value → ProcessingOutcome::Error(msg)
/// - Context contains retry information → ProcessingOutcome::Retry
/// - Otherwise → ProcessingOutcome::Success (default)
#[derive(Debug, Clone)]
pub struct OutcomeEnrichmentMiddleware {
    stage_name: String,
}

impl OutcomeEnrichmentMiddleware {
    /// Create a new outcome enrichment middleware
    pub fn new(stage_name: impl Into<String>) -> Self {
        Self {
            stage_name: stage_name.into(),
        }
    }
    
    /// Detect if this event represents an error based on its characteristics
    fn detect_outcome(&self, event: &ChainEvent, ctx: &MiddlewareContext) -> ProcessingOutcome {
        // Check event type for error indicators
        let event_type_lower = event.event_type.to_lowercase();
        if event_type_lower.contains("error") || 
           event_type_lower.contains("failed") || 
           event_type_lower.contains("failure") {
            // Try to extract error message from payload
            if let Some(error_msg) = event.payload.get("error").and_then(|v| v.as_str()) {
                return ProcessingOutcome::Error(error_msg.to_string());
            } else if let Some(msg) = event.payload.get("message").and_then(|v| v.as_str()) {
                return ProcessingOutcome::Error(msg.to_string());
            } else {
                return ProcessingOutcome::Error(format!("Error in {}: {}", self.stage_name, event.event_type));
            }
        }
        
        // Check payload for error indicators
        if let Some(is_error) = event.payload.get("_error").and_then(|v| v.as_bool()) {
            if is_error {
                let msg = event.payload.get("_error_message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Processing error");
                return ProcessingOutcome::Error(msg.to_string());
            }
        }
        
        // Check for explicit error field in payload
        if let Some(error_value) = event.payload.get("error") {
            if error_value.is_string() {
                return ProcessingOutcome::Error(error_value.as_str().unwrap_or("Unknown error").to_string());
            } else if error_value.is_object() {
                // Handle error objects with message field
                if let Some(msg) = error_value.get("message").and_then(|v| v.as_str()) {
                    return ProcessingOutcome::Error(msg.to_string());
                }
            }
        }
        
        // Check context for retry information
        if let Some(retry_info) = ctx.get_baggage("retry_attempt") {
            if let Some(attempt) = retry_info.as_u64() {
                return ProcessingOutcome::Retry { attempt: attempt as u32 };
            }
        }
        
        // Check if this is a control event that was filtered
        if event.event_type.starts_with("control.") && 
           event.event_type != "control.metrics.state" &&
           event.event_type != "control.middleware.state" {
            // Most control events should be marked as filtered unless they're metrics
            return ProcessingOutcome::Filtered;
        }
        
        // Default to success if no error indicators found
        ProcessingOutcome::Success
    }
}

impl Middleware for OutcomeEnrichmentMiddleware {
    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // No pre-processing needed for outcome enrichment
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, _event: &ChainEvent, _results: &[ChainEvent], _ctx: &mut MiddlewareContext) {
        // No post-processing needed for outcome enrichment
    }
    
    fn on_error(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> ErrorAction {
        // When an actual error occurs during processing, mark it in context
        // This will be picked up in pre_write for any recovery events
        ctx.set_baggage("processing_failed", serde_json::json!(true));
        ctx.set_baggage("failed_event_type", serde_json::json!(event.event_type.clone()));
        
        // Let the error propagate - other middleware might handle it
        ErrorAction::Propagate
    }
    
    fn pre_write(&self, event: &mut ChainEvent, ctx: &MiddlewareContext) {
        // Only set outcome if it's still at default (Success)
        if matches!(event.processing_info.outcome, ProcessingOutcome::Success) {
            let detected_outcome = self.detect_outcome(event, ctx);
            
            // Update the outcome
            event.processing_info.outcome = detected_outcome.clone();
            
            // Also update processed_by if we detected an error
            if matches!(detected_outcome, ProcessingOutcome::Error(_)) {
                event.processing_info.processed_by = self.stage_name.clone();
            }
            
            tracing::trace!(
                "OutcomeEnrichmentMiddleware: Set outcome {:?} for event {} (type: {})", 
                detected_outcome,
                event.id,
                event.event_type
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::{EventId, WriterId};
    use serde_json::json;
    
    #[test]
    fn test_detects_error_from_event_type() {
        let middleware = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "validation.error",
            json!({"data": "invalid"})
        );
        
        middleware.pre_write(&mut event, &ctx);
        
        match &event.processing_info.outcome {
            ProcessingOutcome::Error(msg) => {
                assert!(msg.contains("test_stage"));
                assert!(msg.contains("validation.error"));
            }
            _ => panic!("Expected Error outcome"),
        }
    }
    
    #[test]
    fn test_detects_error_from_payload_flag() {
        let middleware = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "order.processed",
            json!({
                "order_id": "12345",
                "_error": true,
                "_error_message": "Insufficient funds"
            })
        );
        
        middleware.pre_write(&mut event, &ctx);
        
        match &event.processing_info.outcome {
            ProcessingOutcome::Error(msg) => {
                assert_eq!(msg, "Insufficient funds");
            }
            _ => panic!("Expected Error outcome"),
        }
    }
    
    #[test]
    fn test_detects_error_from_error_field() {
        let middleware = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "payment.processed",
            json!({
                "payment_id": "pay_123",
                "error": "Card declined"
            })
        );
        
        middleware.pre_write(&mut event, &ctx);
        
        match &event.processing_info.outcome {
            ProcessingOutcome::Error(msg) => {
                assert_eq!(msg, "Card declined");
            }
            _ => panic!("Expected Error outcome"),
        }
    }
    
    #[test]
    fn test_detects_retry_from_context() {
        let middleware = OutcomeEnrichmentMiddleware::new("test_stage");
        let mut ctx = MiddlewareContext::new();
        ctx.set_baggage("retry_attempt", json!(3));
        
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "order.processing",
            json!({"order_id": "12345"})
        );
        
        middleware.pre_write(&mut event, &ctx);
        
        match event.processing_info.outcome {
            ProcessingOutcome::Retry { attempt } => {
                assert_eq!(attempt, 3);
            }
            _ => panic!("Expected Retry outcome"),
        }
    }
    
    #[test]
    fn test_marks_control_events_as_filtered() {
        let middleware = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "control.eof",
            json!({})
        );
        
        middleware.pre_write(&mut event, &ctx);
        
        assert!(matches!(event.processing_info.outcome, ProcessingOutcome::Filtered));
    }
    
    #[test]
    fn test_preserves_existing_outcome() {
        let middleware = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "error.validation",
            json!({"data": "invalid"})
        );
        
        // Pre-set an outcome
        event.processing_info.outcome = ProcessingOutcome::Retry { attempt: 2 };
        
        middleware.pre_write(&mut event, &ctx);
        
        // Should not override existing outcome
        match event.processing_info.outcome {
            ProcessingOutcome::Retry { attempt } => {
                assert_eq!(attempt, 2);
            }
            _ => panic!("Expected original Retry outcome to be preserved"),
        }
    }
    
    #[test]
    fn test_leaves_success_events_unchanged() {
        let middleware = OutcomeEnrichmentMiddleware::new("test_stage");
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "order.created",
            json!({"order_id": "12345", "amount": 99.99})
        );
        
        middleware.pre_write(&mut event, &ctx);
        
        assert!(matches!(event.processing_info.outcome, ProcessingOutcome::Success));
    }
}

/// Factory for creating OutcomeEnrichmentMiddleware instances with stage context
pub struct OutcomeEnrichmentMiddlewareFactory;

impl OutcomeEnrichmentMiddlewareFactory {
    pub fn new() -> Self {
        Self
    }
}

impl MiddlewareFactory for OutcomeEnrichmentMiddlewareFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(OutcomeEnrichmentMiddleware::new(&config.name))
    }
    
    fn name(&self) -> &str {
        "outcome_enrichment"
    }
}

/// Convenience function to create an outcome enrichment middleware factory
pub fn outcome_enrichment() -> Box<dyn MiddlewareFactory> {
    Box::new(OutcomeEnrichmentMiddlewareFactory::new())
}