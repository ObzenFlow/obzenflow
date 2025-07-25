//! System enrichment middleware for adding flow context to events
//!
//! This middleware implements the wide events pattern by ensuring all events
//! have proper flow context before they're written to the journal.

use crate::middleware::{Middleware, MiddlewareAction, ErrorAction, MiddlewareContext, MiddlewareFactory};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::CorrelationId;
use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
use obzenflow_core::StageId;
use obzenflow_runtime_services::pipeline::config::StageConfig;

/// Middleware that ensures events have proper flow context
///
/// This is a core system middleware that should be added to all stages
/// to provide automatic flow and stage context enrichment.
#[derive(Debug, Clone)]
pub struct SystemEnrichmentMiddleware {
    flow_name: String,
    flow_id: String,
    stage_name: String,
    stage_id: StageId,
    stage_type: StageType,
}

impl SystemEnrichmentMiddleware {
    /// Create a new system enrichment middleware
    pub fn new(
        flow_name: impl Into<String>,
        flow_id: impl Into<String>,
        stage_name: impl Into<String>,
        stage_id: StageId,
        stage_type: StageType,
    ) -> Self {
        Self {
            flow_name: flow_name.into(),
            flow_id: flow_id.into(),
            stage_name: stage_name.into(),
            stage_id,
            stage_type,
        }
    }
}

impl Middleware for SystemEnrichmentMiddleware {
    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // No pre-processing needed for enrichment
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, _event: &ChainEvent, _results: &[ChainEvent], _ctx: &mut MiddlewareContext) {
        // No post-processing needed for enrichment
    }
    
    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        // Just propagate errors - enrichment middleware doesn't handle errors
        ErrorAction::Propagate
    }
    
    fn pre_write(&self, event: &mut ChainEvent, _ctx: &MiddlewareContext) {
        // Only enrich if the flow context is not already set or is default
        if event.flow_context.flow_name == "unknown" || event.flow_context.flow_name.is_empty() {
            event.flow_context = FlowContext {
                flow_name: self.flow_name.clone(),
                flow_id: self.flow_id.clone(),
                stage_name: self.stage_name.clone(),
                stage_id: self.stage_id.clone(),
                stage_type: self.stage_type,
            };
            
            tracing::trace!(
                "SystemEnrichmentMiddleware: Added flow context to event {} - flow: {}, stage: {}", 
                event.id,
                self.flow_name,
                self.stage_name
            );
        } else {
            // Event already has flow context, just ensure stage info is current
            // This handles events that flow between stages but need current stage context
            if event.flow_context.stage_name != self.stage_name {
                tracing::trace!(
                    "SystemEnrichmentMiddleware: Updated stage context for event {} from {} to {}", 
                    event.id,
                    event.flow_context.stage_name,
                    self.stage_name
                );
                event.flow_context.stage_name = self.stage_name.clone();
                event.flow_context.stage_id = self.stage_id.clone();
                event.flow_context.stage_type = self.stage_type;
            }
        }
        
        // Handle correlation IDs for journey tracking
        if !event.is_lifecycle() {  // Don't add correlation to lifecycle/control events
            match self.stage_type {
                StageType::FiniteSource | StageType::InfiniteSource => {
                    // Sources always generate new correlation IDs (journey start)
                    if event.correlation_id.is_none() {
                        let correlation_id = CorrelationId::new();
                        let mut correlation_payload = CorrelationPayload::new(
                            &self.stage_name,
                            event.id.clone()
                        );
                        
                        // Add flow metadata
                        correlation_payload.metadata = Some(serde_json::json!({
                            "flow_name": self.flow_name,
                            "flow_id": self.flow_id,
                            "source_event_id": event.id.to_string(),
                        }));
                        
                        event.correlation_id = Some(correlation_id.clone());
                        event.correlation_payload = Some(correlation_payload);
                        
                        tracing::trace!(
                            "SystemEnrichmentMiddleware: Generated correlation_id {} for source event {}", 
                            correlation_id,
                            event.id
                        );
                    }
                },
                StageType::Transform | StageType::Stateful | StageType::Sink => {
                    // stages (transform, stateful) and sinks preserve correlation IDs
                    // If somehow we get here without a correlation ID, log a warning
                    if event.correlation_id.is_none() && !event.is_lifecycle() {
                        tracing::warn!(
                            "SystemEnrichmentMiddleware: Non-control event {} in {} stage missing correlation_id", 
                            event.id,
                            self.stage_name
                        );
                        // We could generate one here, but that would break journey tracking
                        // Better to let it flow through and debug why it's missing
                    }
                }
            }
        }
        
        // Also enrich lifecycle events with proper context (but no correlation)
        if event.is_lifecycle() {
            // Control events should always have the context of the stage that generated them
            event.flow_context.stage_name = self.stage_name.clone();
            event.flow_context.stage_id = self.stage_id.clone();
            event.flow_context.stage_type = self.stage_type;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::{EventId, WriterId, StageId};
    use obzenflow_core::event::ChainEventFactory;
    use serde_json::json;
    
    #[test]
    fn test_enrichment_adds_flow_context() {
        let middleware = SystemEnrichmentMiddleware::new(
            "test_flow",
            "flow-123",
            "test_stage",
            StageId::new(),
            StageType::Transform
        );
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"data": "test"})
        );
        
        // Initially should have default/unknown context
        assert_eq!(event.flow_context.flow_name, "unknown");
        
        // Pre-write should add the flow context
        middleware.pre_write(&mut event, &ctx);
        
        // Check that flow context was added
        assert_eq!(event.flow_context.flow_name, "test_flow");
        assert_eq!(event.flow_context.flow_id, "flow-123");
        assert_eq!(event.flow_context.stage_name, "test_stage");
        assert_eq!(event.flow_context.stage_type, StageType::Transform);
    }
    
    #[test]
    fn test_source_generates_correlation_id() {
        let middleware = SystemEnrichmentMiddleware::new(
            "test_flow",
            "flow-123", 
            "http_source",
            StageId::new(),
            StageType::InfiniteSource
        );
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "http.request",
            json!({"path": "/api/test"})
        );
        
        // Initially no correlation ID
        assert!(event.correlation_id.is_none());
        
        // Pre-write should add correlation ID for source
        middleware.pre_write(&mut event, &ctx);
        
        // Check that correlation was added
        assert!(event.correlation_id.is_some());
        assert!(event.correlation_payload.is_some());
        
        let payload = event.correlation_payload.as_ref().unwrap();
        assert_eq!(payload.entry_stage, "http_source");
        assert_eq!(payload.metadata.as_ref().unwrap()["flow_name"], "test_flow");
        assert_eq!(payload.metadata.as_ref().unwrap()["flow_id"], "flow-123");
    }
    
    #[test]
    fn test_transform_preserves_correlation_id() {
        let middleware = SystemEnrichmentMiddleware::new(
            "test_flow",
            "flow-456",
            "validation",
            StageId::new(),
            StageType::Transform
        );
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "order.validate",
            json!({"order_id": "12345"})
        );
        
        // Set existing correlation
        let correlation_id = CorrelationId::new();
        event.correlation_id = Some(correlation_id.clone());
        
        // Pre-write should preserve correlation ID
        middleware.pre_write(&mut event, &ctx);
        
        // Correlation should be unchanged
        assert_eq!(event.correlation_id, Some(correlation_id));
    }
    
    #[test]
    fn test_control_events_no_correlation() {
        let middleware = SystemEnrichmentMiddleware::new(
            "test_flow",
            "flow-789",
            "rate_limiter_source",
            StageId::new(),
            StageType::FiniteSource  // Even sources shouldn't add correlation to control events
        );
        let ctx = MiddlewareContext::new();
        
        let writer_id = WriterId::from(StageId::new());
        let mut event = ChainEventFactory::metrics_state_snapshot(
            writer_id,
            json!({
                "middleware": "rate_limiter",
                "state": "throttled"
            })
        );
        
        // Pre-write should NOT add correlation to control events
        middleware.pre_write(&mut event, &ctx);
        
        // Control events should not have correlation
        assert!(event.correlation_id.is_none());
        assert!(event.correlation_payload.is_none());
    }
    
    #[test]
    fn test_sink_preserves_correlation_id() {
        let middleware = SystemEnrichmentMiddleware::new(
            "test_flow",
            "flow-999",
            "postgres_sink",
            StageId::new(),
            StageType::Sink
        );
        let ctx = MiddlewareContext::new();
        
        let mut event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "order.completed",
            json!({"order_id": "12345", "total": 99.99})
        );
        
        // Set existing correlation (from source)
        let correlation_id = CorrelationId::new();
        let mut correlation_payload = CorrelationPayload::new(
            "http_source",
            EventId::new()
        );
        correlation_payload.metadata = Some(json!({
            "flow_name": "test_flow",
            "source_event_id": "src-123"
        }));
        event.correlation_id = Some(correlation_id.clone());
        event.correlation_payload = Some(correlation_payload.clone());
        
        // Pre-write should preserve everything
        middleware.pre_write(&mut event, &ctx);
        
        // Correlation should be completely unchanged
        assert_eq!(event.correlation_id, Some(correlation_id));
        assert_eq!(event.correlation_payload.as_ref().unwrap().entry_stage, "http_source");
    }
}

/// Factory for creating SystemEnrichmentMiddleware instances with flow/stage context
pub struct SystemEnrichmentMiddlewareFactory {
    flow_name: String,
    flow_id: String,
    stage_type: StageType,
}

impl SystemEnrichmentMiddlewareFactory {
    pub fn new(flow_name: impl Into<String>, flow_id: impl Into<String>, stage_type: StageType) -> Self {
        Self {
            flow_name: flow_name.into(),
            flow_id: flow_id.into(),
            stage_type,
        }
    }
}

impl MiddlewareFactory for SystemEnrichmentMiddlewareFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(SystemEnrichmentMiddleware::new(
            &self.flow_name,
            &self.flow_id,
            &config.name,
            config.stage_id,
            self.stage_type,
        ))
    }
    
    fn name(&self) -> &str {
        "system_enrichment"
    }
}

/// Convenience function to create a system enrichment middleware factory
pub fn system_enrichment(flow_name: impl Into<String>, flow_id: impl Into<String>, stage_type: StageType) -> Box<dyn MiddlewareFactory> {
    Box::new(SystemEnrichmentMiddlewareFactory::new(flow_name, flow_id, stage_type))
}