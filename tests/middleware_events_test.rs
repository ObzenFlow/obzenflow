//! Tests for middleware event propagation
//!
//! Demonstrates how operational middleware emits raw events that can be
//! consumed by monitoring, SLI, and logging middleware.

use obzenflow_core::event::{
    processing_info::ProcessingInfo,
    middleware_event::MiddlewareEvent,
    processing_outcome::ProcessingOutcome,
};
use serde_json::json;

#[test]
fn test_middleware_event_propagation() {
    // Start with default processing info
    let mut info = ProcessingInfo::default();
    
    // Simulate circuit breaker rejecting a request
    info.add_middleware_event(MiddlewareEvent::new(
        "circuit_breaker",
        "rejected",
        json!({
            "consecutive_failures": 10,
            "threshold": 10,
            "stage": "payment_processor"
        })
    ));
    
    // Simulate retry middleware attempting
    info.add_middleware_event(MiddlewareEvent::new(
        "retry",
        "attempt_started",
        json!({
            "attempt": 1,
            "max_attempts": 3
        })
    ));
    
    // Test helper methods
    assert!(info.has_middleware_event("circuit_breaker", "rejected"));
    assert!(info.has_middleware_event("retry", "attempt_started"));
    assert!(!info.has_middleware_event("rate_limiter", "throttled"));
    
    // Get all events from circuit breaker
    let cb_events = info.get_middleware_events_from("circuit_breaker");
    assert_eq!(cb_events.len(), 1);
    assert_eq!(cb_events[0].event_type, "rejected");
    
    // Find specific event
    let rejected = info.find_middleware_event("circuit_breaker", "rejected").unwrap();
    assert_eq!(rejected.data["consecutive_failures"], 10);
}

#[test]
fn test_monitoring_observes_operational_events() {
    let mut info = ProcessingInfo {
        processed_by: "payment_processor".to_string(),
        processing_time_ms: 150,
        event_time: 1234567890,
        outcome: ProcessingOutcome::Error("Payment failed".to_string()),
        middleware_events: vec![],
    };
    
    // Operational middleware emits events
    info.add_middleware_event(MiddlewareEvent::new(
        "circuit_breaker",
        "opened",
        json!({
            "failure_rate": 0.15,
            "threshold": 0.10
        })
    ));
    
    info.add_middleware_event(MiddlewareEvent::new(
        "retry",
        "exhausted",
        json!({
            "attempts": 3,
            "last_error": "Connection timeout"
        })
    ));
    
    // Monitoring middleware would observe these events
    // and update RED metrics accordingly:
    // - Errors: increment due to circuit breaker open + retry exhausted
    // - Duration: record the 150ms processing time
    // - Rate: track the request
    
    // SLI middleware would compute availability
    let has_circuit_breaker_rejection = info.has_middleware_event("circuit_breaker", "opened");
    let has_retry_exhaustion = info.has_middleware_event("retry", "exhausted");
    let availability = if has_circuit_breaker_rejection || has_retry_exhaustion { 
        0.0 
    } else { 
        1.0 
    };
    
    // SLI middleware would emit its own event
    info.add_middleware_event(MiddlewareEvent::new(
        "sli",
        "availability",
        json!({
            "indicator": "payment_availability",
            "value": availability,
            "window": "5m"
        })
    ));
    
    // SLO tracker would then observe the SLI event
    let sli_event = info.find_middleware_event("sli", "availability").unwrap();
    assert_eq!(sli_event.data["value"], 0.0);
}

#[test]
fn test_clean_separation_of_concerns() {
    let mut info = ProcessingInfo::default();
    
    // Layer 1: Operational (changes behavior)
    info.add_middleware_event(MiddlewareEvent::new(
        "rate_limiter",
        "tokens_exhausted",
        json!({
            "tokens_per_second": 100,
            "burst_size": 200
        })
    ));
    
    // Layer 2: SLI computation (computes indicators)
    info.add_middleware_event(MiddlewareEvent::new(
        "sli",
        "throughput",
        json!({
            "indicator": "api_throughput",
            "value": 0.0,  // No throughput when rate limited
            "unit": "requests_per_second"
        })
    ));
    
    // Layer 3: Monitoring would track this in RED/USE metrics
    // Layer 4: SLO tracking would check if throughput SLO is violated
    // Layer 5: Logging would capture all events for debugging
    
    // Verify clean separation - each layer has its own events
    assert_eq!(info.get_middleware_events_from("rate_limiter").len(), 1);
    assert_eq!(info.get_middleware_events_from("sli").len(), 1);
}

#[test]
fn test_event_serialization_skips_empty() {
    // ProcessingInfo with no middleware events
    let info1 = ProcessingInfo {
        processed_by: "test".to_string(),
        processing_time_ms: 10,
        event_time: 123456,
        outcome: ProcessingOutcome::Success,
        middleware_events: vec![],
    };
    
    // Should not include middleware_events in JSON when empty
    let json1 = serde_json::to_string(&info1).unwrap();
    assert!(!json1.contains("middleware_events"));
    
    // ProcessingInfo with middleware events
    let mut info2 = info1.clone();
    info2.add_middleware_event(MiddlewareEvent::simple("test", "event"));
    
    // Should include middleware_events when present
    let json2 = serde_json::to_string(&info2).unwrap();
    assert!(json2.contains("middleware_events"));
}