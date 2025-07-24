//! Middleware context for event propagation
//!
//! This module provides a separate context that flows through middleware
//! during event processing, preserving the immutability of ChainEvent.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use obzenflow_core::ChainEvent;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::WriterId;

/// Ephemeral event emitted by middleware during processing
///
/// These events enable middleware composition where outer middleware
/// can communicate with inner middleware layers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MiddlewareEvent {
    /// The source middleware that emitted this event
    /// Examples: "circuit_breaker", "retry", "rate_limiter"
    pub source: String,

    /// The type of event emitted by the middleware
    /// Examples: "opened", "closed", "rejected", "attempt_started"
    pub event_type: String,

    /// Additional structured data about the event
    pub data: Value,
}

impl MiddlewareEvent {
    /// Create a new middleware event
    pub fn new(source: impl Into<String>, event_type: impl Into<String>, data: Value) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            data,
        }
    }

    /// Create a simple middleware event without additional data
    pub fn simple(source: impl Into<String>, event_type: impl Into<String>) -> Self {
        Self::new(source, event_type, Value::Null)
    }
}

/// Ephemeral context that flows through middleware during processing
///
/// This context is created fresh for each event processing and is never
/// persisted. It enables middleware to communicate without mutating the
/// core ChainEvent.
#[derive(Debug, Default, Clone)]
pub struct MiddlewareContext {
    /// Events emitted by middleware for inner layers to observe
    pub events: Vec<MiddlewareEvent>,

    /// Key-value baggage for middleware state
    pub baggage: HashMap<String, Value>,

    /// Control events to be written to the journal after processing
    /// These are collected during middleware execution and appended by MiddlewareTransform
    pub control_events: Vec<ChainEvent>,
}

impl MiddlewareContext {
    /// Create a new empty context
    pub fn new() -> Self {
        Self::default()
    }

    /// Emit an event that inner middleware can observe
    pub fn emit_event(&mut self, source: &str, event_type: &str, data: Value) {
        self.events.push(MiddlewareEvent::new(source, event_type, data));
    }

    /// Check if any middleware event matches the given source and type
    pub fn has_event(&self, source: &str, event_type: &str) -> bool {
        self.events.iter()
            .any(|e| e.source == source && e.event_type == event_type)
    }

    /// Get all events from a specific source
    pub fn events_from(&self, source: &str) -> Vec<&MiddlewareEvent> {
        self.events.iter()
            .filter(|e| e.source == source)
            .collect()
    }

    /// Find the first event matching source and type
    pub fn find_event(&self, source: &str, event_type: &str) -> Option<&MiddlewareEvent> {
        self.events.iter()
            .find(|e| e.source == source && e.event_type == event_type)
    }

    /// Set a baggage value
    pub fn set_baggage(&mut self, key: impl Into<String>, value: Value) {
        self.baggage.insert(key.into(), value);
    }

    /// Get a baggage value
    pub fn get_baggage(&self, key: &str) -> Option<&Value> {
        self.baggage.get(key)
    }

    /// Check if a baggage key exists
    pub fn has_baggage(&self, key: &str) -> bool {
        self.baggage.contains_key(key)
    }

    /// Remove a baggage value
    pub fn remove_baggage(&mut self, key: &str) -> Option<Value> {
        self.baggage.remove(key)
    }

    /// Write a control event to the journal
    ///
    /// Control events are durable events that flow through the journal and are
    /// observed by the MetricsAggregator. Use this for significant state changes,
    /// periodic summaries, or anomaly detection.
    ///
    /// Note: The control event will be appended to the handler's results by
    /// MiddlewareTransform after all middleware has run.
    pub fn write_control_event(&mut self, event: ChainEvent) {
        self.control_events.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_middleware_event_creation() {
        let event = MiddlewareEvent::new(
            "circuit_breaker",
            "opened",
            json!({
                "consecutive_failures": 10,
                "threshold": 10
            })
        );

        assert_eq!(event.source, "circuit_breaker");
        assert_eq!(event.event_type, "opened");
        assert_eq!(event.data["consecutive_failures"], 10);
    }

    #[test]
    fn test_simple_middleware_event() {
        let event = MiddlewareEvent::simple("retry", "exhausted");

        assert_eq!(event.source, "retry");
        assert_eq!(event.event_type, "exhausted");
        assert_eq!(event.data, Value::Null);
    }

    #[test]
    fn test_context_event_emission() {
        let mut ctx = MiddlewareContext::new();

        ctx.emit_event("circuit_breaker", "rejected", json!({
            "reason": "circuit_open"
        }));

        ctx.emit_event("retry", "attempt_started", json!({
            "attempt": 1,
            "max_attempts": 3
        }));

        assert_eq!(ctx.events.len(), 2);
        assert!(ctx.has_event("circuit_breaker", "rejected"));
        assert!(ctx.has_event("retry", "attempt_started"));
        assert!(!ctx.has_event("rate_limiter", "throttled"));
    }

    #[test]
    fn test_context_event_queries() {
        let mut ctx = MiddlewareContext::new();

        ctx.emit_event("circuit_breaker", "rejected", json!({"reason": "open"}));
        ctx.emit_event("circuit_breaker", "opened", json!({"failures": 10}));
        ctx.emit_event("retry", "attempt_started", json!({"attempt": 1}));

        let cb_events = ctx.events_from("circuit_breaker");
        assert_eq!(cb_events.len(), 2);

        let rejected = ctx.find_event("circuit_breaker", "rejected");
        assert!(rejected.is_some());
        assert_eq!(rejected.unwrap().data["reason"], "open");
    }

    #[test]
    fn test_context_baggage() {
        let mut ctx = MiddlewareContext::new();

        ctx.set_baggage("retry_count", json!(3));
        ctx.set_baggage("circuit_state", json!("open"));

        assert!(ctx.has_baggage("retry_count"));
        assert_eq!(ctx.get_baggage("retry_count"), Some(&json!(3)));

        let removed = ctx.remove_baggage("circuit_state");
        assert_eq!(removed, Some(json!("open")));
        assert!(!ctx.has_baggage("circuit_state"));
    }

    #[test]
    fn test_write_control_event() {
        let mut ctx = MiddlewareContext::new();
        let writer_id = WriterId::from(obzenflow_core::StageId::new());

        // Write a circuit breaker opened event
        ctx.write_control_event(ChainEventFactory::circuit_breaker_opened(
            writer_id.clone(),
            0.75, // error_rate
            10,   // failure_count
        ));

        // Write a metrics state snapshot event
        ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
            writer_id,
            json!({
                "queue_depth": 42,
                "in_flight": 7
            })
        ));

        assert_eq!(ctx.control_events.len(), 2);
        assert!(ctx.control_events[0].is_lifecycle());
        assert!(ctx.control_events[1].is_lifecycle());
    }
}