// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware context for event propagation
//!
//! This module provides a separate context that flows through middleware
//! during event processing, preserving the immutability of ChainEvent.

use obzenflow_core::ChainEvent;
use obzenflow_core::MiddlewareContextKey;
use obzenflow_core::MiddlewareExecutionScope;
use std::any::{Any, TypeId};
use std::collections::HashMap;

/// Ephemeral context that flows through middleware during processing
///
/// This context is created fresh for each event processing and is never
/// persisted. It enables middleware to communicate without mutating the
/// core ChainEvent.
#[derive(Default)]
pub struct MiddlewareContext {
    // NOTE: All fields are private; use helper methods.
    ephemeral_events: Vec<ChainEvent>,
    control_events: Vec<ChainEvent>,
    slots: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    /// Execution scope for this event (FLOWIP-120a). Defaults to `LiveHandler`,
    /// so any context not explicitly scoped behaves exactly as before: live, no
    /// suppression. Handler-level control middleware reads this to suppress side
    /// effects during deterministic replay reconstruction.
    execution_scope: MiddlewareExecutionScope,
}

impl MiddlewareContext {
    /// Create a new empty context (live handler scope).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new empty context bound to an explicit execution scope.
    pub fn with_scope(scope: MiddlewareExecutionScope) -> Self {
        Self {
            execution_scope: scope,
            ..Self::default()
        }
    }

    /// The execution scope for this event (FLOWIP-120a).
    pub fn execution_scope(&self) -> MiddlewareExecutionScope {
        self.execution_scope
    }

    /// Bind the execution scope for this event (FLOWIP-120a). Called by the
    /// middleware runners from the stage's replay mode before `pre_handle`.
    pub fn set_execution_scope(&mut self, scope: MiddlewareExecutionScope) {
        self.execution_scope = scope;
    }

    /// Record an ephemeral (in-memory only) event for middleware coordination.
    ///
    /// Ephemeral events are never appended to the journal; they are visible only
    /// within the current middleware pass.
    pub fn emit_ephemeral_event(&mut self, event: ChainEvent) {
        self.ephemeral_events.push(event);
    }

    pub fn ephemeral_events(&self) -> &[ChainEvent] {
        &self.ephemeral_events
    }

    /// Insert a typed slot value.
    pub fn insert<K: MiddlewareContextKey>(&mut self, value: K::Value) {
        self.slots.insert(
            TypeId::of::<K>(),
            Box::new(value) as Box<dyn Any + Send + Sync>,
        );
    }

    /// Get a typed slot value by key.
    pub fn get<K: MiddlewareContextKey>(&self) -> Option<&K::Value> {
        self.slots
            .get(&TypeId::of::<K>())
            .and_then(|boxed| boxed.downcast_ref::<K::Value>())
    }

    /// Get a mutable typed slot value by key.
    pub fn get_mut<K: MiddlewareContextKey>(&mut self) -> Option<&mut K::Value> {
        self.slots
            .get_mut(&TypeId::of::<K>())
            .and_then(|boxed| boxed.downcast_mut::<K::Value>())
    }

    /// Check whether a typed slot value exists for the given key.
    pub fn contains<K: MiddlewareContextKey>(&self) -> bool {
        self.slots.contains_key(&TypeId::of::<K>())
    }

    /// Remove a typed slot value by key.
    pub fn remove<K: MiddlewareContextKey>(&mut self) -> Option<K::Value> {
        let boxed = self.slots.remove(&TypeId::of::<K>())?;
        boxed.downcast::<K::Value>().ok().map(|boxed| *boxed)
    }

    /// Write a control event to the journal
    ///
    /// Control events are durable events that flow through the journal and are
    /// available to downstream consumers and application-metrics aggregation.
    /// Use this for significant state changes, periodic summaries, or anomaly
    /// detection.
    ///
    /// Note: The control event will be appended to the handler's results by
    /// MiddlewareTransform after all middleware has run.
    pub fn write_control_event(&mut self, event: ChainEvent) {
        self.control_events.push(event);
    }

    pub fn control_events(&self) -> &[ChainEvent] {
        &self.control_events
    }

    pub fn take_control_events(&mut self) -> Vec<ChainEvent> {
        std::mem::take(&mut self.control_events)
    }

    pub fn clear_control_events(&mut self) {
        self.control_events.clear();
    }

    pub fn retain_control_events<F>(&mut self, f: F)
    where
        F: FnMut(&ChainEvent) -> bool,
    {
        self.control_events.retain(f);
    }
}

impl std::fmt::Debug for MiddlewareContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareContext")
            .field("ephemeral_events_len", &self.ephemeral_events.len())
            .field("control_events_len", &self.control_events.len())
            .field("slots_len", &self.slots.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::WriterId;
    use serde_json::json;

    struct RetryCountKey;
    impl MiddlewareContextKey for RetryCountKey {
        type Value = u32;
        const LABEL: &'static str = "retry_count";
    }

    struct CircuitStateKey;
    impl MiddlewareContextKey for CircuitStateKey {
        type Value = String;
        const LABEL: &'static str = "circuit_state";
    }

    #[test]
    fn test_context_ephemeral_events_round_trip() {
        let mut ctx = MiddlewareContext::new();
        let writer_id = WriterId::from(obzenflow_core::StageId::new());

        let event = ChainEventFactory::metrics_state_snapshot(writer_id, json!({ "k": 1 }));
        ctx.emit_ephemeral_event(event.clone());

        assert_eq!(ctx.ephemeral_events().len(), 1);
        assert_eq!(ctx.ephemeral_events()[0].id, event.id);
    }

    #[test]
    fn test_context_typed_slots() {
        let mut ctx = MiddlewareContext::new();

        ctx.insert::<RetryCountKey>(3);
        ctx.insert::<CircuitStateKey>("open".to_string());

        assert!(ctx.contains::<RetryCountKey>());
        assert_eq!(ctx.get::<RetryCountKey>(), Some(&3));

        let removed = ctx.remove::<CircuitStateKey>();
        assert_eq!(removed.as_deref(), Some("open"));
        assert!(!ctx.contains::<CircuitStateKey>());
    }

    #[test]
    fn test_write_control_event() {
        let mut ctx = MiddlewareContext::new();
        let writer_id = WriterId::from(obzenflow_core::StageId::new());

        // Write a circuit breaker opened event
        ctx.write_control_event(ChainEventFactory::circuit_breaker_opened(
            writer_id, 0.75, // error_rate
            10,   // failure_count
        ));

        // Write a metrics state snapshot event
        ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
            writer_id,
            json!({
                "queue_depth": 42,
                "in_flight": 7
            }),
        ));

        assert_eq!(ctx.control_events().len(), 2);
        assert!(ctx.control_events()[0].is_lifecycle());
        assert!(ctx.control_events()[1].is_lifecycle());
    }
}
