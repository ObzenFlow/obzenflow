//! State definitions for metrics aggregator FSM
//!
//! The metrics aggregator follows a simple lifecycle:
//! Initializing -> Running -> Draining -> Drained
//! Event processing happens directly without FSM state tracking

use obzenflow_fsm::{StateVariant, EventVariant};
use obzenflow_core::EventId;
use serde::{Deserialize, Serialize};

/// FSM states for metrics aggregator lifecycle
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetricsAggregatorState {
    /// Initial state
    Initializing,
    
    /// Normal operation - processing events
    Running,
    
    /// Processing final events before shutdown
    Draining,
    
    /// Terminal state - all events processed
    Drained {
        last_event_id: Option<EventId>,
    },
}

impl StateVariant for MetricsAggregatorState {
    fn variant_name(&self) -> &str {
        match self {
            MetricsAggregatorState::Initializing => "Initializing",
            MetricsAggregatorState::Running => "Running",
            MetricsAggregatorState::Draining => "Draining",
            MetricsAggregatorState::Drained { .. } => "Drained",
        }
    }
}

/// Events that drive state transitions
#[derive(Clone, Debug)]
pub enum MetricsAggregatorEvent {
    /// Initialization complete, start processing
    StartRunning,
    
    /// Start draining process (from journal control event)
    StartDraining,
    
    /// No more events available during drain
    DrainComplete {
        last_event_id: Option<EventId>,
    },
}

impl EventVariant for MetricsAggregatorEvent {
    fn variant_name(&self) -> &str {
        match self {
            MetricsAggregatorEvent::StartRunning => "StartRunning",
            MetricsAggregatorEvent::StartDraining => "StartDraining",
            MetricsAggregatorEvent::DrainComplete { .. } => "DrainComplete",
        }
    }
}

/// Actions performed during transitions
#[derive(Clone, Debug, PartialEq)]
pub enum MetricsAggregatorAction {
    /// Initialize metrics collection
    Initialize,
    
    /// Publish drain complete event to journal
    PublishDrainComplete {
        last_event_id: Option<EventId>,
    },
}

/// Minimal context for the FSM
#[derive(Clone)]
pub struct MetricsAggregatorContext {
    pub journal: std::sync::Arc<crate::event_flow::reactive_journal::ReactiveJournal>,
    pub exporter: Option<std::sync::Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
    pub metrics_store: std::sync::Arc<tokio::sync::RwLock<MetricsStore>>,
    pub export_interval_secs: u64,
}

/// Simple metrics storage
#[derive(Default)]
pub struct MetricsStore {
    pub stage_metrics: std::collections::HashMap<String, StageMetrics>,
    pub last_event_id: Option<EventId>,
}

#[derive(Default, Clone)]
pub struct StageMetrics {
    pub events_in: u64,
    pub events_out: u64,
    pub errors: u64,
    pub total_processing_time_ms: u64,
    pub event_count: u64,
}

impl MetricsAggregatorContext {
    pub fn new(
        journal: std::sync::Arc<crate::event_flow::reactive_journal::ReactiveJournal>,
        exporter: Option<std::sync::Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
        export_interval_secs: u64,
    ) -> Self {
        Self {
            journal,
            exporter,
            metrics_store: std::sync::Arc::new(tokio::sync::RwLock::new(MetricsStore::default())),
            export_interval_secs,
        }
    }
}



