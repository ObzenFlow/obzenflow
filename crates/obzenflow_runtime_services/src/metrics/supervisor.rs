//! Metrics aggregator supervisor - self-contained event loop
//!
//! The supervisor owns the FSM directly and runs autonomously.
//! Once started, all communication happens through journal events only.

use obzenflow_core::{ChainEvent, EventEnvelope, EventId, WriterId, Journal};
use obzenflow_core::metrics::{AppMetricsSnapshot, MetricsExporter};
use obzenflow_fsm::StateMachine;
use std::sync::Arc;

use crate::event_flow::{reactive_journal::ReactiveJournal, JournalSubscription, SubscriptionFilter};

use super::{
    build_metrics_aggregator_fsm, MetricsAggregatorAction, MetricsAggregatorContext,
    MetricsAggregatorEvent, MetricsAggregatorState, StageMetrics,
};

/// Type alias for the metrics FSM
type MetricsAggregatorFsm = StateMachine<
    MetricsAggregatorState,
    MetricsAggregatorEvent,
    MetricsAggregatorContext,
    MetricsAggregatorAction,
>;

/// The supervisor that manages the metrics aggregator
pub struct MetricsAggregatorSupervisor {
    /// The metrics FSM - owned directly
    fsm: MetricsAggregatorFsm,
    
    /// Metrics context
    context: Arc<MetricsAggregatorContext>,
    
    /// Reactive journal
    journal: Arc<ReactiveJournal>,
    
    /// Journal subscription
    subscription: Option<JournalSubscription>,
    
    /// Writer ID for journal events
    writer_id: Option<WriterId>,
}

impl MetricsAggregatorSupervisor {
    /// Create a new metrics aggregator supervisor
    pub fn new(
        journal: Arc<ReactiveJournal>,
        exporter: Option<Arc<dyn MetricsExporter>>,
        export_interval_secs: u64,
    ) -> Self {
        let context = Arc::new(MetricsAggregatorContext::new(
            journal.clone(),
            exporter,
            export_interval_secs,
        ));

        Self {
            fsm: build_metrics_aggregator_fsm(),
            context,
            journal,
            subscription: None,
            writer_id: None,
        }
    }

    /// Start the aggregator - this is the only public method
    /// It runs until the aggregator reaches the Drained state
    pub async fn start(&mut self) -> Result<(), String> {
        // Register as a writer
        let stage_id = obzenflow_topology_services::stages::StageId::new();
        self.writer_id = Some(
            self.journal
                .register_writer(stage_id, None)
                .await
                .map_err(|e| format!("Failed to register writer: {}", e))?
        );

        // Initialize FSM
        let actions = self.fsm
            .handle(MetricsAggregatorEvent::StartRunning, self.context.clone())
            .await
            .map_err(|e| format!("Failed to initialize FSM: {}", e))?;
        
        self.handle_actions(actions).await;

        // Subscribe to all events
        self.subscription = Some(
            self.journal
                .subscribe(SubscriptionFilter::All)
                .await
                .map_err(|e| format!("Failed to subscribe to journal: {}", e))?
        );
        
        // Publish ready event to signal we're ready to receive events
        if let Some(writer_id) = &self.writer_id {
            let ready_event = ChainEvent::new(
                EventId::new(),
                writer_id.clone(),
                "system.metrics.ready",
                serde_json::json!({})
            );
            
            self.journal.append(writer_id, ready_event, None).await
                .map_err(|e| format!("Failed to publish ready event: {}", e))?;
            
            tracing::info!("Metrics aggregator published ready event");
        }

        // Run the event loop
        self.run().await;
        
        Ok(())
    }

    /// Internal event loop - runs until drained
    async fn run(&mut self) {
        let mut export_timer = tokio::time::interval(
            tokio::time::Duration::from_secs(self.context.export_interval_secs)
        );
        export_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Take ownership of subscription to avoid borrow checker issues
        let mut subscription = match self.subscription.take() {
            Some(sub) => sub,
            None => {
                tracing::error!("No subscription available");
                return;
            }
        };

        loop {
            match self.fsm.state() {
                MetricsAggregatorState::Running => {
                    tokio::select! {
                        // Process journal events
                        result = subscription.recv_batch() => {
                            match result {
                                Ok(batch) => {
                                    for envelope in &batch {
                                        // Check for control events
                                        if envelope.event.event_type == "system.metrics.drain" {
                                            let actions = self.fsm
                                                .handle(MetricsAggregatorEvent::StartDraining, self.context.clone())
                                                .await
                                                .unwrap_or_default();
                                            self.handle_actions(actions).await;
                                        }
                                        
                                        // Update metrics directly
                                        self.update_metrics(&envelope).await;
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to receive events: {}", e);
                                }
                            }
                        }
                        
                        // Export periodically
                        _ = export_timer.tick() => {
                            self.export_metrics().await;
                        }
                    }
                }
                
                MetricsAggregatorState::Draining => {
                    // Keep draining until we get no events for a timeout period
                    let mut consecutive_empty_batches = 0;
                    loop {
                        match tokio::time::timeout(
                            tokio::time::Duration::from_millis(50),
                            subscription.recv_batch()
                        ).await {
                            Ok(Ok(batch)) if !batch.is_empty() => {
                                // Got events, reset counter
                                consecutive_empty_batches = 0;
                                for envelope in batch {
                                    self.update_metrics(&envelope).await;
                                }
                            }
                            _ => {
                                // No events in this batch
                                consecutive_empty_batches += 1;
                                
                                // If we've had 2 consecutive empty batches, we're done
                                if consecutive_empty_batches >= 2 {
                                    // Export final metrics before draining
                                    tracing::info!("Exporting final metrics before drain");
                                    self.export_metrics().await;
                                    
                                    // Now transition to drained
                                    let last_event_id = self.get_last_event_id().await;
                                    let actions = self.fsm
                                        .handle(
                                            MetricsAggregatorEvent::DrainComplete { last_event_id },
                                            self.context.clone()
                                        )
                                        .await
                                        .unwrap_or_default();
                                    self.handle_actions(actions).await;
                                    break;
                                }
                            }
                        }
                    }
                }
                
                MetricsAggregatorState::Drained { .. } => {
                    // Terminal state - exit
                    tracing::info!("Metrics aggregator drained, exiting");
                    break;
                }
                
                _ => {
                    // Initializing state - should not be here during run
                    tracing::warn!("Unexpected state in run loop: {:?}", self.fsm.state());
                    break;
                }
            }
        }
    }

    /// Handle FSM actions
    async fn handle_actions(&self, actions: Vec<MetricsAggregatorAction>) {
        for action in actions {
            match action {
                MetricsAggregatorAction::Initialize => {
                    tracing::info!("Metrics aggregator initialized");
                }
                
                MetricsAggregatorAction::PublishDrainComplete { last_event_id } => {
                    if let Some(writer_id) = &self.writer_id {
                        let mut payload = serde_json::json!({});
                        if let Some(id) = last_event_id {
                            payload["last_event_id"] = serde_json::json!(id.to_string());
                        }
                        
                        let drain_event = ChainEvent::new(
                            EventId::new(),
                            writer_id.clone(),
                            "system.metrics.drained",
                            payload
                        );
                        
                        if let Err(e) = self.journal.append(writer_id, drain_event, None).await {
                            tracing::error!("Failed to publish drain complete event: {}", e);
                        }
                    } else {
                        tracing::error!("No writer ID available to publish drain event");
                    }
                }
            }
        }
    }

    /// Update metrics from an event
    async fn update_metrics(&self, envelope: &EventEnvelope) {
        let mut store = self.context.metrics_store.write().await;
        
        // Update last event ID
        store.last_event_id = Some(envelope.event.id.clone());
        
        let event = &envelope.event;
        
        // Skip control events that start with "control." or "system."
        if event.is_control() || event.event_type.starts_with("system.") {
            // Handle specific control events if needed
            match event.event_type.as_str() {
                // We could handle control events here if needed
                _ => return,
            }
        }
        
        // Process data events - extract metrics from flow context
        let stage_name = &event.flow_context.stage_name;
        let flow_name = &event.flow_context.flow_name;
        
        // Create a key combining flow and stage name
        let key = format!("{}:{}", flow_name, stage_name);
        
        let metrics = store
            .stage_metrics
            .entry(key.clone())
            .or_insert_with(StageMetrics::default);
        
        // Count the event
        metrics.events_in += 1;
        metrics.events_out += 1; // For now, treat as both in and out
        
        // Check for errors from processing outcome
        if matches!(event.processing_info.outcome, obzenflow_core::event::processing_outcome::ProcessingOutcome::Error(_)) {
            metrics.errors += 1;
        }
        
        // Record processing time
        let duration_ms = event.processing_info.processing_time_ms;
        metrics.total_processing_time_ms += duration_ms;
        metrics.event_count += 1;
        
        tracing::debug!(
            "Updated metrics for {}: events={}, errors={}, avg_time={}ms",
            key,
            metrics.events_in,
            metrics.errors,
            if metrics.event_count > 0 { metrics.total_processing_time_ms / metrics.event_count } else { 0 }
        );
    }

    /// Export metrics snapshot
    async fn export_metrics(&self) {
        if let Some(exporter) = &self.context.exporter {
            let store = self.context.metrics_store.read().await;
            let mut snapshot = AppMetricsSnapshot::default();
            
            tracing::info!("Exporting metrics: {} stage entries", store.stage_metrics.len());
            
            // Convert stage metrics to snapshot format
            for (stage_key, metrics) in &store.stage_metrics {
                // Use events_in as the event count (since we increment both in and out)
                snapshot
                    .event_counts
                    .insert(stage_key.clone(), metrics.events_in);
                    
                snapshot
                    .error_counts
                    .insert(stage_key.clone(), metrics.errors);
                    
                // Add processing time as a simple histogram snapshot
                if metrics.event_count > 0 {
                    let avg_time_seconds = (metrics.total_processing_time_ms as f64 / metrics.event_count as f64) / 1000.0;
                    
                    // Create a simple histogram snapshot with the average
                    let mut percentiles = std::collections::HashMap::new();
                    percentiles.insert("p50".to_string(), avg_time_seconds);
                    percentiles.insert("p90".to_string(), avg_time_seconds);
                    percentiles.insert("p99".to_string(), avg_time_seconds);
                    
                    let hist_snapshot = obzenflow_core::metrics::HistogramSnapshot {
                        count: metrics.event_count,
                        sum: (metrics.total_processing_time_ms as f64) / 1000.0,
                        min: avg_time_seconds,
                        max: avg_time_seconds,
                        percentiles,
                    };
                    
                    snapshot
                        .processing_times
                        .insert(stage_key.clone(), hist_snapshot);
                }
                
                tracing::debug!(
                    "Exported metrics for {}: events={}, errors={}, avg_time={}ms",
                    stage_key,
                    metrics.events_in,
                    metrics.errors,
                    if metrics.event_count > 0 { metrics.total_processing_time_ms / metrics.event_count } else { 0 }
                );
            }
            
            drop(store); // Release the lock before exporting
            
            tracing::info!("Pushing metrics snapshot to exporter");
            
            if let Err(e) = exporter.update_app_metrics(snapshot) {
                tracing::warn!("Failed to export metrics: {}", e);
            } else {
                tracing::info!("Successfully exported metrics");
            }
        }
    }

    /// Get the last processed event ID
    async fn get_last_event_id(&self) -> Option<EventId> {
        let store = self.context.metrics_store.read().await;
        store.last_event_id.clone()
    }
}

impl Drop for MetricsAggregatorSupervisor {
    fn drop(&mut self) {
        // Clean shutdown - subscription will be dropped automatically
        tracing::debug!("Metrics aggregator supervisor dropped");
    }
}