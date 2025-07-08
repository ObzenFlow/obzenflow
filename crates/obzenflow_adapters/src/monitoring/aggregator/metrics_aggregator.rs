//! MetricsAggregator implementation
//!
//! Subscribes to the journal and maintains real-time metric aggregates
//! by processing ChainEvents as they flow through the system.

use std::collections::HashMap;
use std::sync::Arc;

use obzenflow_core::{
    ChainEvent,
    EventId,
    EventEnvelope,
    event::processing_outcome::ProcessingOutcome,
    event::correlation::CorrelationId,
    event::flow_context::StageType,
};
use obzenflow_runtime_services::event_flow::{
    reactive_journal::ReactiveJournal,
    JournalSubscription,
    SubscriptionFilter,
};
use tokio::task::JoinHandle;

use crate::monitoring::metrics::primitives::{Counter, Gauge, Histogram};

/// Key for identifying a stage in metrics
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct StageKey {
    pub flow_name: String,
    pub stage_name: String,
}

/// Aggregates metrics from the event stream
pub struct EventMetricsAggregator {
    // Core metrics derived from ALL data events
    pub events_total: HashMap<StageKey, Counter>,
    pub errors_total: HashMap<StageKey, Counter>,
    pub duration_seconds: HashMap<StageKey, Histogram>,
    
    // Concurrency metrics (updated via control events)
    pub in_flight: HashMap<StageKey, Gauge>,
    pub queue_depth: HashMap<StageKey, Gauge>,
    
    // Resource metrics (from control events)
    pub cpu_usage_ratio: HashMap<StageKey, Gauge>,
    pub memory_bytes: HashMap<StageKey, Gauge>,
    
    // SAAFE metrics
    pub anomalies_total: HashMap<StageKey, Counter>,
    pub amendments_total: HashMap<StageKey, Counter>,
    pub saturation_ratio: HashMap<StageKey, Gauge>,
    
    // Flow-level metrics
    flow_start_times: HashMap<CorrelationId, std::time::Instant>,
    pub flow_latency_seconds: HashMap<String, Histogram>, // by flow name
    
    // Dropped events tracking (computed as source_events - sink_events)
    pub dropped_events: HashMap<String, Gauge>, // by flow name
    
    // Middleware-specific metrics (from CONTROL_MIDDLEWARE_STATE/SUMMARY events)
    pub circuit_breaker_state: HashMap<StageKey, Gauge>, // 0=closed, 0.5=half_open, 1=open
    pub circuit_breaker_rejection_rate: HashMap<StageKey, Gauge>,
    pub circuit_breaker_consecutive_failures: HashMap<StageKey, Gauge>,
    pub rate_limiter_delay_rate: HashMap<StageKey, Gauge>,
    pub rate_limiter_utilization: HashMap<StageKey, Gauge>,
    
    // TODO(FLOWIP-053a): Custom metrics support
    // See docs/flowip-proposals/FLOWIP-053a-custom-metrics.md for design
    // This field is reserved for future implementation
    pub custom_metrics: HashMap<String, Box<dyn MetricValue>>,
    
    // Journal subscription
    subscription: Option<JournalSubscription>,
    
    // Last processed event for resume capability
    last_event_id: Option<EventId>,
    
    // Event loop task handle
    event_loop_handle: Option<JoinHandle<()>>,
}

/// Trait for custom metric values
/// TODO(FLOWIP-053a): This trait is reserved for future custom metrics implementation
/// See docs/flowip-proposals/FLOWIP-053a-custom-metrics.md for design
pub trait MetricValue: Send + Sync {
    fn export(&self) -> f64;
    fn metric_type(&self) -> &'static str;
}

impl EventMetricsAggregator {
    /// Create a new MetricsAggregator
    pub fn new() -> Self {
        let result = Self {
            events_total: HashMap::new(),
            errors_total: HashMap::new(),
            duration_seconds: HashMap::new(),
            in_flight: HashMap::new(),
            queue_depth: HashMap::new(),
            cpu_usage_ratio: HashMap::new(),
            memory_bytes: HashMap::new(),
            anomalies_total: HashMap::new(),
            amendments_total: HashMap::new(),
            saturation_ratio: HashMap::new(),
            flow_start_times: HashMap::new(),
            flow_latency_seconds: HashMap::new(),
            dropped_events: HashMap::new(),
            circuit_breaker_state: HashMap::new(),
            circuit_breaker_rejection_rate: HashMap::new(),
            circuit_breaker_consecutive_failures: HashMap::new(),
            rate_limiter_delay_rate: HashMap::new(),
            rate_limiter_utilization: HashMap::new(),
            custom_metrics: HashMap::new(),
            subscription: None,
            last_event_id: None,
            event_loop_handle: None,
        };
        tracing::warn!("Created new MetricsAggregator instance at {:p}", &result as *const _);
        result
    }
    
    /// Process a single event from the journal
    pub fn process_event_impl(&mut self, envelope: &EventEnvelope) {
        let event = &envelope.event;
        tracing::info!("MetricsAggregator processing event: type={}, flow={}, stage={}, processing_time_ms={}, outcome={:?}", 
            event.event_type, 
            event.flow_context.flow_name,
            event.flow_context.stage_name,
            event.processing_info.processing_time_ms,
            event.processing_info.outcome);
        
        // Extract stage key from flow context
        let key = StageKey {
            flow_name: event.flow_context.flow_name.clone(),
            stage_name: event.flow_context.stage_name.clone(),
        };
        
        // Handle control events for metrics
        tracing::trace!("is_control() = {}", event.is_control());
        if event.is_control() {
            match event.event_type.as_str() {
                ChainEvent::CONTROL_METRICS_STATE => {
                    self.process_state_metrics(event, &key);
                }
                ChainEvent::CONTROL_METRICS_RESOURCE => {
                    self.process_resource_metrics(event, &key);
                }
                ChainEvent::CONTROL_MIDDLEWARE_STATE => {
                    self.process_middleware_state(event, &key);
                }
                ChainEvent::CONTROL_MIDDLEWARE_SUMMARY => {
                    self.process_middleware_summary(event, &key);
                }
                ChainEvent::CONTROL_METRICS_ANOMALY => {
                    self.process_anomaly_event(event, &key);
                }
                // Lifecycle events for amendments
                "control.stage.started" | "control.stage.stopped" | "control.stage.reconfigured" => {
                    self.process_amendment_event(event, &key);
                }
                // Sink consumption tracking
                "control.sink.consumed" => {
                    // Count this as a regular event for the sink
                    tracing::info!("Incrementing events_total for sink consumption: flow='{}', stage='{}'", 
                        key.flow_name, key.stage_name);
                    self.events_total
                        .entry(key.clone())
                        .or_insert_with(Counter::new)
                        .increment();
                }
                _ => {
                    // Skip other control events
                    tracing::trace!("Skipping control event type: {}", event.event_type);
                }
            }
            return;
        }
        
        // Update events_total counter
        tracing::info!("Incrementing events_total for flow='{}', stage='{}'", 
            key.flow_name, key.stage_name);
        
        // Debug: Check HashMap state before and after
        tracing::debug!("events_total.len() before: {}", self.events_total.len());
        
        self.events_total
            .entry(key.clone())
            .or_insert_with(Counter::new)
            .increment();
            
        tracing::debug!("events_total.len() after: {}", self.events_total.len());
        
        // Debug: Print the actual counter value
        if let Some(counter) = self.events_total.get(&key) {
            tracing::debug!("Counter value for key {:?}: {}", key, counter.get());
        }
        
        // Record duration from processing_info
        let duration_ms = event.processing_info.processing_time_ms;
        let duration_seconds = duration_ms as f64 / 1000.0;
        
        
        // Debug log for all events
        tracing::info!(
            "MetricsAggregator: Processing event {} from {}/{} with duration_ms={} ({}s)",
            event.id,
            key.flow_name,
            key.stage_name,
            duration_ms,
            duration_seconds
        );
        
        self.duration_seconds
            .entry(key.clone())
            .or_insert_with(Histogram::for_seconds)
            .observe(duration_seconds);
        
        // Check for errors from outcome
        if matches!(event.processing_info.outcome, ProcessingOutcome::Error(_)) {
            self.errors_total
                .entry(key.clone())
                .or_insert_with(Counter::new)
                .increment();
        }
        
        // Track flow-level metrics
        self.track_flow_metrics(event, &key);
        
        // Update dropped events calculation
        self.update_dropped_events(&event.flow_context.flow_name);
        
        // Store last event ID for resume capability
        self.last_event_id = Some(event.id.clone());
        
        tracing::trace!(
            "Processed event {} for stage {}/{}", 
            event.id, 
            key.flow_name, 
            key.stage_name
        );
    }
    
    /// Start the aggregator with a subscription to the journal and exporter
    /// This method is designed to work with the trait-based architecture
    pub async fn start_with_exporter(
        aggregator: Arc<std::sync::Mutex<dyn obzenflow_runtime_services::metrics::MetricsAggregator>>,
        journal: Arc<ReactiveJournal>,
        exporter: Arc<dyn obzenflow_core::metrics::MetricsExporter>,
    ) -> Result<JoinHandle<()>, String> {
        // Subscribe to ALL events to see every stage's activity
        let filter = SubscriptionFilter::All;
        
        let mut subscription = journal.subscribe(filter).await
            .map_err(|e| format!("Failed to create subscription: {:?}", e))?;
        
        // Clone for the event loop
        let aggregator_clone = aggregator.clone();
        
        // Spawn the event loop task with periodic snapshot pushing
        let handle = tokio::spawn(async move {
            tracing::info!("MetricsAggregator event loop started");
            
            // Create interval for periodic snapshot pushing
            let mut snapshot_interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            snapshot_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            
            loop {
                tokio::select! {
                    // Process events from subscription
                    batch_result = subscription.recv_batch() => {
                        match batch_result {
                            Ok(events) if !events.is_empty() => {
                                tracing::debug!("MetricsAggregator received {} events", events.len());
                                
                                // Process events
                                for envelope in events {
                                    if let Ok(mut agg) = aggregator_clone.lock() {
                                        agg.process_event(&envelope);
                                    } else {
                                        tracing::error!("Failed to lock aggregator - mutex poisoned");
                                        return;
                                    }
                                }
                            }
                            Ok(_) => {
                                // Empty batch, continue
                            }
                            Err(e) => {
                                tracing::error!("Subscription error: {:?}", e);
                                return;
                            }
                        }
                    }
                    
                    // Push snapshots periodically
                    _ = snapshot_interval.tick() => {
                        tracing::info!("MetricsAggregator pushing snapshot to exporter");
                        
                        // Create snapshot
                        let snapshot = {
                            if let Ok(agg) = aggregator_clone.lock() {
                                agg.create_snapshot()
                            } else {
                                tracing::error!("Failed to lock aggregator for snapshot");
                                continue;
                            }
                        };
                        
                        // Push to exporter
                        if let Err(e) = exporter.update_app_metrics(snapshot) {
                            tracing::warn!("Failed to push app metrics snapshot: {}", e);
                        } else {
                            tracing::debug!("Successfully pushed metrics snapshot");
                        }
                    }
                }
            }
        });
        
        Ok(handle)
    }
    
    /// Start the aggregator with a subscription to the journal (legacy method)
    pub async fn start(aggregator: Arc<std::sync::Mutex<Self>>, journal: Arc<ReactiveJournal>) -> Result<(), String> {
        // Subscribe to ALL events to see every stage's activity
        // The MetricsAggregator filters control vs data events internally
        let filter = SubscriptionFilter::All;
        
        let mut subscription = journal.subscribe(filter).await
            .map_err(|e| format!("Failed to create subscription: {:?}", e))?;
        
        // Clone the Arc for the event loop
        let aggregator_clone = aggregator.clone();
        
        // Spawn the event loop task
        let handle = tokio::spawn(async move {
            tracing::info!("MetricsAggregator event loop started");
            
            loop {
                match subscription.recv_batch().await {
                    Ok(events) if !events.is_empty() => {
                        tracing::info!("MetricsAggregator received {} events", events.len());
                        
                        // Process events
                        for envelope in events {
                            // Lock aggregator for each event to avoid holding lock across await
                            if let Ok(mut agg) = aggregator_clone.lock() {
                                use obzenflow_runtime_services::metrics::MetricsAggregator as _;
                                agg.process_event(&envelope);
                            } else {
                                tracing::error!("Failed to lock aggregator - mutex poisoned");
                                break;
                            }
                        }
                    }
                    Ok(_) => {
                        // Empty batch, continue
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                    Err(e) => {
                        tracing::error!("Subscription error: {:?}", e);
                        break;
                    }
                }
            }
            
            tracing::info!("MetricsAggregator event loop stopped");
        });
        
        // Store the handle
        if let Ok(mut agg) = aggregator.lock() {
            agg.event_loop_handle = Some(handle);
        } else {
            return Err("Failed to lock aggregator to store handle".to_string());
        }
        
        Ok(())
    }
    
    /// Stop the aggregator
    pub async fn stop(&mut self) {
        if let Some(handle) = self.event_loop_handle.take() {
            handle.abort();
            let _ = handle;
        }
        self.subscription = None;
    }
    
    /// Get current value of events_total for a stage
    pub fn get_events_total(&self, flow: &str, stage: &str) -> f64 {
        let key = StageKey {
            flow_name: flow.to_string(),
            stage_name: stage.to_string(),
        };
        self.events_total
            .get(&key)
            .map(|c| c.get() as f64)
            .unwrap_or(0.0)
    }
    
    /// Get current value of errors_total for a stage
    pub fn get_errors_total(&self, flow: &str, stage: &str) -> f64 {
        let key = StageKey {
            flow_name: flow.to_string(),
            stage_name: stage.to_string(),
        };
        self.errors_total
            .get(&key)
            .map(|c| c.get() as f64)
            .unwrap_or(0.0)
    }
    
    /// Get duration histogram for a stage
    pub fn get_duration_histogram(&self, flow: &str, stage: &str) -> Option<&Histogram> {
        let key = StageKey {
            flow_name: flow.to_string(),
            stage_name: stage.to_string(),
        };
        self.duration_seconds.get(&key)
    }
    
    /// Process state metrics (saturation: queue depth, in-flight)
    fn process_state_metrics(&mut self, event: &ChainEvent, key: &StageKey) {
        if let Some(queue_depth) = event.payload.get("queue_depth").and_then(|v| v.as_u64()) {
            self.queue_depth
                .entry(key.clone())
                .or_insert_with(Gauge::new)
                .set(queue_depth as f64);
                
            // Calculate saturation ratio if max_queue_size is provided
            if let Some(max_size) = event.payload.get("max_queue_size").and_then(|v| v.as_u64()) {
                if max_size > 0 {
                    let saturation = queue_depth as f64 / max_size as f64;
                    self.saturation_ratio
                        .entry(key.clone())
                        .or_insert_with(Gauge::new)
                        .set(saturation);
                    
                    tracing::trace!(
                        "Calculated saturation ratio for {}/{}: {} (queue_depth={}, max_size={})",
                        key.flow_name,
                        key.stage_name,
                        saturation,
                        queue_depth,
                        max_size
                    );
                }
            }
        }
        
        if let Some(in_flight) = event.payload.get("in_flight").and_then(|v| v.as_u64()) {
            self.in_flight
                .entry(key.clone())
                .or_insert_with(Gauge::new)
                .set(in_flight as f64);
        }
        
        tracing::trace!(
            "Updated state metrics for {}/{}: queue_depth={:?}, in_flight={:?}",
            key.flow_name,
            key.stage_name,
            event.payload.get("queue_depth"),
            event.payload.get("in_flight")
        );
    }
    
    /// Process resource metrics (utilization: CPU, memory)
    fn process_resource_metrics(&mut self, event: &ChainEvent, key: &StageKey) {
        if let Some(cpu_usage) = event.payload.get("cpu_usage_ratio").and_then(|v| v.as_f64()) {
            self.cpu_usage_ratio
                .entry(key.clone())
                .or_insert_with(Gauge::new)
                .set(cpu_usage);
        }
        
        if let Some(memory_bytes) = event.payload.get("memory_bytes").and_then(|v| v.as_u64()) {
            self.memory_bytes
                .entry(key.clone())
                .or_insert_with(Gauge::new)
                .set(memory_bytes as f64);
        }
        
        tracing::trace!(
            "Updated resource metrics for {}/{}: cpu={:?}, memory={:?}",
            key.flow_name,
            key.stage_name,
            event.payload.get("cpu_usage_ratio"),
            event.payload.get("memory_bytes")
        );
    }
    
    /// Process middleware state transitions
    fn process_middleware_state(&mut self, event: &ChainEvent, key: &StageKey) {
        if let Some(middleware) = event.payload.get("middleware").and_then(|v| v.as_str()) {
            if let Some(transition) = event.payload.get("state_transition").and_then(|v| v.as_object()) {
                let new_state = transition.get("to").and_then(|v| v.as_str()).unwrap_or("unknown");
                
                tracing::info!(
                    "Middleware state change for {}/{}: {} transitioned from {:?} to {:?}",
                    key.flow_name,
                    key.stage_name,
                    middleware,
                    transition.get("from"),
                    transition.get("to")
                );
                
                // Export circuit breaker state as a gauge
                if middleware == "circuit_breaker" {
                    // Handle both lowercase and capitalized state names
                    let state_value = match new_state.to_lowercase().as_str() {
                        "closed" => 0.0,
                        "halfopen" | "half_open" => 0.5,
                        "open" => 1.0,
                        _ => {
                            tracing::warn!("Unknown circuit breaker state: {}", new_state);
                            0.0
                        }
                    };
                    
                    self.circuit_breaker_state
                        .entry(key.clone())
                        .or_insert_with(Gauge::new)
                        .set(state_value);
                    
                    tracing::debug!(
                        "Updated circuit breaker state gauge for {}/{} to {}",
                        key.flow_name,
                        key.stage_name,
                        state_value
                    );
                }
                
                // Rate limiter state changes could be tracked here too
                if middleware == "rate_limiter" {
                    // Rate limiter doesn't have discrete states like circuit breaker,
                    // but we could track when it transitions to/from exhausted state
                    if let Some(reason) = transition.get("reason").and_then(|v| v.as_str()) {
                        if reason == "rate_limit_exceeded" || reason == "rate_limit_recovered" {
                            tracing::info!(
                                "Rate limiter state change for {}/{}: {}",
                                key.flow_name,
                                key.stage_name,
                                reason
                            );
                        }
                    }
                }
            }
        }
    }
    
    /// Process middleware summaries (periodic aggregated stats)
    fn process_middleware_summary(&mut self, event: &ChainEvent, key: &StageKey) {
        if let Some(middleware) = event.payload.get("middleware").and_then(|v| v.as_str()) {
            if let Some(stats) = event.payload.get("stats").and_then(|v| v.as_object()) {
                tracing::debug!(
                    "Middleware summary for {}/{} [{}]: {:?}",
                    key.flow_name,
                    key.stage_name,
                    middleware,
                    stats
                );
                
                // Extract and store circuit breaker metrics
                if middleware == "circuit_breaker" {
                    // Calculate rejection rate
                    if let (Some(processed), Some(rejected)) = (
                        stats.get("requests_processed").and_then(|v| v.as_u64()),
                        stats.get("requests_rejected").and_then(|v| v.as_u64())
                    ) {
                        let total = processed + rejected;
                        if total > 0 {
                            let rejection_rate = rejected as f64 / total as f64;
                            self.circuit_breaker_rejection_rate
                                .entry(key.clone())
                                .or_insert_with(Gauge::new)
                                .set(rejection_rate);
                                
                            tracing::trace!(
                                "Circuit breaker rejection rate for {}/{}: {:.2}% ({}/{})",
                                key.flow_name,
                                key.stage_name,
                                rejection_rate * 100.0,
                                rejected,
                                total
                            );
                        }
                    }
                    
                    // Store consecutive failures
                    if let Some(failures) = stats.get("consecutive_failures").and_then(|v| v.as_u64()) {
                        self.circuit_breaker_consecutive_failures
                            .entry(key.clone())
                            .or_insert_with(Gauge::new)
                            .set(failures as f64);
                    }
                    
                } else if middleware == "rate_limiter" {
                    // Extract rate limiter metrics
                    if let Some(window_stats) = stats.get("window_stats").and_then(|v| v.as_object()) {
                        // Calculate delay rate
                        if let (Some(allowed), Some(delayed)) = (
                            window_stats.get("requests_allowed").and_then(|v| v.as_u64()),
                            window_stats.get("requests_delayed").and_then(|v| v.as_u64())
                        ) {
                            let total = allowed + delayed;
                            if total > 0 {
                                let delay_rate = delayed as f64 / total as f64;
                                self.rate_limiter_delay_rate
                                    .entry(key.clone())
                                    .or_insert_with(Gauge::new)
                                    .set(delay_rate);
                                    
                                tracing::trace!(
                                    "Rate limiter delay rate for {}/{}: {:.2}% ({}/{})",
                                    key.flow_name,
                                    key.stage_name,
                                    delay_rate * 100.0,
                                    delayed,
                                    total
                                );
                            }
                        }
                        
                        // Store utilization metric
                        if let Some(utilization) = window_stats.get("utilization").and_then(|v| v.as_f64()) {
                            self.rate_limiter_utilization
                                .entry(key.clone())
                                .or_insert_with(Gauge::new)
                                .set(utilization);
                                
                            tracing::trace!(
                                "Rate limiter utilization for {}/{}: {:.2}%",
                                key.flow_name,
                                key.stage_name,
                                utilization * 100.0
                            );
                        }
                    }
                }
            }
        }
    }
    
    /// Process anomaly events
    fn process_anomaly_event(&mut self, event: &ChainEvent, key: &StageKey) {
        // Increment anomaly counter
        self.anomalies_total
            .entry(key.clone())
            .or_insert_with(Counter::new)
            .increment();
            
        tracing::info!(
            "Anomaly detected for {}/{}: {:?}",
            key.flow_name,
            key.stage_name,
            event.payload
        );
        
        // TODO: Track anomaly types if provided
        if let Some(anomaly_type) = event.payload.get("anomaly_type").and_then(|v| v.as_str()) {
            tracing::trace!("Anomaly type: {}", anomaly_type);
        }
    }
    
    /// Process amendment events (lifecycle changes)
    fn process_amendment_event(&mut self, event: &ChainEvent, key: &StageKey) {
        // Increment amendment counter
        self.amendments_total
            .entry(key.clone())
            .or_insert_with(Counter::new)
            .increment();
            
        tracing::info!(
            "Amendment event for {}/{}: {}",
            key.flow_name,
            key.stage_name,
            event.event_type
        );
    }
    
    /// Update dropped events metric (source events - sink events)
    fn update_dropped_events(&mut self, flow_name: &str) {
        // Debug: log all stage names for this flow
        let flow_stages: Vec<_> = self.events_total.iter()
            .filter(|(key, _)| key.flow_name == flow_name)
            .map(|(key, counter)| format!("{}: {}", key.stage_name, counter.get()))
            .collect();
        if !flow_stages.is_empty() {
            tracing::debug!("Stages for flow '{}': {:?}", flow_name, flow_stages);
        }
        
        // Calculate total source events for this flow
        let source_events: u64 = self.events_total.iter()
            .filter(|(key, _)| key.flow_name == flow_name && key.stage_name.contains("source"))
            .map(|(_, counter)| counter.get())
            .sum();
            
        // Calculate total sink events for this flow
        let sink_events: u64 = self.events_total.iter()
            .filter(|(key, _)| key.flow_name == flow_name && key.stage_name.contains("sink"))
            .map(|(_, counter)| counter.get())
            .sum();
            
        // Dropped events = source events - sink events
        let dropped = source_events.saturating_sub(sink_events) as f64;
        
        self.dropped_events
            .entry(flow_name.to_string())
            .or_insert_with(Gauge::new)
            .set(dropped);
            
        tracing::debug!("Flow '{}' dropped events calculation: {} source - {} sink = {} dropped", 
            flow_name, source_events, sink_events, dropped);
    }
    
    /// Create a snapshot of current metrics for export
    fn create_app_snapshot(&self) -> obzenflow_core::metrics::AppMetricsSnapshot {
        let mut snapshot = obzenflow_core::metrics::AppMetricsSnapshot::default();
        
        tracing::debug!("Creating snapshot: {} event counters, {} error counters, {} duration histograms", 
            self.events_total.len(), self.errors_total.len(), self.duration_seconds.len());
        tracing::warn!("MetricsAggregator@{:p} creating snapshot: {} event counters, {} error counters, {} duration histograms", 
            self as *const _, self.events_total.len(), self.errors_total.len(), self.duration_seconds.len());
        
        // Aggregate event counts by flow:stage
        for (stage_key, counter) in &self.events_total {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            let count = counter.get();
            tracing::info!("Adding event count: {} = {}", key, count);
            snapshot.event_counts.insert(key, count);
        }
        
        // Aggregate error counts by flow:stage
        for (stage_key, counter) in &self.errors_total {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.error_counts.insert(key, counter.get());
        }
        
        // Convert histograms to snapshots
        for (stage_key, histogram) in &self.duration_seconds {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.processing_times.insert(key, histogram.snapshot());
        }
        
        // In-flight gauges
        for (stage_key, gauge) in &self.in_flight {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.in_flight.insert(key, gauge.get());
        }
        
        // Queue depth gauges
        for (stage_key, gauge) in &self.queue_depth {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.queue_depth.insert(key, gauge.get());
        }
        
        // CPU usage ratios
        for (stage_key, gauge) in &self.cpu_usage_ratio {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.cpu_usage_ratio.insert(key, gauge.get());
        }
        
        // Memory bytes
        for (stage_key, gauge) in &self.memory_bytes {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.memory_bytes.insert(key, gauge.get());
        }
        
        // SAAFE metrics - anomalies
        for (stage_key, counter) in &self.anomalies_total {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.anomalies_total.insert(key, counter.get());
        }
        
        // SAAFE metrics - amendments
        for (stage_key, counter) in &self.amendments_total {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.amendments_total.insert(key, counter.get());
        }
        
        // SAAFE metrics - saturation
        for (stage_key, gauge) in &self.saturation_ratio {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.saturation_ratio.insert(key, gauge.get());
        }
        
        // Flow-level latency
        for (flow_name, histogram) in &self.flow_latency_seconds {
            snapshot.flow_latency_seconds.insert(flow_name.clone(), histogram.snapshot());
        }
        
        // Dropped events
        for (flow_name, gauge) in &self.dropped_events {
            snapshot.dropped_events.insert(flow_name.clone(), gauge.get());
        }
        
        // Circuit breaker metrics
        for (stage_key, gauge) in &self.circuit_breaker_state {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.circuit_breaker_state.insert(key, gauge.get());
        }
        
        for (stage_key, gauge) in &self.circuit_breaker_rejection_rate {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.circuit_breaker_rejection_rate.insert(key, gauge.get());
        }
        
        for (stage_key, gauge) in &self.circuit_breaker_consecutive_failures {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.circuit_breaker_consecutive_failures.insert(key, gauge.get());
        }
        
        // Rate limiter metrics
        for (stage_key, gauge) in &self.rate_limiter_delay_rate {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.rate_limiter_delay_rate.insert(key, gauge.get());
        }
        
        for (stage_key, gauge) in &self.rate_limiter_utilization {
            let key = format!("{}:{}", stage_key.flow_name, stage_key.stage_name);
            snapshot.rate_limiter_utilization.insert(key, gauge.get());
        }
        
        snapshot
    }
    
    /// Track flow-level metrics based on stage type and correlation
    fn track_flow_metrics(&mut self, event: &ChainEvent, key: &StageKey) {
        // Only track if event has correlation ID
        let Some(correlation_id) = &event.correlation_id else {
            return;
        };
        
        match event.flow_context.stage_type {
            StageType::Source => {
                // Track flow start time
                self.flow_start_times.insert(correlation_id.clone(), std::time::Instant::now());
                
                tracing::debug!(
                    "Flow started: correlation_id={}, flow={}, source={}",
                    correlation_id,
                    key.flow_name,
                    key.stage_name
                );
            }
            StageType::Sink => {
                // Calculate flow latency if we have a start time
                if let Some(start_time) = self.flow_start_times.remove(correlation_id) {
                    let latency = start_time.elapsed();
                    let latency_seconds = latency.as_secs_f64();
                    
                    // Record in histogram by flow name
                    self.flow_latency_seconds
                        .entry(key.flow_name.clone())
                        .or_insert_with(Histogram::for_seconds)
                        .observe(latency_seconds);
                    
                    tracing::info!(
                        "Flow completed: correlation_id={}, flow={}, latency_ms={:.2}, sink={}",
                        correlation_id,
                        key.flow_name,
                        latency.as_millis(),
                        key.stage_name
                    );
                } else {
                    tracing::warn!(
                        "Sink event without matching source: correlation_id={}, flow={}, sink={}",
                        correlation_id,
                        key.flow_name,
                        key.stage_name
                    );
                }
            }
            StageType::Transform => {
                // Transforms don't affect flow-level metrics
            }
        }
    }
}

impl obzenflow_runtime_services::metrics::MetricsAggregator for EventMetricsAggregator {
    fn process_event(&mut self, envelope: &EventEnvelope) {
        tracing::debug!("MetricsAggregator process_event called with event type: {}", envelope.event.event_type);
        self.process_event_impl(envelope);
    }
    
    fn create_snapshot(&self) -> obzenflow_core::metrics::AppMetricsSnapshot {
        tracing::info!("MetricsAggregator trait create_snapshot called");
        // Call the internal implementation method
        self.create_app_snapshot()
    }
    
    fn reset(&mut self) {
        // Clear all metrics
        self.events_total.clear();
        self.errors_total.clear();
        self.duration_seconds.clear();
        self.in_flight.clear();
        self.queue_depth.clear();
        self.cpu_usage_ratio.clear();
        self.memory_bytes.clear();
        self.anomalies_total.clear();
        self.amendments_total.clear();
        self.saturation_ratio.clear();
        self.flow_start_times.clear();
        self.flow_latency_seconds.clear();
        self.dropped_events.clear();
        self.circuit_breaker_state.clear();
        self.circuit_breaker_rejection_rate.clear();
        self.circuit_breaker_consecutive_failures.clear();
        self.rate_limiter_delay_rate.clear();
        self.rate_limiter_utilization.clear();
        self.custom_metrics.clear();
        self.last_event_id = None;
    }
}

/// Factory for creating MetricsAggregator instances
pub struct MetricsAggregatorFactory;

impl MetricsAggregatorFactory {
    pub fn new() -> Self {
        Self
    }
}

impl obzenflow_runtime_services::metrics::MetricsAggregatorFactory for MetricsAggregatorFactory {
    fn create(&self) -> Box<dyn obzenflow_runtime_services::metrics::MetricsAggregator> {
        tracing::warn!("MetricsAggregatorFactory creating new instance");
        let aggregator = EventMetricsAggregator::new();
        let boxed: Box<dyn obzenflow_runtime_services::metrics::MetricsAggregator> = Box::new(aggregator);
        tracing::warn!("MetricsAggregatorFactory created Box at {:p}", &*boxed);
        boxed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::{
        WriterId, 
        ChainEvent,
        event::flow_context::{FlowContext, StageType},
        event::processing_info::ProcessingInfo,
    };
    use serde_json::json;
    
    fn create_test_envelope(
        flow_name: &str, 
        stage_name: &str, 
        processing_time_ms: u64,
        is_error: bool
    ) -> EventEnvelope {
        let outcome = if is_error {
            ProcessingOutcome::Error("test error".to_string())
        } else {
            ProcessingOutcome::Success
        };
        
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "test_event",
            json!({"test": "data"}),
        );
        
        // Set flow context
        event.flow_context = FlowContext {
            flow_id: format!("test_flow_{}", flow_name),
            flow_name: flow_name.to_string(),
            stage_name: stage_name.to_string(),
            stage_type: StageType::Transform,
        };
        
        // Set processing info
        event.processing_info = ProcessingInfo {
            processing_time_ms,
            outcome,
            ..Default::default()
        };
        
        EventEnvelope::new(WriterId::new(), event)
    }
    
    #[tokio::test]
    async fn test_process_data_event() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Process a successful event
        let envelope = create_test_envelope("test_flow", "test_stage", 100, false);
        aggregator.process_event_impl(&envelope);
        
        // Check metrics
        assert_eq!(aggregator.get_events_total("test_flow", "test_stage"), 1.0);
        assert_eq!(aggregator.get_errors_total("test_flow", "test_stage"), 0.0);
        
        let histogram = aggregator.get_duration_histogram("test_flow", "test_stage").unwrap();
        assert_eq!(histogram.count(), 1);
    }
    
    #[tokio::test]
    async fn test_process_error_event() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Process an error event
        let envelope = create_test_envelope("test_flow", "test_stage", 50, true);
        aggregator.process_event_impl(&envelope);
        
        // Check metrics
        assert_eq!(aggregator.get_events_total("test_flow", "test_stage"), 1.0);
        assert_eq!(aggregator.get_errors_total("test_flow", "test_stage"), 1.0);
    }
    
    #[tokio::test]
    async fn test_multiple_stages() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Process events from different stages
        let envelope1 = create_test_envelope("flow1", "stage1", 100, false);
        let envelope2 = create_test_envelope("flow1", "stage2", 200, false);
        let envelope3 = create_test_envelope("flow2", "stage1", 150, true);
        
        aggregator.process_event_impl(&envelope1);
        aggregator.process_event_impl(&envelope2);
        aggregator.process_event_impl(&envelope3);
        
        // Check metrics are separated by stage
        assert_eq!(aggregator.get_events_total("flow1", "stage1"), 1.0);
        assert_eq!(aggregator.get_events_total("flow1", "stage2"), 1.0);
        assert_eq!(aggregator.get_events_total("flow2", "stage1"), 1.0);
        
        assert_eq!(aggregator.get_errors_total("flow1", "stage1"), 0.0);
        assert_eq!(aggregator.get_errors_total("flow2", "stage1"), 1.0);
    }
    
    fn create_control_envelope(
        flow_name: &str,
        stage_name: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> EventEnvelope {
        let mut event = ChainEvent::control(event_type, payload);
        
        // Set flow context
        event.flow_context = FlowContext {
            flow_id: format!("test_flow_{}", flow_name),
            flow_name: flow_name.to_string(),
            stage_name: stage_name.to_string(),
            stage_type: StageType::Transform,
        };
        
        EventEnvelope::new(WriterId::new(), event)
    }
    
    #[tokio::test]
    async fn test_process_state_metrics() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Create state metrics event
        let envelope = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_METRICS_STATE,
            json!({
                "queue_depth": 42,
                "in_flight": 7
            })
        );
        
        aggregator.process_event(&envelope);
        
        // Check gauges were updated
        let key = StageKey {
            flow_name: "test_flow".to_string(),
            stage_name: "test_stage".to_string(),
        };
        
        assert_eq!(aggregator.queue_depth.get(&key).unwrap().get(), 42.0);
        assert_eq!(aggregator.in_flight.get(&key).unwrap().get(), 7.0);
    }
    
    #[tokio::test]
    async fn test_process_resource_metrics() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Create resource metrics event
        let envelope = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_METRICS_RESOURCE,
            json!({
                "cpu_usage_ratio": 0.75,
                "memory_bytes": 1073741824  // 1GB
            })
        );
        
        aggregator.process_event(&envelope);
        
        // Check gauges were updated
        let key = StageKey {
            flow_name: "test_flow".to_string(),
            stage_name: "test_stage".to_string(),
        };
        
        assert_eq!(aggregator.cpu_usage_ratio.get(&key).unwrap().get(), 0.75);
        assert_eq!(aggregator.memory_bytes.get(&key).unwrap().get(), 1073741824.0);
    }
    
    #[tokio::test]
    async fn test_process_middleware_state() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Create middleware state transition event
        let envelope = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_MIDDLEWARE_STATE,
            json!({
                "middleware": "circuit_breaker",
                "state_transition": {
                    "from": "closed",
                    "to": "open",
                    "reason": "threshold_exceeded"
                }
            })
        );
        
        // Should process without error
        aggregator.process_event(&envelope);
        // State transitions are logged, not stored as metrics
    }
    
    #[tokio::test]
    async fn test_process_middleware_summary() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Create middleware summary event
        let envelope = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_MIDDLEWARE_SUMMARY,
            json!({
                "middleware": "circuit_breaker",
                "window_duration_s": 10,
                "stats": {
                    "requests_processed": 1000,
                    "requests_rejected": 50,
                    "state": "open",
                    "consecutive_failures": 10
                }
            })
        );
        
        // Should process without error
        aggregator.process_event(&envelope);
        // Summaries are logged, not stored as metrics (for now)
    }
    
    #[tokio::test]
    async fn test_process_anomaly_event() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Create anomaly event
        let envelope = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_METRICS_ANOMALY,
            json!({
                "anomaly_type": "high_error_rate",
                "threshold": 0.05,
                "actual": 0.15
            })
        );
        
        aggregator.process_event(&envelope);
        
        // Check anomaly counter was incremented
        let key = StageKey {
            flow_name: "test_flow".to_string(),
            stage_name: "test_stage".to_string(),
        };
        
        assert_eq!(aggregator.anomalies_total.get(&key).unwrap().get(), 1);
    }
    
    #[tokio::test]
    async fn test_process_amendment_event() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Create amendment events
        let events = vec![
            create_control_envelope(
                "test_flow",
                "test_stage",
                "control.stage.started",
                json!({"timestamp": 1234567890})
            ),
            create_control_envelope(
                "test_flow",
                "test_stage",
                "control.stage.stopped",
                json!({"timestamp": 1234567900})
            ),
            create_control_envelope(
                "test_flow",
                "test_stage",
                "control.stage.reconfigured",
                json!({"config": "new_config"})
            ),
        ];
        
        for envelope in events {
            aggregator.process_event(&envelope);
        }
        
        // Check amendment counter
        let key = StageKey {
            flow_name: "test_flow".to_string(),
            stage_name: "test_stage".to_string(),
        };
        
        assert_eq!(aggregator.amendments_total.get(&key).unwrap().get(), 3);
    }
    
    #[tokio::test]
    async fn test_saturation_ratio_calculation() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Create state metrics event with max_queue_size
        let envelope = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_METRICS_STATE,
            json!({
                "queue_depth": 75,
                "max_queue_size": 100,
                "in_flight": 10
            })
        );
        
        aggregator.process_event(&envelope);
        
        // Check saturation ratio was calculated
        let key = StageKey {
            flow_name: "test_flow".to_string(),
            stage_name: "test_stage".to_string(),
        };
        
        assert_eq!(aggregator.saturation_ratio.get(&key).unwrap().get(), 0.75);
        assert_eq!(aggregator.queue_depth.get(&key).unwrap().get(), 75.0);
    }
    
    #[tokio::test]
    async fn test_middleware_state_metrics() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Test circuit breaker state transitions
        let envelope = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_MIDDLEWARE_STATE,
            json!({
                "middleware": "circuit_breaker",
                "state_transition": {
                    "from": "closed",
                    "to": "open",
                    "reason": "threshold_exceeded"
                }
            })
        );
        
        aggregator.process_event(&envelope);
        
        let key = StageKey {
            flow_name: "test_flow".to_string(),
            stage_name: "test_stage".to_string(),
        };
        
        // Check circuit breaker state gauge
        assert_eq!(aggregator.circuit_breaker_state.get(&key).unwrap().get(), 1.0);
        
        // Test half-open state
        let envelope2 = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_MIDDLEWARE_STATE,
            json!({
                "middleware": "circuit_breaker",
                "state_transition": {
                    "from": "open",
                    "to": "half_open",
                    "reason": "timeout_expired"
                }
            })
        );
        
        aggregator.process_event(&envelope2);
        assert_eq!(aggregator.circuit_breaker_state.get(&key).unwrap().get(), 0.5);
    }
    
    #[tokio::test]
    async fn test_middleware_summary_metrics() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Test circuit breaker metrics
        let envelope = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_MIDDLEWARE_SUMMARY,
            json!({
                "middleware": "circuit_breaker",
                "window_duration_s": 10,
                "stats": {
                    "requests_processed": 950,
                    "requests_rejected": 50,
                    "state": "open",
                    "consecutive_failures": 10
                }
            })
        );
        
        aggregator.process_event(&envelope);
        
        let key = StageKey {
            flow_name: "test_flow".to_string(),
            stage_name: "test_stage".to_string(),
        };
        
        // Check rejection rate (50/1000 = 0.05)
        assert_eq!(aggregator.circuit_breaker_rejection_rate.get(&key).unwrap().get(), 0.05);
        // Check consecutive failures
        assert_eq!(aggregator.circuit_breaker_consecutive_failures.get(&key).unwrap().get(), 10.0);
        
        // Test rate limiter metrics
        let envelope2 = create_control_envelope(
            "test_flow",
            "test_stage",
            ChainEvent::CONTROL_MIDDLEWARE_SUMMARY,
            json!({
                "middleware": "rate_limiter",
                "window_duration_s": 10,
                "stats": {
                    "window_stats": {
                        "requests_allowed": 800,
                        "requests_delayed": 200,
                        "utilization": 0.85
                    }
                }
            })
        );
        
        aggregator.process_event(&envelope2);
        
        // Check delay rate (200/1000 = 0.2)
        assert_eq!(aggregator.rate_limiter_delay_rate.get(&key).unwrap().get(), 0.2);
        // Check utilization
        assert_eq!(aggregator.rate_limiter_utilization.get(&key).unwrap().get(), 0.85);
    }
    
    #[tokio::test]
    async fn test_flow_level_metrics() {
        use obzenflow_core::event::correlation::{CorrelationId, CorrelationPayload};
        
        let mut aggregator = EventMetricsAggregator::new();
        let correlation_id = CorrelationId::new();
        
        // Create source event with correlation
        let mut source_event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "test_data",
            json!({"value": 100}),
        );
        source_event.correlation_id = Some(correlation_id);
        source_event.correlation_payload = Some(CorrelationPayload::new("http_source", source_event.id));
        source_event.flow_context = FlowContext {
            flow_id: "test_flow_123".to_string(),
            flow_name: "test_flow".to_string(),
            stage_name: "http_source".to_string(),
            stage_type: StageType::Source,
        };
        source_event.processing_info = ProcessingInfo {
            processing_time_ms: 10,
            outcome: ProcessingOutcome::Success,
            ..Default::default()
        };
        
        let source_envelope = EventEnvelope::new(WriterId::new(), source_event);
        aggregator.process_event(&source_envelope);
        
        // Verify source tracking
        assert!(aggregator.flow_start_times.contains_key(&correlation_id));
        
        // Simulate some processing time
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        
        // Create sink event with same correlation
        let mut sink_event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "test_result",
            json!({"result": 200}),
        );
        sink_event.correlation_id = Some(correlation_id);
        sink_event.flow_context = FlowContext {
            flow_id: "test_flow_123".to_string(),
            flow_name: "test_flow".to_string(),
            stage_name: "http_sink".to_string(),
            stage_type: StageType::Sink,
        };
        sink_event.processing_info = ProcessingInfo {
            processing_time_ms: 20,
            outcome: ProcessingOutcome::Success,
            ..Default::default()
        };
        
        let sink_envelope = EventEnvelope::new(WriterId::new(), sink_event);
        aggregator.process_event(&sink_envelope);
        
        // Verify flow metrics
        assert!(!aggregator.flow_start_times.contains_key(&correlation_id));
        assert!(aggregator.flow_latency_seconds.contains_key("test_flow"));
        
        let histogram = aggregator.flow_latency_seconds.get("test_flow").unwrap();
        assert_eq!(histogram.count(), 1);
        assert!(histogram.sum() >= 0.05); // At least 50ms
    }
    
    #[tokio::test]
    async fn test_flow_without_correlation() {
        let mut aggregator = EventMetricsAggregator::new();
        
        // Create source event without correlation
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "test_data",
            json!({}),
        );
        event.flow_context = FlowContext {
            flow_id: "test_flow_123".to_string(),
            flow_name: "test_flow".to_string(),
            stage_name: "source".to_string(),
            stage_type: StageType::Source,
        };
        event.processing_info = ProcessingInfo {
            processing_time_ms: 10,
            outcome: ProcessingOutcome::Success,
            ..Default::default()
        };
        
        let envelope = EventEnvelope::new(WriterId::new(), event);
        aggregator.process_event(&envelope);
        
        // Should not track without correlation ID
        assert!(aggregator.flow_start_times.is_empty());
        assert!(aggregator.flow_latency_seconds.is_empty());
    }
}