// src/event_sourcing.rs
//! Event Sourcing Architecture for flowstate_rs
//!
//! This module implements true event sourcing where:
//! 1. Each stage reads ONLY from the EventStore (never from in-memory channels)
//! 2. Each stage writes ONLY to the EventStore with vector clock causality
//! 3. Processing is deterministic and can be replayed
//! 4. No state is lost if a stage fails - everything is in the EventStore
//! 5. Stages can be developed and tested independently
//! 6. Vector clocks provide causal consistency across parallel workers

use crate::chain_event::ChainEvent;
use crate::step::Result;
use crate::lifecycle::{EventHandler, ProcessingMode, BehaviorCondition, HandlerState};
use crate::event_store::{EventStore, EventWriter, EventEnvelope, StageSemantics, EventSubscription, SubscriptionFilter, SubscriptionEvent};
use crate::event_store::constants::SOURCE_IDLE_SLEEP;
use crate::event_types::{
    FlowContext, StageType as EventStageType,
    CausalityInfo, ProcessingInfo, BoundaryType, CorrelationId
};
use crate::topology::{StageId, PipelineTopology, ShutdownSignal, StageLifecycleHandle, LayeredPipelineLifecycle};
// Monitoring is now handled by middleware
// Removed broadcast - using StageShutdownHandle instead
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use ulid::Ulid;

// ShutdownSignal moved to topology::shutdown module

/// State machine for stage execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StageState {
    Running,        // Normal operation
    Draining,       // Shutdown requested, still processing
    ReadyToDrain,   // All upstreams done, waiting for in-flight
    Drained,        // Complete, ready to exit
}

/// Event sourced stage that reads from EventStore with causal consistency
pub struct EventSourcedStage<H: EventHandler> {
    handler: H,
    stage_id: StageId,
    stage_name: String,
    writer: Option<EventWriter>,
    semantics: StageSemantics,
    /// Track processed events to avoid reprocessing
    processed: HashMap<Ulid, EventEnvelope>,
    /// Unified lifecycle handle for init and shutdown
    lifecycle: StageLifecycleHandle,
    /// Push-based subscription for receiving events
    subscription: Option<EventSubscription>,
    /// Pipeline topology for lookups
    topology: Arc<PipelineTopology>,
    /// Flow context for event enrichment
    flow_context: Option<FlowContext>,
    /// Current correlation ID for tracing related events
    current_correlation_id: Option<CorrelationId>,
    /// Processing mode of the handler
    processing_mode: ProcessingMode,
    /// Whether this stage acts as a source (generates events without input)
    is_source: bool,
    /// Reference to event store for signaling completion (FLOWIP-058)
    event_store: Arc<EventStore>,
    /// Track if this stage completed naturally (FLOWIP-058)
    natural_completion: bool,
    /// Shared flag to mark adapter as drained (FLOWIP-074)
    adapter_drained_flag: Option<Arc<AtomicBool>>,
}

impl<H: EventHandler> EventSourcedStage<H> {
    /// Create a new builder for constructing an EventSourcedStage
    pub fn builder() -> EventSourcedStageBuilder<H> {
        EventSourcedStageBuilder::new()
    }
    
    /// Check if source handler has naturally completed
    /// For now returns false - actual behavior checking happens in process_new_events
    /// where we have mutable access via interior mutability patterns
    fn check_source_complete(&self) -> bool {
        // BehaviorEventHandler checking is handled in process_new_events
        // where we can properly handle the behavior update cycle
        false
    }
    
    /// Enrich a ChainEvent with all metadata inline
    /// This ensures deterministic event creation for replay scenarios
    fn enrich_event(
        &mut self,
        mut event: ChainEvent,
        parent: Option<&EventEnvelope>,
        processing_start: std::time::Instant,
    ) -> ChainEvent {
        // Get or create flow context
        if self.flow_context.is_none() {
            // Try to get flow context from parent or create a default one
            if let Some(parent_envelope) = parent {
                self.flow_context = Some(parent_envelope.event.flow_context.clone());
            }
            
            // If still none, create from topology
            if self.flow_context.is_none() {
                self.flow_context = Some(FlowContext {
                    flow_name: self.topology.flow_name(),
                    flow_id: self.topology.flow_id(),
                    stage_name: self.stage_name.clone(),
                    stage_type: if self.is_source {
                        EventStageType::Source
                    } else {
                        match self.processing_mode {
                            ProcessingMode::Transform => EventStageType::Transform,
                            ProcessingMode::Observe => EventStageType::Transform, // Observers are like transforms
                            ProcessingMode::Aggregate => EventStageType::Transform, // Aggregators transform too
                        }
                    },
                });
            }
        }
        
        // Update writer ID with the stage's writer ID
        if let Some(ref writer) = self.writer {
            event.writer_id = writer.writer_id().clone();
        }
        
        // Build causality info from parent
        event.causality = if let Some(parent_envelope) = parent {
            // Extract correlation ID from parent if we don't have one
            if self.current_correlation_id.is_none() {
                self.current_correlation_id = parent_envelope.event.processing_info.correlation_id.clone();
            }
            
            // Get parent event ID
            let parent_id = parent_envelope.event.ulid;
            
            CausalityInfo {
                parent_ids: vec![parent_id],
            }
        } else {
            // No parent - this might be a source event
            CausalityInfo {
                parent_ids: Vec::new(),
            }
        };
        
        // Set flow context
        if let Some(ref flow_context) = self.flow_context {
            event.flow_context = flow_context.clone();
        }
        
        // Calculate processing time
        let processing_time_ms = processing_start.elapsed().as_millis() as u64;
        
        // Determine boundary type
        let stage_type = if self.is_source {
            EventStageType::Source
        } else {
            match self.processing_mode {
                ProcessingMode::Transform => EventStageType::Transform,
                ProcessingMode::Observe => EventStageType::Transform, // Observers are like transforms
                ProcessingMode::Aggregate => EventStageType::Transform, // Aggregators transform too
            }
        };
        
        let is_boundary_event = match stage_type {
            EventStageType::Source => {
                // Source events without parent are flow entries
                if parent.is_none() {
                    // Also generate a new correlation ID for new flows
                    if self.current_correlation_id.is_none() {
                        self.current_correlation_id = Some(crate::event_types::new_correlation_id());
                    }
                    Some(BoundaryType::FlowEntry)
                } else {
                    None
                }
            }
            EventStageType::Sink => Some(BoundaryType::FlowExit),
            _ => None,
        };
        
        // Stage position could be computed from topology traversal
        // For now, we'll leave it None
        let stage_position = None;
        
        // Update processing info
        event.processing_info = ProcessingInfo {
            processed_by: self.stage_name.clone(),
            processing_time_ms,
            taxonomy: None, // Taxonomy is now tracked in middleware
            event_time_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            stage_position,
            outcome: event.processing_info.outcome, // Preserve if set by stage
            is_boundary_event,
            correlation_id: self.current_correlation_id.clone(),
        };
        
        event
    }

    /// Process any new events from the EventStore
    async fn process_new_events(&mut self, state: &mut StageState) -> Result<usize> {
        let mut processed_count = 0;
        
        // Process subscribed events if we have a subscription
        if let Some(ref mut subscription) = self.subscription {
            // Get shutdown receiver from lifecycle handle
            let mut shutdown_rx = self.lifecycle.shutdown_receiver().await;
            
            // Use recv_with_eof for deterministic shutdown (FLOWIP-058)
            let subscription_event = if let Some(ref mut rx) = shutdown_rx {
                tokio::select! {
                    // Normal event/EOF processing
                    event = subscription.recv_with_eof() => event?,
                    
                    // Shutdown signal received
                    signal = rx.recv() => {
                        match signal {
                            Ok(ShutdownSignal::BeginDrain) => {
                                *state = StageState::Draining;
                                // Continue to process normally
                                subscription.recv_with_eof().await?
                            }
                            Ok(ShutdownSignal::ForceShutdown(reason)) => {
                                return Err(format!("Force shutdown: {}", reason).into());
                            }
                            Err(_) => {
                                // Channel closed, treat as drain
                                *state = StageState::Draining;
                                subscription.recv_with_eof().await?
                            }
                        }
                    }
                }
            } else {
                // No shutdown receiver, just get events normally
                subscription.recv_with_eof().await?
            };
            
            match subscription_event {
                SubscriptionEvent::Events(events) => {
                    for envelope in events {
                // Skip already processed
                if self.processed.contains_key(&envelope.event.ulid) {
                    continue;
                }
                
                // Track event entering this stage
                // TODO: Add metrics via middleware instead
                
                // Note: Source completion events are not processed or forwarded.
                // Sources detect their own completion when emitting (see source generation section).
                // Downstream stages receive EOF signals through SubscriptionEvent::EndOfStream.
                
                // Check for shutdown event
                if envelope.event.event_type == "_shutdown" {
                    // Propagate shutdown to downstream stages
                    // For now, always propagate - we can add sink detection later if needed
                    if let Some(ref mut writer) = self.writer {
                        let _ = writer.append(envelope.event.clone(), Some(&envelope)).await;
                    }
                    // Mark as processed but don't increment count
                    self.processed.insert(envelope.event.ulid.clone(), envelope);
                    // Event completed
                    // TODO: Add metrics via middleware instead
                    // Move to draining state
                    *state = StageState::Draining;
                    continue;
                }
                
                // Track processing start time
                let start_time = std::time::Instant::now();
                
                // Don't process source completion events through handlers
                let results = if envelope.event.event_type == "flowstate.source.complete" {
                    // Just mark as processed, don't forward
                    vec![]
                } else {
                    // Process normal events based on handler mode
                    match self.processing_mode {
                        ProcessingMode::Transform => {
                            self.handler.transform(envelope.event.clone())
                        }
                        ProcessingMode::Observe => {
                            // Observers don't produce output events
                            self.handler.observe(&envelope.event)?;
                            vec![]
                        }
                        ProcessingMode::Aggregate => {
                            // TODO: Aggregate mode not yet supported with Arc<H>
                            tracing::warn!("Aggregate mode not yet supported in EventSourcedStage");
                            vec![]
                        }
                    }
                };
                
                // Enrich all results first (before borrowing writer)
                let enriched_results: Vec<ChainEvent> = results
                    .into_iter()
                    .map(|result| self.enrich_event(result, Some(&envelope), start_time))
                    .collect();
                
                // Now write the enriched results
                let writer = self.writer.as_mut().ok_or("Writer not initialized")?;
                for enriched in enriched_results {
                    tracing::debug!("Stage '{}' writing event type '{}'", self.stage_name, enriched.event_type);
                    writer.append(enriched, Some(&envelope)).await?;
                }
                
                self.processed.insert(envelope.event.ulid.clone(), envelope);
                processed_count += 1;
                
                        // Event completed
                        // TODO: Add metrics via middleware instead
                    }
                }
                
                SubscriptionEvent::EndOfStream { stage_id, natural_completion, .. } => {
                    tracing::info!(
                        "Stage '{}' received EOF from upstream stage {:?} (natural: {})", 
                        self.stage_name, stage_id, natural_completion
                    );
                    // Mark upstream as complete in lifecycle handle
                    self.lifecycle.mark_upstream_complete(stage_id).await;
                    // Continue processing - other upstreams might have events
                }
                
                SubscriptionEvent::AllUpstreamsComplete => {
                    tracing::info!(
                        "Stage '{}' received EOF from all upstream stages", 
                        self.stage_name
                    );
                    // All upstreams are done - transition to draining
                    match *state {
                        StageState::Running => {
                            tracing::info!("Stage '{}' transitioning to Draining (all upstreams complete)", self.stage_name);
                            *state = StageState::Draining;
                        }
                        StageState::Draining => {
                            *state = StageState::ReadyToDrain;
                        }
                        _ => {
                            // Already draining or drained
                        }
                    }
                }
            }
        }
        
        // Sources generate events (even if they also have subscriptions!)
        if self.is_source {
            // Don't generate new events if we're draining
            if *state == StageState::Draining {
                return Ok(0);
            }
            
            // Sources without subscriptions need to check for shutdown signal
            if self.subscription.is_none() {
                // TODO: Check shutdown receiver for sources without subscriptions
                // For now, they won't receive shutdown signals until we implement this
            }
            
            // Track processing start time
            let start_time = std::time::Instant::now();
            
            let tick_event = ChainEvent::new("_tick", serde_json::json!({}));
            let results = match self.processing_mode {
                ProcessingMode::Transform => {
                    self.handler.transform(tick_event)
                }
                _ => {
                    // Sources should only use transform mode
                    vec![]
                }
            };
            
            // Check results BEFORE enrichment to detect our own completion
            for result in &results {
                if result.event_type == "flowstate.source.complete" {
                    if let Some(stage_id_str) = result.payload.get("stage_id").and_then(|v| v.as_str()) {
                        if stage_id_str == self.stage_id.to_string() {
                            // We're emitting our own completion - transition immediately!
                            let natural = result.payload.get("natural")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            tracing::info!("Source '{}' emitting completion event (natural: {})", 
                                self.stage_name, natural);
                            self.natural_completion = natural;
                            *state = StageState::Draining;
                            // Continue to write the event for audit trail
                        }
                    }
                }
            }
            
            // Enrich all results (including completion event)
            let enriched_results: Vec<ChainEvent> = results
                .into_iter()
                .map(|result| self.enrich_event(result, None, start_time))
                .collect();
            
            // Write all enriched results to EventStore
            let writer = self.writer.as_mut().ok_or("Writer not initialized")?;
            for enriched in enriched_results {
                tracing::debug!("Source '{}' writing event type '{}'", self.stage_name, enriched.event_type);
                writer.append(enriched, None).await?;
                processed_count += 1;
            }
        }

        Ok(processed_count)
    }

    /// Run the stage continuously with shutdown support
    pub async fn run(&mut self) -> Result<()> {
        // Signal that this stage is ready (subscriptions are set up)
        self.lifecycle.signal_ready().await;
        
        // Sources must wait for ALL stages to be ready before emitting
        if self.is_source {
            tracing::info!("Source '{}' synchronized - all stages ready", self.stage_name);
            // The signal_ready() call above already includes the barrier wait
        }
        
        // State machine for stage lifecycle
        let mut state = StageState::Running;
        
        loop {
            match state {
                StageState::Running => {
                    match self.process_new_events(&mut state).await {
                        Ok(0) => {
                            // No events processed
                            if state == StageState::Draining {
                                // State was changed to Draining by process_new_events
                                continue;
                            }
                            
                            // Sources need to sleep when they have no events to avoid busy-waiting.
                            // This is critical for:
                            // - Infinite sources with bursty traffic (e.g., Kafka consumers, HTTP endpoints)
                            // - Rate-limited sources waiting for their next allowed emission
                            // - Sources polling external systems that may temporarily have no data
                            // - File watchers waiting for new data to be written
                            // 
                            // This sleep is NOT about completion detection - completed sources will have
                            // already emitted a flowstate.source.complete event and transitioned to Draining.
                            // This is purely about CPU efficiency during temporary "no data" periods.
                            if self.is_source {
                                tokio::time::sleep(SOURCE_IDLE_SLEEP).await;
                            }
                            // For other stages, recv_with_eof() blocks naturally
                        }
                        Ok(count) => {
                            if count > 0 {
                                tracing::debug!(
                                    "Stage '{}' processed {} events", 
                                    self.stage_name, count
                                );
                            }
                        }
                        Err(e) if e.to_string().contains("Force shutdown") => {
                            tracing::info!("Stage '{}' force shutting down: {}", self.stage_name, e);
                            // EventHandlers don't have shutdown method - that's handled by Drainable trait
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::error!("Stage '{}' error: {}", self.stage_name, e);
                            // Continue processing - don't fail on individual errors
                        }
                    }
                }
                
                StageState::Draining => {
                    // We're draining - try to process any remaining events
                    match self.process_new_events(&mut state).await {
                        Ok(0) => {
                            // No events processed - check if we can drain
                            if self.is_source && self.subscription.is_none() {
                                // Source with no upstreams - can drain immediately
                                tracing::info!("Source '{}' transitioning to Drained (no upstreams)", self.stage_name);
                                state = StageState::Drained;
                            } else if !self.lifecycle.has_pending_work().await {
                                // Stage with upstreams - check pending work
                                tracing::info!("Stage '{}' transitioning to Drained (no pending work)", self.stage_name);
                                state = StageState::Drained;
                            }
                            // No sleep needed! recv_with_eof will be notified of new events
                        }
                        Ok(count) => {
                            tracing::debug!(
                                "Stage '{}' drained {} events", 
                                self.stage_name, count
                            );
                            // Successfully processed events, loop immediately to process more
                        }
                        Err(e) if e.to_string().contains("Force shutdown") => {
                            tracing::info!("Stage '{}' force shutting down during drain: {}", self.stage_name, e);
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::error!("Stage '{}' error during drain: {}", self.stage_name, e);
                            // Continue without sleep - errors shouldn't delay drain
                        }
                    }
                }
                
                StageState::ReadyToDrain => {
                    // All upstreams complete, wait for in-flight to finish
                    self.lifecycle.wait_for_drain_ready().await;
                    state = StageState::Drained;
                }
                
                StageState::Drained => {
                    // Use the tracked natural completion status
                    let natural = self.natural_completion;
                    
                    // Signal completion to EventStore (FLOWIP-058)
                    tracing::info!("Stage '{}' signaling completion to EventStore (natural: {})", self.stage_name, natural);
                    self.event_store.signal_stage_complete(self.stage_id, natural).await?;
                    
                    // Signal completion to lifecycle
                    self.lifecycle.signal_drained().await;
                    
                    // Also mark the adapter as drained if we have the flag (FLOWIP-074)
                    if let Some(ref flag) = self.adapter_drained_flag {
                        flag.store(true, Ordering::SeqCst);
                        tracing::debug!("Stage '{}' marked adapter as drained", self.stage_name);
                    }
                    
                    tracing::info!("Stage '{}' cleanly shut down", self.stage_name);
                    return Ok(());
                }
            }
        }
    }

    /// Process events once and exit (useful for batch processing)
    pub async fn run_once(&mut self) -> Result<usize> {
        let mut state = StageState::Running;
        self.process_new_events(&mut state).await
    }
}

/// Handle for controlling a running flow
///
/// Provides graceful shutdown and monitoring capabilities,
/// inspired by stream processing frameworks like Pekko Streams.
pub struct FlowHandle {
    /// Handles to all running stage tasks
    stage_handles: Vec<tokio::task::JoinHandle<Result<()>>>,
    /// Lifecycle coordinator for shutdown
    lifecycle: Arc<LayeredPipelineLifecycle>,
    /// Track if shutdown has been called
    shutdown_called: Arc<std::sync::atomic::AtomicBool>,
    /// Event store for statistics on shutdown
    event_store: Arc<EventStore>,
    /// Handle to the natural completion monitor (FLOWIP-074)
    completion_monitor: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl FlowHandle {
    /// Create a new flow handle
    pub fn new(
        stage_handles: Vec<tokio::task::JoinHandle<Result<()>>>,
        lifecycle: Arc<LayeredPipelineLifecycle>,
        event_store: Arc<EventStore>,
    ) -> Self {
        // Spawn natural completion monitor (FLOWIP-074)
        let monitor_handle = {
            let lifecycle = lifecycle.clone();
            tokio::spawn(async move {
                lifecycle.monitor_completion().await
            })
        };
        
        Self {
            stage_handles,
            lifecycle,
            shutdown_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            event_store,
            completion_monitor: Some(monitor_handle),
        }
    }

    /// Wait for natural completion of all stages
    ///
    /// Returns when all stage tasks have exited naturally (without shutdown signal)
    pub async fn wait_for_completion(&mut self) -> Result<()> {
        use std::sync::atomic::Ordering;

        // Check if already shutting down
        if self.shutdown_called.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Take ownership of handles to consume them
        let handles = std::mem::take(&mut self.stage_handles);
        let monitor_handle = self.completion_monitor.take();
        
        // Wait for all stage tasks to complete naturally
        let mut errors = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {
                    // Stage completed naturally
                },
                Ok(Err(e)) => errors.push(e.to_string()),
                Err(e) if e.is_cancelled() => {},
                Err(e) => errors.push(format!("Task panic: {}", e)),
            }
        }
        
        // Also wait for the completion monitor (FLOWIP-074)
        if let Some(monitor) = monitor_handle {
            match monitor.await {
                Ok(Ok(())) => {
                    tracing::info!("Natural completion monitor finished successfully");
                },
                Ok(Err(e)) => {
                    tracing::warn!("Completion monitor error: {}", e);
                    errors.push(format!("Monitor error: {}", e));
                },
                Err(e) if e.is_cancelled() => {},
                Err(e) => errors.push(format!("Monitor panic: {}", e)),
            }
        }
        
        // Mark that shutdown happened naturally
        self.shutdown_called.store(true, Ordering::SeqCst);
        
        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!("Flow completion errors: {}", errors.join(", ")).into())
        }
    }

    /// Gracefully shutdown the flow
    ///
    /// Allows stages to finish processing current events before shutting down.
    pub async fn shutdown(mut self) -> Result<()> {
        use std::sync::atomic::Ordering;

        // First check if stages have already completed or might complete naturally
        if !self.stage_handles.is_empty() {
            // Check if any stages have already exited
            let all_completed = self.stage_handles.iter().all(|h| h.is_finished());
            
            if all_completed {
                // All stages already completed! Just collect the results
                let result = self.wait_for_completion().await;
                match result {
                    Ok(()) => {
                        // Print statistics and return
                        let stats = self.event_store.get_statistics().await;
                        println!("\n📊 Event log summary:");
                        println!("   Location: {}", stats.path.display());
                        println!("   Segments: {}", stats.segment_count);
                        println!("   Total size: {:.2} MB", stats.total_size_bytes as f64 / (1024.0 * 1024.0));
                        println!("   Events written: {}", stats.event_count);
                        println!("\n✅ Flow completed naturally!");
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                }
            } else {
                // Give stages a brief chance to complete naturally
                let wait_for_natural = self.wait_for_completion();
                
                tokio::select! {
                    result = wait_for_natural => {
                        // Stages completed naturally during our wait!
                        match result {
                            Ok(()) => {
                                // Print statistics and return
                                let stats = self.event_store.get_statistics().await;
                                println!("\n📊 Event log summary:");
                                println!("   Location: {}", stats.path.display());
                                println!("   Segments: {}", stats.segment_count);
                                println!("   Total size: {:.2} MB", stats.total_size_bytes as f64 / (1024.0 * 1024.0));
                                println!("   Events written: {}", stats.event_count);
                                println!("\n✅ Flow completed naturally!");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    // If natural completion doesn't happen quickly, proceed with shutdown
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(200)) => {
                        // Continue with explicit shutdown
                    }
                }
            }
        }

        // Ensure shutdown is only called once
        if self.shutdown_called.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Send graceful shutdown signal
        let _ = self.lifecycle.begin_shutdown().await;

        // Cancel the completion monitor since we're doing explicit shutdown (FLOWIP-074)
        if let Some(monitor) = self.completion_monitor.take() {
            monitor.abort();
        }

        // Wait for all stages to complete
        let mut errors = Vec::new();
        for handle in self.stage_handles {
            match handle.await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => errors.push(e.to_string()),
                Err(e) if e.is_cancelled() => {},
                Err(e) => errors.push(format!("Task panic: {}", e)),
            }
        }

        // Print event store statistics before returning
        let stats = self.event_store.get_statistics().await;
        println!("\n📊 Event log summary:");
        println!("   Location: {}", stats.path.display());
        println!("   Segments: {}", stats.segment_count);
        println!("   Total size: {:.2} MB", stats.total_size_bytes as f64 / (1024.0 * 1024.0));
        println!("   Events written: {}", stats.event_count);
        
        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!("Flow shutdown errors: {}", errors.join(", ")).into())
        }
    }

    /// Immediately abort the flow
    pub fn abort(mut self, reason: &str) {
        use std::sync::atomic::Ordering;

        if self.shutdown_called.swap(true, Ordering::SeqCst) {
            return;
        }

        // Send immediate shutdown signal - fire and forget
        let lifecycle = self.lifecycle.clone();
        let reason = reason.to_string();
        tokio::spawn(async move {
            let _ = lifecycle.force_shutdown(&reason).await;
        });

        // Cancel all tasks
        for handle in self.stage_handles {
            handle.abort();
        }
        
        // Also cancel the completion monitor (FLOWIP-074)
        if let Some(monitor) = self.completion_monitor.take() {
            monitor.abort();
        }
    }
}
//
// Then in the EventSourcedStage, write with no parent:
// writer.append(event, None).await?;

// TODO: Add macro support for event-sourced flows integrated with flow! DSL

/// Builder for constructing EventSourcedStage with proper initialization
pub struct EventSourcedStageBuilder<H: EventHandler> {
    handler: Option<H>,
    store: Option<Arc<EventStore>>,
    stage_id: Option<StageId>,
    stage_name: Option<String>,
    topology: Option<Arc<PipelineTopology>>,
    pipeline_lifecycle: Option<Arc<LayeredPipelineLifecycle>>,
    stage_lifecycle_handle: Option<StageLifecycleHandle>,
    is_source: bool,
    processing_mode: Option<ProcessingMode>,
    adapter_drained_flag: Option<Arc<AtomicBool>>,
}

impl<H: EventHandler> EventSourcedStageBuilder<H> {
    pub fn new() -> Self {
        Self {
            handler: None,
            store: None,
            stage_id: None,
            stage_name: None,
            topology: None,
            pipeline_lifecycle: None,
            stage_lifecycle_handle: None,
            is_source: false,
            processing_mode: None,
            adapter_drained_flag: None,
        }
    }
    
    pub fn with_handler(mut self, handler: H) -> Self {
        self.handler = Some(handler);
        self
    }
    
    pub fn with_topology(mut self, id: StageId, name: String, topology: Arc<PipelineTopology>) -> Self {
        self.stage_id = Some(id);
        self.stage_name = Some(name);
        self.topology = Some(topology);
        self
    }
    
    pub fn is_source(mut self, is_source: bool) -> Self {
        self.is_source = is_source;
        self
    }
    
    pub fn with_store(mut self, store: Arc<EventStore>) -> Self {
        self.store = Some(store);
        self
    }
    
    pub fn with_pipeline_lifecycle(mut self, lifecycle: Arc<LayeredPipelineLifecycle>) -> Self {
        self.pipeline_lifecycle = Some(lifecycle);
        self
    }
    
    pub fn with_stage_lifecycle_handle(mut self, handle: StageLifecycleHandle) -> Self {
        self.stage_lifecycle_handle = Some(handle);
        self
    }
    
    pub fn with_adapter_drained_flag(mut self, flag: Arc<AtomicBool>) -> Self {
        self.adapter_drained_flag = Some(flag);
        self
    }
    
    pub async fn build(self) -> Result<EventSourcedStage<H>> {
        // Validate required fields
        let handler = self.handler.ok_or("Handler is required")?;
        let store = self.store.ok_or("EventStore is required")?;
        let stage_id = self.stage_id.ok_or("Stage ID is required")?;
        let stage_name = self.stage_name.ok_or("Stage name is required")?;
        let topology = self.topology.ok_or("Pipeline topology is required")?;
        
        // Get stage lifecycle - either from handle or create from pipeline lifecycle
        let lifecycle = if let Some(handle) = self.stage_lifecycle_handle {
            handle
        } else {
            let pipeline_lifecycle = self.pipeline_lifecycle.ok_or("Pipeline lifecycle is required")?;
            pipeline_lifecycle.stage_lifecycle(stage_id).await?
        };
        
        // For now, all stages use stateless semantics
        let semantics = StageSemantics::Stateless;
        
        // Determine processing mode from handler or builder override
        let processing_mode = self.processing_mode.unwrap_or_else(|| handler.processing_mode());
        let is_source = self.is_source;
        
        // Get upstream stages from topology
        let upstream_stage_ids = topology.upstream_stages(stage_id);
        
        // Create writer
        let writer = store.create_writer(stage_id, semantics.clone()).await?;
        
        // Create subscription if we have upstream stages
        let subscription = if !upstream_stage_ids.is_empty() {
            let filter = SubscriptionFilter {
                upstream_stages: upstream_stage_ids.to_vec(),
            };
            
            tracing::info!("Stage '{}' subscribing to {} upstream stages", 
                stage_name, upstream_stage_ids.len());
            
            match store.subscribe(filter).await {
                Ok(sub) => {
                    tracing::info!(
                        "Stage '{}' subscribed to {} upstream stages",
                        stage_name,
                        upstream_stage_ids.len()
                    );
                    Some(sub)
                }
                Err(e) => {
                    return Err(format!(
                        "Failed to create subscription for stage '{}': {}",
                        stage_name, e
                    ).into());
                }
            }
        } else {
            None
        };
        
        // Create initial flow context from topology
        // For now, we'll leave it as None and populate it on first event
        let flow_context = None;
        
        Ok(EventSourcedStage {
            handler,
            stage_id,
            stage_name,
            writer: Some(writer),
            semantics,
            processed: HashMap::new(),
            lifecycle,
            subscription,
            topology,
            flow_context,
            current_correlation_id: None,  // Will be set for source stages
            processing_mode,
            is_source,
            event_store: store,
            natural_completion: false,
            adapter_drained_flag: self.adapter_drained_flag,
        })
    }
}
