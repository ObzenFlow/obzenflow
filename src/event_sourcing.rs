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

use crate::step::{Step, StepType, Result, ChainEvent};
use crate::event_store::{EventStore, EventWriter, EventEnvelope, StageSemantics, EventSubscription, SubscriptionFilter};
use crate::event_store::constants::SOURCE_IDLE_SLEEP;
use crate::topology::{StageId, PipelineTopology, StageLifecycle, ShutdownSignal, PipelineLifecycle};
// Removed broadcast - using StageShutdownHandle instead
use std::sync::Arc;
use std::collections::HashMap;
use ulid::Ulid;

// ShutdownSignal moved to topology::shutdown module

/// State machine for stage execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StageState {
    Running,   // Normal operation
    Draining,  // Received shutdown, processing remaining events
    Drained,   // No more events, ready to exit
}

/// Event sourced stage that reads from EventStore with causal consistency
pub struct EventSourcedStage<S: Step> {
    inner: S,
    stage_id: StageId,
    stage_name: String,
    writer: Option<EventWriter>,
    semantics: StageSemantics,
    /// Track processed events to avoid reprocessing
    processed: HashMap<Ulid, EventEnvelope>,
    /// Unified lifecycle handle for init and shutdown
    lifecycle: StageLifecycle,
    /// Push-based subscription for receiving events
    subscription: Option<EventSubscription>,
    /// Pipeline topology for lookups
    topology: Arc<PipelineTopology>,
}

impl<S: Step> EventSourcedStage<S> {
    /// Create a new builder for constructing an EventSourcedStage
    pub fn builder() -> EventSourcedStageBuilder<S> {
        EventSourcedStageBuilder::new()
    }

    /// Process any new events from the EventStore
    async fn process_new_events(&mut self, state: &mut StageState) -> Result<usize> {
        let mut processed_count = 0;
        
        // Process subscribed events if we have a subscription
        if let Some(ref mut subscription) = self.subscription {
            // Use select! to handle both events and shutdown
            let events = tokio::select! {
                // Normal event processing
                events = subscription.recv_causal_batch() => events?,
                
                // Shutdown signal received
                signal = self.lifecycle.wait_for_shutdown() => {
                    match signal {
                        ShutdownSignal::BeginDrain => {
                            *state = StageState::Draining;
                            // Return empty to check if we can drain immediately
                            vec![]
                        }
                        ShutdownSignal::ForceShutdown(reason) => {
                            return Err(format!("Force shutdown: {}", reason).into());
                        }
                    }
                }
            };
            
            
            for envelope in events {
                // Skip already processed
                if self.processed.contains_key(&envelope.event.ulid) {
                    continue;
                }
                
                // Track event entering this stage
                self.lifecycle.event_entered().await;
                
                // Check for shutdown event
                if envelope.event.event_type == "_shutdown" {
                    // Propagate shutdown to downstream stages (except sinks)
                    if self.inner.step_type() != StepType::Sink {
                        if let Some(ref mut writer) = self.writer {
                            let _ = writer.append(envelope.event.clone(), Some(&envelope)).await;
                        }
                    }
                    // Mark as processed but don't increment count
                    self.processed.insert(envelope.event.ulid.clone(), envelope);
                    // Event completed
                    self.lifecycle.event_completed().await;
                    // Move to draining state
                    *state = StageState::Draining;
                    continue;
                }
                
                // Process event
                let results = self.inner.handle(envelope.event.clone());
                
                // Write results
                let writer = self.writer.as_mut().ok_or("Writer not initialized")?;
                for result in results {
                    tracing::debug!("Stage '{}' writing event type '{}'", self.stage_name, result.event_type);
                    writer.append(result, Some(&envelope)).await?;
                }
                
                self.processed.insert(envelope.event.ulid.clone(), envelope);
                processed_count += 1;
                
                // Event completed
                self.lifecycle.event_completed().await;
            }
        }
        
        // Sources generate events (even if they also have subscriptions!)
        if self.inner.step_type() == StepType::Source {
            // Sources without subscriptions need to check for shutdown
            if self.subscription.is_none() {
                // Check for shutdown signal without blocking
                match self.lifecycle.is_shutdown_requested() {
                    Some(ShutdownSignal::BeginDrain) => {
                        *state = StageState::Draining;
                        return Ok(0);
                    }
                    Some(ShutdownSignal::ForceShutdown(reason)) => {
                        return Err(format!("Force shutdown: {}", reason).into());
                    }
                    None => {}
                }
            }
            
            let tick_event = ChainEvent::new("_tick", serde_json::json!({}));
            let results = self.inner.handle(tick_event);
            
            let writer = self.writer.as_mut().ok_or("Writer not initialized")?;
            for result in results {
                tracing::debug!("Source '{}' writing event type '{}'", self.stage_name, result.event_type);
                writer.append(result, None).await?;
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
        if self.lifecycle.should_wait_before_processing() {
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
                            
                            // For sources, prevent tight loop when no events generated
                            if self.inner.step_type() == StepType::Source {
                                tokio::time::sleep(SOURCE_IDLE_SLEEP).await;
                            }
                            // For other stages, recv_causal_batch() blocks naturally
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
                            self.inner.shutdown().await?;
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::error!("Stage '{}' error: {}", self.stage_name, e);
                            // Continue processing - don't fail on individual errors
                        }
                    }
                }
                
                StageState::Draining => {
                    // We're draining - check if we can transition to Drained
                    let in_flight = self.lifecycle.in_flight_count().await;
                    if in_flight == 0 {
                        state = StageState::Drained;
                    } else {
                        // Still have events in flight, try to process more
                        match self.process_new_events(&mut state).await {
                            Ok(count) => {
                                if count > 0 {
                                    tracing::debug!(
                                        "Stage '{}' drained {} events, {} remaining", 
                                        self.stage_name, count, in_flight
                                    );
                                }
                            }
                            Err(e) if e.to_string().contains("Force shutdown") => {
                                tracing::info!("Stage '{}' force shutting down during drain: {}", self.stage_name, e);
                                self.inner.shutdown().await?;
                                return Ok(());
                            }
                            Err(e) => {
                                tracing::error!("Stage '{}' error during drain: {}", self.stage_name, e);
                            }
                        }
                        
                        // Small delay to avoid tight loop during drain
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
                
                StageState::Drained => {
                    // Signal that we're drained and exit
                    self.lifecycle.signal_drained().await;
                    self.inner.shutdown().await?;
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
    lifecycle: Arc<PipelineLifecycle>,
    /// Track if shutdown has been called
    shutdown_called: Arc<std::sync::atomic::AtomicBool>,
}

impl FlowHandle {
    /// Create a new flow handle
    pub fn new(
        stage_handles: Vec<tokio::task::JoinHandle<Result<()>>>,
        lifecycle: Arc<PipelineLifecycle>,
    ) -> Self {
        Self {
            stage_handles,
            lifecycle,
            shutdown_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Gracefully shutdown the flow
    ///
    /// Allows stages to finish processing current events before shutting down.
    pub async fn shutdown(self) -> Result<()> {
        use std::sync::atomic::Ordering;

        // Ensure shutdown is only called once
        if self.shutdown_called.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Send graceful shutdown signal
        let _ = self.lifecycle.begin_shutdown().await;

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

        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!("Flow shutdown errors: {}", errors.join(", ")).into())
        }
    }

    /// Immediately abort the flow
    pub fn abort(self, reason: &str) {
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
    }
}
//
// Then in the EventSourcedStage, write with no parent:
// writer.append(event, None).await?;

// TODO: Add macro support for event-sourced flows integrated with flow! DSL

/// Builder for constructing EventSourcedStage with proper initialization
pub struct EventSourcedStageBuilder<S: Step> {
    inner: Option<S>,
    store: Option<Arc<EventStore>>,
    stage_id: Option<StageId>,
    stage_name: Option<String>,
    topology: Option<Arc<PipelineTopology>>,
    pipeline_lifecycle: Option<Arc<PipelineLifecycle>>,
}

impl<S: Step> EventSourcedStageBuilder<S> {
    pub fn new() -> Self {
        Self {
            inner: None,
            store: None,
            stage_id: None,
            stage_name: None,
            topology: None,
            pipeline_lifecycle: None,
        }
    }
    
    pub fn with_step(mut self, step: S) -> Self {
        self.inner = Some(step);
        self
    }
    
    pub fn with_topology(mut self, id: StageId, name: String, topology: Arc<PipelineTopology>) -> Self {
        self.stage_id = Some(id);
        self.stage_name = Some(name);
        self.topology = Some(topology);
        self
    }
    
    pub fn with_store(mut self, store: Arc<EventStore>) -> Self {
        self.store = Some(store);
        self
    }
    
    pub fn with_pipeline_lifecycle(mut self, lifecycle: Arc<PipelineLifecycle>) -> Self {
        self.pipeline_lifecycle = Some(lifecycle);
        self
    }
    
    pub async fn build(self) -> Result<EventSourcedStage<S>> {
        // Validate required fields
        let inner = self.inner.ok_or("Step is required")?;
        let store = self.store.ok_or("EventStore is required")?;
        let stage_id = self.stage_id.ok_or("Stage ID is required")?;
        let stage_name = self.stage_name.ok_or("Stage name is required")?;
        let topology = self.topology.ok_or("Pipeline topology is required")?;
        let pipeline_lifecycle = self.pipeline_lifecycle.ok_or("Pipeline lifecycle is required")?;
        
        // Create stage lifecycle from pipeline lifecycle
        let lifecycle = pipeline_lifecycle.stage_lifecycle(stage_id);
        
        // Determine semantics based on step type
        let semantics = match inner.step_type() {
            StepType::Source => StageSemantics::Stateless,
            StepType::Stage => StageSemantics::Stateless,
            StepType::Sink => StageSemantics::Stateless,
        };
        
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
        
        Ok(EventSourcedStage {
            inner,
            stage_id,
            stage_name,
            writer: Some(writer),
            semantics,
            processed: HashMap::new(),
            lifecycle,
            subscription,
            topology,
        })
    }
}
