//! Generic event processor that handles subscription loops and draining
//! 
//! This eliminates duplicate code across components by providing a standard
//! implementation for event subscription, processing, and graceful shutdown.

use crate::event_store::{EventStore, EventSubscription, SubscriptionFilter, SubscriptionEvent};
use crate::step::Result;
use crate::topology::{ShutdownSignal, ComponentType, Drainable};
use crate::lifecycle::{EventHandler, ProcessingMode};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use tokio::task::JoinHandle;
use tokio::sync::{broadcast, RwLock};

/// State machine for processor lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorState {
    /// Initial state, not yet started
    Uninitialized,
    /// Running and processing events
    Running,
    /// Received drain signal, processing remaining events
    Draining,
    /// All events processed, ready to exit
    Drained,
}

/// Generic processor that handles subscription and draining for any EventHandler
pub struct GenericEventProcessor<H: EventHandler> {
    name: String,
    handler: Arc<H>,
    event_store: Arc<EventStore>,
    filter: SubscriptionFilter,
    state: Arc<RwLock<ProcessorState>>,
    pending_events: Arc<AtomicUsize>,
    shutdown_tx: Option<broadcast::Sender<ShutdownSignal>>,
    component_type: ComponentType,
    is_running: Arc<AtomicBool>,
}

impl<H: EventHandler> Clone for GenericEventProcessor<H> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            handler: self.handler.clone(),
            event_store: self.event_store.clone(),
            filter: self.filter.clone(),
            state: self.state.clone(),
            pending_events: self.pending_events.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            component_type: self.component_type,
            is_running: self.is_running.clone(),
        }
    }
}

impl<H: EventHandler> GenericEventProcessor<H> {
    /// Create a new generic event processor
    pub fn new(
        name: String,
        handler: H,
        event_store: Arc<EventStore>,
        filter: SubscriptionFilter,
        component_type: ComponentType,
    ) -> Self {
        Self {
            name,
            handler: Arc::new(handler),
            event_store,
            filter,
            state: Arc::new(RwLock::new(ProcessorState::Uninitialized)),
            pending_events: Arc::new(AtomicUsize::new(0)),
            shutdown_tx: None,
            component_type,
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }
    
    /// Set shutdown sender for coordination
    pub fn with_shutdown_tx(mut self, shutdown_tx: broadcast::Sender<ShutdownSignal>) -> Self {
        self.shutdown_tx = Some(shutdown_tx);
        self
    }
    
    /// Start the processor task
    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> 
    where 
        H: 'static 
    {
        // Check if already started
        if self.is_running.swap(true, Ordering::SeqCst) {
            return Err("Processor already started".into());
        }
        
        // Clone self for the spawned task
        let processor = self.clone();
        
        // Set state to Running
        {
            let mut state = self.state.write().await;
            *state = ProcessorState::Running;
        }
        
        // Spawn task to process events
        let handle = tokio::spawn(async move {
            processor.run().await
        });
        
        Ok(handle)
    }
    
    /// Internal run method that does the actual processing
    async fn run(&self) -> Result<()> {
        tracing::info!("Generic processor '{}' starting, subscribing to events...", self.name);
        
        // Subscribe to events
        let mut subscription = self.event_store
            .subscribe(self.filter.clone())
            .await?;
            
        let name = self.name.clone();
        let handler = self.handler.clone();
        let state_ref = self.state.clone();
        let pending_ref = self.pending_events.clone();
        let mut shutdown_rx = self.shutdown_tx.as_ref().map(|tx| tx.subscribe());
        let processing_mode = handler.processing_mode();
        
        tracing::info!("Generic processor '{}' started in {:?} mode", name, processing_mode);
        
        loop {
                // Get current state
                let current_state = {
                    let state = state_ref.read().await;
                    *state
                };
                
                match current_state {
                    ProcessorState::Running | ProcessorState::Draining => {
                        // Check for shutdown signal if we have a receiver
                        if let Some(ref mut rx) = shutdown_rx {
                            match rx.try_recv() {
                                Ok(ShutdownSignal::BeginDrain) => {
                                    let mut state = state_ref.write().await;
                                    *state = ProcessorState::Draining;
                                    tracing::info!("Processor '{}' beginning drain", name);
                                }
                                Ok(ShutdownSignal::ForceShutdown(reason)) => {
                                    let mut state = state_ref.write().await;
                                    *state = ProcessorState::Drained;
                                    tracing::warn!("Processor '{}' force shutdown: {}", name, reason);
                                    return Ok(());
                                }
                                Err(_) => {} // No signal or channel closed
                            }
                        }
                        
                        // Process events with shutdown and EOF handling
                        let subscription_event = if let Some(ref mut rx) = shutdown_rx {
                            tokio::select! {
                                event = subscription.recv_with_eof() => event?,
                                signal = rx.recv() => {
                                    match signal {
                                        Ok(ShutdownSignal::BeginDrain) => {
                                            let mut state = state_ref.write().await;
                                            *state = ProcessorState::Draining;
                                            tracing::info!("Processor '{}' beginning drain", name);
                                            // Still need to receive events or EOF
                                            subscription.recv_with_eof().await?
                                        }
                                        Ok(ShutdownSignal::ForceShutdown(reason)) => {
                                            let mut state = state_ref.write().await;
                                            *state = ProcessorState::Drained;
                                            tracing::warn!("Processor '{}' force shutdown: {}", name, reason);
                                            return Ok(());
                                        }
                                        Err(_) => {
                                            // Channel closed, treat as drain
                                            let mut state = state_ref.write().await;
                                            *state = ProcessorState::Draining;
                                            subscription.recv_with_eof().await?
                                        }
                                    }
                                }
                            }
                        } else {
                            subscription.recv_with_eof().await?
                        };
                        
                        // Handle the subscription event
                        let events = match subscription_event {
                            SubscriptionEvent::Events(events) => events,
                            SubscriptionEvent::EndOfStream { stage_id, natural_completion, .. } => {
                                tracing::info!(
                                    "Processor '{}' received EOF from stage {:?} (natural: {})", 
                                    name, stage_id, natural_completion
                                );
                                vec![] // Continue to process other upstreams
                            }
                            SubscriptionEvent::AllUpstreamsComplete => {
                                tracing::info!("Processor '{}' all upstreams complete", name);
                                // Transition to drained if we're draining
                                if current_state == ProcessorState::Draining {
                                    let mut state = state_ref.write().await;
                                    *state = ProcessorState::Drained;
                                }
                                break;
                            }
                        };
                        
                        if events.is_empty() {
                            if current_state == ProcessorState::Draining {
                                // No more events and we're draining - transition to drained
                                let mut state = state_ref.write().await;
                                *state = ProcessorState::Drained;
                                break;
                            }
                            // Otherwise continue waiting for events
                            continue;
                        }
                        
                        // Track pending events
                        pending_ref.fetch_add(events.len(), Ordering::SeqCst);
                        
                        // Process based on handler mode
                        match processing_mode {
                            ProcessingMode::Transform => {
                                for envelope in events {
                                    let _outputs = handler.transform(envelope.event);
                                    // TODO: Write outputs when we have a writer
                                    pending_ref.fetch_sub(1, Ordering::SeqCst);
                                }
                            }
                            ProcessingMode::Observe => {
                                for envelope in events {
                                    if let Err(e) = handler.observe(&envelope.event) {
                                        tracing::error!("Observer '{}' error: {}", name, e);
                                    }
                                    pending_ref.fetch_sub(1, Ordering::SeqCst);
                                }
                            }
                            ProcessingMode::Aggregate => {
                                // TODO: Aggregate mode requires mutable access
                                // For now, we don't support aggregate mode with Arc<H>
                                // This would need Arc<Mutex<H>> or interior mutability
                                tracing::warn!("Aggregate mode not yet supported in GenericEventProcessor");
                                for envelope in events {
                                    // Just consume the events for now
                                    let _ = envelope.event;
                                    pending_ref.fetch_sub(1, Ordering::SeqCst);
                                }
                            }
                        }
                    }
                    ProcessorState::Drained => {
                        break;
                    }
                    _ => unreachable!("Invalid state transition")
                }
            }
            
            tracing::info!("Generic processor '{}' stopped", name);
            Ok(())
    }
}

#[async_trait]
impl<H: EventHandler + 'static> Drainable for GenericEventProcessor<H> {
    fn id(&self) -> &str {
        &self.name
    }
    
    fn component_type(&self) -> ComponentType {
        self.component_type
    }
    
    async fn signal_ready(&self) -> Result<()> {
        // GenericEventProcessor is ready when started
        Ok(())
    }
    
    async fn begin_drain(&mut self) -> Result<()> {
        let mut state = self.state.write().await;
        match *state {
            ProcessorState::Running => {
                *state = ProcessorState::Draining;
                tracing::info!("Processor '{}' beginning drain", self.name);
                Ok(())
            }
            ProcessorState::Draining | ProcessorState::Drained => {
                // Already draining or drained
                Ok(())
            }
            ProcessorState::Uninitialized => {
                Err("Cannot drain uninitialized processor".into())
            }
        }
    }
    
    fn is_drained(&self) -> bool {
        // Note: This is called from async context, so we can't use blocking_read
        // We'll try_read and default to false if we can't get the lock
        let pending = self.pending_events.load(Ordering::SeqCst);
        
        if let Ok(state) = self.state.try_read() {
            matches!(*state, ProcessorState::Drained) && pending == 0
        } else {
            false // If we can't read the state, assume not drained
        }
    }
    
    fn pending_count(&self) -> usize {
        self.pending_events.load(Ordering::SeqCst)
    }
    
    async fn force_shutdown(&mut self) -> Result<()> {
        let mut state = self.state.write().await;
        *state = ProcessorState::Drained;
        
        tracing::warn!("Processor '{}' force shutdown", self.name);
        Ok(())
    }
}