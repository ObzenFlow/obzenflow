//! Layered pipeline lifecycle coordination
//! 
//! Solves the "Russian Doll" initialization problem by recognizing that
//! different components have different initialization dependencies and
//! should not share a single synchronization barrier.

use crate::topology::{PipelineTopology, StageId, ShutdownSignal};
use crate::step::Result;
use crate::event_store::EventStore;
use std::sync::Arc;
use tokio::sync::{Barrier, RwLock, broadcast, Notify};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use async_trait::async_trait;

/// Hierarchical initialization model with distinct layers
pub struct LayeredPipelineLifecycle {
    topology: Arc<PipelineTopology>,
    layers: Vec<InitializationLayer>,
    shutdown_tx: broadcast::Sender<ShutdownSignal>,
    /// Run signal for source stages (materialize/run pattern)
    run_signal: Arc<Notify>,
}

/// A single initialization layer with its own barrier and components
pub struct InitializationLayer {
    pub name: String,
    pub layer_type: LayerType,
    pub barrier: Arc<RwLock<Option<Arc<Barrier>>>>,
    pub components: Arc<RwLock<Vec<Box<dyn LayerComponent>>>>,
    pub state: Arc<RwLock<LayerState>>,
}

/// State of an initialization layer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerState {
    Uninitialized,
    Initializing,
    Ready,
    Running,
    Draining,
    Drained,
    Failed(LayerError),
}

/// Layer initialization errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerError {
    InitializationFailed,
    DependencyFailed,
    Timeout,
}

/// Component that belongs to a specific initialization layer
#[async_trait]
pub trait LayerComponent: Send + Sync {
    /// Which layer this component belongs to
    fn layer_type(&self) -> LayerType;
    
    /// Component identifier
    fn id(&self) -> &str;
    
    /// Initialize the component (create subscriptions, etc.)
    async fn initialize(&self) -> Result<()>;
    
    /// Start the component (begin processing)
    async fn start(&self) -> Result<()>;
    
    /// Begin draining
    async fn begin_drain(&mut self) -> Result<()>;
    
    /// Check if drained
    fn is_drained(&self) -> bool;
    
    /// Force shutdown
    async fn force_shutdown(&mut self) -> Result<()>;
    
    /// Allow downcasting for specific operations
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Types of initialization layers
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LayerType {
    /// Layer A: Event processing stages (must be ready before events flow)
    Stage = 0,
    
    /// Layer B: Flow-level observers (start after stages are ready)
    Observer = 1,
    
    /// Layer C: Control/coordination components
    Control = 2,
}

impl LayeredPipelineLifecycle {
    /// Create a new layered lifecycle coordinator
    pub fn new(topology: Arc<PipelineTopology>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);
        
        tracing::info!("Creating LayeredPipelineLifecycle with {} stages", topology.num_stages());
        
        // Create layers in order
        // FLOWIP-075: Stages handle their own coordination - no layer barriers
        let layers = vec![
            InitializationLayer::new("Stage Layer", LayerType::Stage, 0), // No barrier - stages self-coordinate
            InitializationLayer::new("Observer Layer", LayerType::Observer, 0), // Dynamic size
            InitializationLayer::new("Control Layer", LayerType::Control, 0),   // Dynamic size
        ];
        
        Self {
            topology,
            layers,
            shutdown_tx,
            run_signal: Arc::new(Notify::new()),
        }
    }
    
    /// Register a component in its appropriate layer
    pub async fn register_component<C: LayerComponent + 'static>(&mut self, component: C) -> Result<()> {
        let layer_type = component.layer_type();
        let layer_index = layer_type as usize;
        
        if layer_index >= self.layers.len() {
            return Err(format!("Invalid layer type: {:?}", layer_type).into());
        }
        
        let component_id = component.id().to_string();
        let layer = &mut self.layers[layer_index];
        
        // Add to components
        let mut components = layer.components.write().await;
        components.push(Box::new(component));
        let component_count = components.len();
        drop(components);
        
        // FLOWIP-075: No barrier coordination - components handle their own lifecycle
        
        tracing::info!(
            "Registered component '{}' in {} (now {} components)",
            component_id,
            layer.name,
            component_count
        );
        
        Ok(())
    }
    
    /// Initialize all layers using materialize/run pattern
    pub async fn initialize(&self) -> Result<()> {
        tracing::info!("Starting initialization with materialize/run pattern");
        
        // Phase 1: Materialize everything
        self.materialize_all().await?;
        
        // Phase 2: Start the layers (stages will wait for run signal)
        for (index, layer) in self.layers.iter().enumerate() {
            // Check if previous layer is running (dependency check)
            if index > 0 {
                let prev_layer = &self.layers[index - 1];
                let prev_state = prev_layer.state.read().await;
                
                match *prev_state {
                    LayerState::Running => {
                        tracing::info!("{} dependency satisfied (previous layer running)", layer.name);
                    }
                    LayerState::Failed(err) => {
                        tracing::error!("{} cannot start - dependency failed: {:?}", layer.name, err);
                        let mut state = layer.state.write().await;
                        *state = LayerState::Failed(LayerError::DependencyFailed);
                        continue;
                    }
                    state => {
                        tracing::error!("{} cannot start - dependency in wrong state: {:?}", layer.name, state);
                        let mut state = layer.state.write().await;
                        *state = LayerState::Failed(LayerError::DependencyFailed);
                        continue;
                    }
                }
            }
            
            tracing::info!("Starting layer {}: {}", index, layer.name);
            
            // Update state
            {
                let mut state = layer.state.write().await;
                *state = LayerState::Initializing;
            }
            
            // Get components and barrier
            let components = layer.components.read().await;
            let barrier_opt = layer.barrier.read().await;
            
            if components.is_empty() {
                tracing::info!("{} has no components, marking as ready", layer.name);
                let mut state = layer.state.write().await;
                *state = LayerState::Ready;
                drop(state);
                
                let mut state = layer.state.write().await;
                *state = LayerState::Running;
                continue;
            }
            
            // Initialize all components in this layer
            let mut init_failed = false;
            for component in components.iter() {
                tracing::debug!("Initializing component '{}' in {}", component.id(), layer.name);
                if let Err(e) = component.initialize().await {
                    tracing::error!("Component '{}' failed to initialize: {}", component.id(), e);
                    init_failed = true;
                    break;
                }
            }
            
            if init_failed {
                let mut state = layer.state.write().await;
                *state = LayerState::Failed(LayerError::InitializationFailed);
                continue;
            }
            
            // FLOWIP-075: No barrier coordination
            // Stages handle their own coordination internally
            // Observers start when ready without coordination
            
            // Mark layer as ready
            {
                let mut state = layer.state.write().await;
                *state = LayerState::Ready;
            }
            
            tracing::info!("{} initialization complete", layer.name);
            
            // Start all components in this layer before moving to next layer
            let mut start_failed = false;
            for component in components.iter() {
                tracing::debug!("Starting component '{}' in {}", component.id(), layer.name);
                if let Err(e) = component.start().await {
                    tracing::error!("Component '{}' failed to start: {}", component.id(), e);
                    start_failed = true;
                    break;
                }
            }
            
            if start_failed {
                let mut state = layer.state.write().await;
                *state = LayerState::Failed(LayerError::InitializationFailed);
                continue;
            }
            
            // Mark layer as running
            {
                let mut state = layer.state.write().await;
                *state = LayerState::Running;
            }
            
            tracing::info!("{} is now running", layer.name);
        }
        
        tracing::info!("All layers initialized and running");
        
        // Phase 3: Fire the starting gun for sources
        self.run_sources().await?;
        
        Ok(())
    }
    
    /// Get a shutdown receiver
    pub fn shutdown_receiver(&self) -> broadcast::Receiver<ShutdownSignal> {
        self.shutdown_tx.subscribe()
    }
    
    /// Get the shutdown sender (for compatibility)
    pub fn shutdown_sender(&self) -> broadcast::Sender<ShutdownSignal> {
        self.shutdown_tx.clone()
    }
    
    /// Begin graceful shutdown (drain layers from outside-in)
    pub async fn begin_shutdown(&self) -> Result<()> {
        tracing::info!("Beginning layered shutdown");
        
        // Broadcast shutdown signal
        let _ = self.shutdown_tx.send(ShutdownSignal::BeginDrain);
        
        // Drain layers in reverse order (outside-in)
        for (index, layer) in self.layers.iter().rev().enumerate() {
            let layer_index = self.layers.len() - index - 1;
            tracing::info!("Draining layer {}: {}", layer_index, layer.name);
            
            // Check current state
            let current_state = *layer.state.read().await;
            match current_state {
                LayerState::Running => {
                    // Proceed with drain
                }
                LayerState::Failed(_) | LayerState::Uninitialized => {
                    tracing::info!("{} not running, skipping drain", layer.name);
                    continue;
                }
                LayerState::Draining | LayerState::Drained => {
                    tracing::info!("{} already draining/drained", layer.name);
                    continue;
                }
                _ => {
                    tracing::warn!("{} in unexpected state {:?}, attempting drain", layer.name, current_state);
                }
            }
            
            // Update state
            {
                let mut state = layer.state.write().await;
                *state = LayerState::Draining;
            }
            
            // Drain all components in this layer
            let mut components = layer.components.write().await;
            for component in components.iter_mut() {
                tracing::debug!("Draining component '{}' in {}", component.id(), layer.name);
                if let Err(e) = component.begin_drain().await {
                    tracing::error!("Component '{}' drain failed: {}", component.id(), e);
                }
            }
            
            // Wait for all components in this layer to drain
            let mut drain_attempts = 0;
            const MAX_DRAIN_ATTEMPTS: u32 = 100; // 5 seconds with 50ms sleep
            
            loop {
                let all_drained = components.iter().all(|c| c.is_drained());
                if all_drained {
                    break;
                }
                
                drain_attempts += 1;
                if drain_attempts >= MAX_DRAIN_ATTEMPTS {
                    tracing::warn!("{} drain timeout, forcing shutdown", layer.name);
                    for component in components.iter_mut() {
                        if !component.is_drained() {
                            let _ = component.force_shutdown().await;
                        }
                    }
                    break;
                }
                
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            
            // Mark layer as drained
            {
                let mut state = layer.state.write().await;
                *state = LayerState::Drained;
            }
            
            tracing::info!("{} fully drained", layer.name);
        }
        
        tracing::info!("All layers drained");
        Ok(())
    }
    
    /// Force shutdown all layers
    pub async fn force_shutdown(&self, reason: &str) -> Result<()> {
        tracing::warn!("Forcing layered shutdown: {}", reason);
        
        // Broadcast force shutdown signal
        let _ = self.shutdown_tx.send(ShutdownSignal::ForceShutdown(reason.to_string()));
        
        // Force shutdown all layers immediately
        for layer in self.layers.iter() {
            let mut components = layer.components.write().await;
            for component in components.iter_mut() {
                let _ = component.force_shutdown().await;
            }
            
            let mut state = layer.state.write().await;
            *state = LayerState::Drained;
        }
        
        Ok(())
    }
    
    /// Wait for all stages to drain (FLOWIP-074)
    async fn wait_for_all_stages_drained(&self) -> Result<()> {
        let stage_layer = &self.layers[LayerType::Stage as usize];
        
        loop {
            // Check if all stage components are drained
            let components = stage_layer.components.read().await;
            let all_drained = components.iter().all(|c| c.is_drained());
            
            if all_drained && !components.is_empty() {
                tracing::info!("All {} stages have drained", components.len());
                return Ok(());
            }
            
            // Brief sleep before checking again
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
    
    /// Notify observers to begin draining but don't wait (FLOWIP-074)
    async fn begin_observer_drain(&self) -> Result<()> {
        let observer_layer = &self.layers[LayerType::Observer as usize];
        let mut components = observer_layer.components.write().await;
        
        if components.is_empty() {
            tracing::debug!("No observers to drain");
            return Ok(());
        }
        
        tracing::info!("Notifying {} observers to begin draining", components.len());
        
        // Tell everyone to start draining
        for component in components.iter_mut() {
            tracing::debug!("Notifying observer '{}' to drain", component.id());
            if let Err(e) = component.begin_drain().await {
                tracing::warn!("Observer '{}' drain notification failed: {}", component.id(), e);
                // Continue anyway - fire and forget
            }
        }
        
        // But DON'T WAIT - let them drain on their own time
        tracing::info!("Observer drain notifications sent (fire-and-forget)");
        Ok(())
    }
    
    /// Monitor for natural completion (FLOWIP-074)
    pub async fn monitor_completion(&self) -> Result<()> {
        tracing::info!("Monitoring for natural flow completion");
        
        // Wait for all stages to complete
        self.wait_for_all_stages_drained().await?;
        
        // Notify observers to start draining (fire and forget)
        self.begin_observer_drain().await?;
        
        tracing::info!("Natural flow completion detected and observers notified");
        Ok(())
    }
}

impl InitializationLayer {
    fn new(name: &str, layer_type: LayerType, initial_size: usize) -> Self {
        let barrier = if initial_size > 0 {
            Some(Arc::new(Barrier::new(initial_size)))
        } else {
            None
        };
        
        Self {
            name: name.to_string(),
            layer_type,
            barrier: Arc::new(RwLock::new(barrier)),
            components: Arc::new(RwLock::new(Vec::new())),
            state: Arc::new(RwLock::new(LayerState::Uninitialized)),
        }
    }
}

/// Compatibility layer for stages to work with LayerComponent
pub struct StageLifecycleHandle {
    stage_id: StageId,
    init_barrier: Arc<Barrier>,
    shutdown_rx: Arc<RwLock<Option<broadcast::Receiver<ShutdownSignal>>>>,
    /// Track completed upstream stages (FLOWIP-058)
    completed_upstreams: Arc<RwLock<HashSet<StageId>>>,
    /// Count of in-flight events being processed
    in_flight_count: Arc<AtomicUsize>,
    /// Notify when drain conditions are met
    drain_ready_notify: Arc<Notify>,
    /// Reference to topology for upstream lookups
    topology: Arc<PipelineTopology>,
    /// Reference to event store for checking unprocessed events
    event_store: Option<Arc<EventStore>>,
}

impl StageLifecycleHandle {
    /// Create a new stage lifecycle handle
    pub fn new(
        stage_id: StageId,
        init_barrier: Arc<Barrier>,
        shutdown_tx: broadcast::Sender<ShutdownSignal>,
        topology: Arc<PipelineTopology>,
        event_store: Option<Arc<EventStore>>,
    ) -> Self {
        Self {
            stage_id,
            init_barrier,
            shutdown_rx: Arc::new(RwLock::new(Some(shutdown_tx.subscribe()))),
            completed_upstreams: Arc::new(RwLock::new(HashSet::new())),
            in_flight_count: Arc::new(AtomicUsize::new(0)),
            drain_ready_notify: Arc::new(Notify::new()),
            topology,
            event_store,
        }
    }
    
    /// Signal that this stage is ready
    pub async fn signal_ready(&self) {
        tracing::info!("Stage {:?} signaling ready", self.stage_id);
        self.init_barrier.wait().await;
        tracing::info!("Stage {:?} passed barrier", self.stage_id);
    }
    
    /// Get shutdown receiver
    pub async fn shutdown_receiver(&self) -> Option<broadcast::Receiver<ShutdownSignal>> {
        let mut rx = self.shutdown_rx.write().await;
        rx.take()
    }
    
    /// Check if this stage has any pending work (FLOWIP-058)
    pub async fn has_pending_work(&self) -> bool {
        // In-flight events currently being processed
        let in_flight = self.in_flight_count.load(Ordering::SeqCst) > 0;
        
        // Unprocessed events in EventStore for this stage
        let has_unprocessed = if let Some(ref store) = self.event_store {
            // For now, we'll consider there are no unprocessed events
            // In a real implementation, EventStore would track this
            false
        } else {
            false
        };
        
        // Any upstream stages still active (not drained)
        let upstreams_active = self.any_upstream_active().await;
        
        in_flight || has_unprocessed || upstreams_active
    }
    
    /// Wait for drain conditions to be met (FLOWIP-058)
    pub async fn wait_for_drain_ready(&self) {
        // Notified when:
        // 1. All in-flight events complete
        // 2. All upstream stages signal EOF
        // 3. No unprocessed events remain
        self.drain_ready_notify.notified().await
    }
    
    /// Check if any upstream is still active (FLOWIP-058)
    pub async fn any_upstream_active(&self) -> bool {
        let completed = self.completed_upstreams.read().await;
        let upstream_stages = self.topology.upstream_stages(self.stage_id);
        
        // If we have upstreams and not all are completed, some are active
        !upstream_stages.is_empty() && completed.len() < upstream_stages.len()
    }
    
    /// Mark an upstream stage as completed (FLOWIP-058)
    pub async fn mark_upstream_complete(&self, stage_id: StageId) {
        let mut completed = self.completed_upstreams.write().await;
        completed.insert(stage_id);
        
        // Check if all upstreams are now complete
        let upstream_stages = self.topology.upstream_stages(self.stage_id);
        if completed.len() == upstream_stages.len() && self.in_flight_count.load(Ordering::SeqCst) == 0 {
            // All conditions met, notify drain ready
            self.drain_ready_notify.notify_waiters();
        }
    }
    
    /// Get count of active upstream stages (FLOWIP-058)
    pub async fn active_upstream_count(&self) -> usize {
        let completed = self.completed_upstreams.read().await;
        let upstream_stages = self.topology.upstream_stages(self.stage_id);
        upstream_stages.len() - completed.len()
    }
    
    /// Increment in-flight count when processing starts
    pub fn increment_in_flight(&self) {
        self.in_flight_count.fetch_add(1, Ordering::SeqCst);
    }
    
    /// Decrement in-flight count when processing completes
    pub fn decrement_in_flight(&self) {
        let previous = self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
        
        // If this was the last in-flight event and all upstreams are done, notify
        if previous == 1 {
            // Check drain conditions asynchronously
            let handle = self.clone();
            tokio::spawn(async move {
                if !handle.any_upstream_active().await {
                    handle.drain_ready_notify.notify_waiters();
                }
            });
        }
    }
    
    /// Signal that this stage has drained
    pub async fn signal_drained(&self) {
        tracing::info!("Stage {:?} signaling drained", self.stage_id);
        // In the future, this could update layer state
    }
}

impl Clone for StageLifecycleHandle {
    fn clone(&self) -> Self {
        Self {
            stage_id: self.stage_id,
            init_barrier: self.init_barrier.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            completed_upstreams: self.completed_upstreams.clone(),
            in_flight_count: self.in_flight_count.clone(),
            drain_ready_notify: self.drain_ready_notify.clone(),
            topology: self.topology.clone(),
            event_store: self.event_store.clone(),
        }
    }
}

/// Get stage lifecycle handle from layered lifecycle
impl LayeredPipelineLifecycle {
    /// Create a stage lifecycle handle for backward compatibility
    pub async fn stage_lifecycle(&self, stage_id: StageId) -> Result<StageLifecycleHandle> {
        // Get the stage layer
        let stage_layer = &self.layers[LayerType::Stage as usize];
        
        // Get the barrier if one exists (stages don't use layer barriers)
        let barrier_opt = stage_layer.barrier.read().await;
        let barrier = if let Some(ref b) = *barrier_opt {
            b.clone()
        } else {
            // Stages coordinate themselves, create a dummy barrier that's already satisfied
            Arc::new(Barrier::new(0))
        };
        
        Ok(StageLifecycleHandle::new(
            stage_id,
            barrier,
            self.shutdown_tx.clone(),
            self.topology.clone(),
            None, // EventStore will be set later if available
        ))
    }
    
    /// Create a stage lifecycle handle with event store
    pub async fn stage_lifecycle_with_store(
        &self, 
        stage_id: StageId, 
        event_store: Arc<EventStore>
    ) -> Result<StageLifecycleHandle> {
        // Get the stage layer
        let stage_layer = &self.layers[LayerType::Stage as usize];
        
        // Get the barrier if one exists (stages don't use layer barriers)
        let barrier_opt = stage_layer.barrier.read().await;
        let barrier = if let Some(ref b) = *barrier_opt {
            b.clone()
        } else {
            // Stages coordinate themselves, create a dummy barrier that's already satisfied
            Arc::new(Barrier::new(0))
        };
        
        Ok(StageLifecycleHandle::new(
            stage_id,
            barrier,
            self.shutdown_tx.clone(),
            self.topology.clone(),
            Some(event_store),
        ))
    }
    
    /// Get the run signal for source stages
    pub fn run_signal(&self) -> Arc<Notify> {
        self.run_signal.clone()
    }
    
    /// Phase 1 of materialize/run pattern: Set up all components
    pub async fn materialize_all(&self) -> Result<()> {
        tracing::info!("Starting materialize phase");
        
        // Materialize stages first (they're the foundation)
        self.materialize_stages().await?;
        
        // Then materialize auxiliary layers (observers, middleware, control)
        self.materialize_auxiliary_layers().await?;
        
        tracing::info!("Materialize phase complete");
        Ok(())
    }
    
    /// Materialize stage layer
    async fn materialize_stages(&self) -> Result<()> {
        tracing::info!("Materializing stage layer");
        
        let stage_layer = &self.layers[LayerType::Stage as usize];
        let components = stage_layer.components.read().await;
        
        // Initialize all stage components
        for component in components.iter() {
            tracing::debug!("Materializing stage component '{}'", component.id());
            component.initialize().await?;
        }
        
        tracing::info!("Stage layer materialized with {} components", components.len());
        Ok(())
    }
    
    /// Materialize auxiliary layers (observers, control)
    async fn materialize_auxiliary_layers(&self) -> Result<()> {
        tracing::info!("Materializing auxiliary layers");
        
        // Skip stage layer (index 0) and materialize the rest
        for (index, layer) in self.layers.iter().enumerate().skip(1) {
            let components = layer.components.read().await;
            
            if components.is_empty() {
                tracing::debug!("Skipping empty layer: {}", layer.name);
                continue;
            }
            
            tracing::info!("Materializing {} with {} components", layer.name, components.len());
            
            for component in components.iter() {
                tracing::debug!("Materializing component '{}' in {}", component.id(), layer.name);
                component.initialize().await?;
            }
        }
        
        tracing::info!("Auxiliary layers materialized");
        Ok(())
    }
    
    /// Phase 2 of materialize/run pattern: Start event generation
    pub async fn run_sources(&self) -> Result<()> {
        tracing::info!("Firing the starting gun for sources! 🔫");
        
        // Notify all waiting sources to start generating events
        self.run_signal.notify_waiters();
        
        tracing::info!("Sources notified to start");
        Ok(())
    }
}