//! Unified pipeline lifecycle coordination
//! 
//! Handles both initialization and shutdown in a symmetric way

use crate::topology::{PipelineTopology, StageId, Drainable};
use std::sync::Arc;
use tokio::sync::{Barrier, RwLock, broadcast, Semaphore};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Coordinates the entire pipeline lifecycle
pub struct PipelineLifecycle {
    topology: Arc<PipelineTopology>,
    /// Initialization barrier - all stages must be ready
    init_barrier: Arc<Barrier>,
    /// Tracks ready stages
    ready_stages: Arc<RwLock<HashSet<StageId>>>,
    /// Tracks drained stages
    drained_stages: Arc<RwLock<HashSet<StageId>>>,
    /// In-flight event tracking
    in_flight: Arc<RwLock<HashMap<StageId, usize>>>,
    /// Shutdown signal broadcaster
    shutdown_tx: broadcast::Sender<ShutdownSignal>,
    /// Drain completion semaphore
    drain_semaphore: Arc<Semaphore>,
    /// All drainable components (stages, observers, writers, etc.)
    drainables: Arc<RwLock<HashMap<String, Box<dyn Drainable>>>>,
}

#[derive(Debug, Clone)]
pub enum ShutdownSignal {
    /// Begin graceful shutdown - drain in-flight events
    BeginDrain,
    /// Force immediate shutdown
    ForceShutdown(String),
}

/// Per-stage lifecycle handle - symmetric for init and shutdown
pub struct StageLifecycle {
    stage_id: StageId,
    topology: Arc<PipelineTopology>,
    /// Initialization
    init_barrier: Arc<Barrier>,
    ready_stages: Arc<RwLock<HashSet<StageId>>>,
    /// Shutdown
    drained_stages: Arc<RwLock<HashSet<StageId>>>,
    in_flight: Arc<RwLock<HashMap<StageId, usize>>>,
    shutdown_rx: broadcast::Receiver<ShutdownSignal>,
    drain_semaphore: Arc<Semaphore>,
}

impl PipelineLifecycle {
    pub fn new(topology: Arc<PipelineTopology>) -> Self {
        let total_stages = topology.num_stages();
        let (shutdown_tx, _) = broadcast::channel(16);
        
        tracing::info!("PipelineLifecycle: Creating with {} stages", total_stages);
        
        // Initialize in-flight counters
        let mut in_flight_map = HashMap::new();
        for stage in topology.stages() {
            in_flight_map.insert(stage.id, 0);
        }
        
        Self {
            topology,
            init_barrier: Arc::new(Barrier::new(total_stages)),
            ready_stages: Arc::new(RwLock::new(HashSet::new())),
            drained_stages: Arc::new(RwLock::new(HashSet::new())),
            in_flight: Arc::new(RwLock::new(in_flight_map)),
            shutdown_tx,
            drain_semaphore: Arc::new(Semaphore::new(0)),
            drainables: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Get a lifecycle handle for a specific stage
    pub fn stage_lifecycle(&self, stage_id: StageId) -> StageLifecycle {
        StageLifecycle {
            stage_id,
            topology: self.topology.clone(),
            init_barrier: self.init_barrier.clone(),
            ready_stages: self.ready_stages.clone(),
            drained_stages: self.drained_stages.clone(),
            in_flight: self.in_flight.clone(),
            shutdown_rx: self.shutdown_tx.subscribe(),
            drain_semaphore: self.drain_semaphore.clone(),
        }
    }
    
    /// Get a shutdown receiver for non-stage components (observers, exporters, etc.)
    /// These components don't participate in the initialization barrier
    pub fn shutdown_receiver(&self) -> broadcast::Receiver<ShutdownSignal> {
        self.shutdown_tx.subscribe()
    }
    
    /// Register a drainable component
    pub async fn register_drainable<D: Drainable + 'static>(&self, component: D) {
        let id = component.id().to_string();
        let component_type = component.component_type();
        
        tracing::info!(
            "Registering drainable component '{}' of type {:?}", 
            id, component_type
        );
        
        let mut drainables = self.drainables.write().await;
        drainables.insert(id, Box::new(component));
    }
    
    /// Begin graceful shutdown
    pub async fn begin_shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Beginning graceful pipeline shutdown");
        
        // First broadcast the signal
        self.shutdown_tx.send(ShutdownSignal::BeginDrain)?;
        
        // Then tell all drainables to begin draining
        let drainables = self.drainables.read().await;
        for (id, drainable) in drainables.iter() {
            tracing::debug!("Beginning drain for component '{}'", id);
            // Note: We can't call begin_drain here because it requires &mut
            // This will be handled by each component's select! loop
        }
        
        Ok(())
    }
    
    /// Force immediate shutdown
    pub async fn force_shutdown(&self, reason: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::warn!("Forcing pipeline shutdown: {}", reason);
        self.shutdown_tx.send(ShutdownSignal::ForceShutdown(reason.to_string()))?;
        Ok(())
    }
    
    /// Wait for stages to drain (up to timeout)
    /// Returns current drain status regardless of whether all stages have drained
    pub async fn wait_for_drain(&self, timeout: Duration) -> Result<DrainReport, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let deadline = start + timeout;
        let total_stages = self.topology.num_stages();
        
        // Try to collect drain signals until timeout
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining == Duration::ZERO {
                break;
            }
            
            match tokio::time::timeout(remaining, self.drain_semaphore.acquire()).await {
                Ok(Ok(_permit)) => {
                    // One more stage drained, continue collecting
                    let drained_count = self.drained_stages.read().await.len();
                    if drained_count >= total_stages {
                        break; // All stages drained
                    }
                }
                Ok(Err(e)) => return Err(format!("Semaphore error: {}", e).into()),
                Err(_) => break, // Timeout - return current status
            }
        }
        
        // Return current drain status
        let drained = self.drained_stages.read().await;
        let in_flight = self.in_flight.read().await;
        
        let mut remaining_events = 0;
        let mut undrained_stages = Vec::new();
        
        for (stage_id, &count) in in_flight.iter() {
            if count > 0 || !drained.contains(stage_id) {
                if !drained.contains(stage_id) {
                    undrained_stages.push(*stage_id);
                }
                if count > 0 {
                    remaining_events += count;
                }
            }
        }
        
        Ok(DrainReport {
            total_stages,
            drained_stages: drained.len(),
            remaining_events,
            undrained_stages,
            elapsed: start.elapsed(),
        })
    }
    
    /// Wait for all drainable components to finish draining
    pub async fn wait_for_all_drained(&self, timeout: Duration) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let deadline = tokio::time::Instant::now() + timeout;
        
        loop {
            let mut all_drained = true;
            let mut pending_components = Vec::new();
            
            {
                let drainables = self.drainables.read().await;
                for (id, drainable) in drainables.iter() {
                    if !drainable.is_drained() {
                        all_drained = false;
                        pending_components.push(id.clone());
                    }
                }
            }
            
            if all_drained {
                tracing::info!("All drainable components have completed draining");
                return Ok(true);
            }
            
            if tokio::time::Instant::now() > deadline {
                tracing::warn!(
                    "Timeout waiting for components to drain. Still pending: {:?}", 
                    pending_components
                );
                return Ok(false);
            }
            
            // Small delay before checking again
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl StageLifecycle {
    // ===== INITIALIZATION PHASE =====
    
    /// Signal that this stage is ready (subscriptions created, etc.)
    pub async fn signal_ready(&self) {
        {
            let mut ready = self.ready_stages.write().await;
            ready.insert(self.stage_id);
            
            let stage_name = self.topology.stage_name(self.stage_id)
                .unwrap_or("unknown");
            tracing::info!(
                "Stage '{}' ({}) ready - {}/{} stages initialized", 
                stage_name, 
                self.stage_id,
                ready.len(),
                self.topology.num_stages()
            );
        }
        
        // Wait at barrier - unblocks all stages when last one is ready
        self.init_barrier.wait().await;
    }
    
    /// Check if this stage should wait before processing
    pub fn should_wait_before_processing(&self) -> bool {
        // Sources should wait for all stages to be ready
        self.topology.upstream_stages(self.stage_id).is_empty()
    }
    
    // ===== PROCESSING PHASE =====
    
    /// Track an event entering this stage
    pub async fn event_entered(&self) {
        let mut in_flight = self.in_flight.write().await;
        *in_flight.get_mut(&self.stage_id).unwrap() += 1;
    }
    
    /// Track an event leaving this stage
    pub async fn event_completed(&self) {
        let mut in_flight = self.in_flight.write().await;
        let count = in_flight.get_mut(&self.stage_id).unwrap();
        *count = count.saturating_sub(1);
    }
    
    /// Get current in-flight count
    pub async fn in_flight_count(&self) -> usize {
        let in_flight = self.in_flight.read().await;
        in_flight.get(&self.stage_id).copied().unwrap_or(0)
    }
    
    // ===== SHUTDOWN PHASE =====
    
    /// Check if shutdown has been requested
    pub fn is_shutdown_requested(&mut self) -> Option<ShutdownSignal> {
        match self.shutdown_rx.try_recv() {
            Ok(signal) => Some(signal),
            Err(_) => None,
        }
    }
    
    /// Wait for shutdown signal
    pub async fn wait_for_shutdown(&mut self) -> ShutdownSignal {
        self.shutdown_rx.recv().await
            .unwrap_or(ShutdownSignal::ForceShutdown("Channel closed".to_string()))
    }
    
    /// Signal that this stage has drained all events
    pub async fn signal_drained(&self) {
        // Sinks should wait for upstream stages to drain first
        if self.should_wait_for_upstream_drain() {
            self.wait_for_upstream_drain().await;
        }
        
        {
            let mut drained = self.drained_stages.write().await;
            drained.insert(self.stage_id);
            
            let stage_name = self.topology.stage_name(self.stage_id)
                .unwrap_or("unknown");
            let in_flight = self.in_flight.read().await;
            let remaining = in_flight.get(&self.stage_id).copied().unwrap_or(0);
            
            tracing::info!(
                "Stage '{}' ({}) drained - {} events remaining in pipeline",
                stage_name,
                self.stage_id,
                remaining
            );
        }
        
        // Signal completion
        self.drain_semaphore.add_permits(1);
    }
    
    fn should_wait_for_upstream_drain(&self) -> bool {
        // Sinks wait for upstream to drain
        self.topology.downstream_stages(self.stage_id).is_empty()
    }
    
    async fn wait_for_upstream_drain(&self) {
        let upstream = self.topology.upstream_stages(self.stage_id);
        
        loop {
            let drained = self.drained_stages.read().await;
            if upstream.iter().all(|&id| drained.contains(&id)) {
                break;
            }
            drop(drained);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[derive(Debug)]
pub struct DrainReport {
    pub total_stages: usize,
    pub drained_stages: usize,
    pub remaining_events: usize,
    pub undrained_stages: Vec<StageId>,
    pub elapsed: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::PipelineBuilder;
    
    #[tokio::test]
    async fn test_lifecycle_coordination() {
        // Build topology
        let mut builder = PipelineBuilder::new();
        let source = builder.add_stage(None);
        let transform = builder.add_stage(None);
        let sink = builder.add_stage(None);
        
        let topology = Arc::new(builder.build().unwrap());
        let lifecycle = PipelineLifecycle::new(topology);
        
        // Each stage gets one lifecycle handle
        let mut source_life = lifecycle.stage_lifecycle(source);
        let mut transform_life = lifecycle.stage_lifecycle(transform);
        let mut sink_life = lifecycle.stage_lifecycle(sink);
        
        // === INITIALIZATION PHASE ===
        
        // All stages signal ready
        tokio::join!(
            source_life.signal_ready(),
            transform_life.signal_ready(),
            sink_life.signal_ready()
        );
        
        // Sources should wait
        assert!(source_life.should_wait_before_processing());
        assert!(!transform_life.should_wait_before_processing());
        
        // === PROCESSING PHASE ===
        
        // Track some events
        source_life.event_entered().await;
        source_life.event_completed().await;
        
        // === SHUTDOWN PHASE ===
        
        // Begin shutdown
        lifecycle.begin_shutdown().await.unwrap();
        
        // Check shutdown signal received
        assert!(matches!(
            source_life.wait_for_shutdown().await,
            ShutdownSignal::BeginDrain
        ));
        
        // Signal stages drained
        source_life.signal_drained().await;
        transform_life.signal_drained().await;
        sink_life.signal_drained().await;
        
        // Verify complete drain
        let report = lifecycle.wait_for_drain(Duration::from_secs(1)).await.unwrap();
        assert_eq!(report.drained_stages, 3);
        assert_eq!(report.remaining_events, 0);
    }
    
    #[tokio::test]
    async fn test_initialization_barrier() {
        // Build topology with 3 stages
        let mut builder = PipelineBuilder::new();
        let s1 = builder.add_stage(None);
        let s2 = builder.add_stage(None);
        let s3 = builder.add_stage(None);
        
        let topology = Arc::new(builder.build().unwrap());
        let lifecycle = PipelineLifecycle::new(topology);
        
        let life1 = lifecycle.stage_lifecycle(s1);
        let life2 = lifecycle.stage_lifecycle(s2);
        let life3 = lifecycle.stage_lifecycle(s3);
        
        // Track when each stage becomes ready
        let ready_order = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let order_clone1 = ready_order.clone();
        let order_clone2 = ready_order.clone();
        let order_clone3 = ready_order.clone();
        
        // Spawn tasks that signal ready at different times
        let task1 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            life1.signal_ready().await;
            order_clone1.lock().await.push(1);
        });
        
        let task2 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            life2.signal_ready().await;
            order_clone2.lock().await.push(2);
        });
        
        let task3 = tokio::spawn(async move {
            // Stage 3 signals immediately
            life3.signal_ready().await;
            order_clone3.lock().await.push(3);
        });
        
        // Wait for all to complete
        tokio::join!(task1, task2, task3);
        
        // All should have signaled ready (barrier ensures all wait for each other)
        let order = ready_order.lock().await;
        assert_eq!(order.len(), 3);
        assert!(order.contains(&1));
        assert!(order.contains(&2));
        assert!(order.contains(&3));
    }
    
    #[tokio::test]
    async fn test_shutdown_with_inflight_events() {
        let mut builder = PipelineBuilder::new();
        // Sequential stages are automatically connected
        let source = builder.add_stage(None);
        let transform = builder.add_stage(None);
        
        let topology = Arc::new(builder.build().unwrap());
        let lifecycle = PipelineLifecycle::new(topology);
        
        let source_life = lifecycle.stage_lifecycle(source);
        let transform_life = lifecycle.stage_lifecycle(transform);
        
        // Signal ready (must be concurrent due to barrier)
        tokio::join!(
            source_life.signal_ready(),
            transform_life.signal_ready()
        );
        
        // Add in-flight events
        source_life.event_entered().await;
        source_life.event_entered().await;
        transform_life.event_entered().await;
        
        assert_eq!(source_life.in_flight_count().await, 2);
        assert_eq!(transform_life.in_flight_count().await, 1);
        
        // Begin shutdown
        lifecycle.begin_shutdown().await.unwrap();
        
        // Source drains its events
        source_life.event_completed().await;
        source_life.event_completed().await;
        source_life.signal_drained().await;
        
        // Transform still has an event
        let report = lifecycle.wait_for_drain(Duration::from_millis(50)).await.unwrap();
        assert_eq!(report.drained_stages, 1); // Only source drained
        assert_eq!(report.remaining_events, 1); // Transform's event
        assert_eq!(report.undrained_stages, vec![transform]);
        
        // Transform completes
        transform_life.event_completed().await;
        transform_life.signal_drained().await;
        
        // Now fully drained
        let report = lifecycle.wait_for_drain(Duration::from_millis(50)).await.unwrap();
        assert_eq!(report.drained_stages, 2);
        assert_eq!(report.remaining_events, 0);
    }
    
    #[tokio::test]
    async fn test_force_shutdown() {
        let mut builder = PipelineBuilder::new();
        // Sequential stages are automatically connected
        let source = builder.add_stage(None);
        let sink = builder.add_stage(None);
        
        let topology = Arc::new(builder.build().unwrap());
        let lifecycle = PipelineLifecycle::new(topology);
        
        let mut source_life = lifecycle.stage_lifecycle(source);
        let mut sink_life = lifecycle.stage_lifecycle(sink);
        
        // Must signal ready concurrently since barrier waits for all stages
        tokio::join!(
            source_life.signal_ready(),
            sink_life.signal_ready()
        );
        
        // Add in-flight event
        source_life.event_entered().await;
        
        // Force shutdown
        lifecycle.force_shutdown("emergency").await.unwrap();
        
        // Should receive force signal
        match source_life.wait_for_shutdown().await {
            ShutdownSignal::ForceShutdown(reason) => {
                assert_eq!(reason, "emergency");
            }
            _ => panic!("Expected ForceShutdown"),
        }
    }
    
    #[tokio::test]
    async fn test_sink_waits_for_upstream_drain() {
        let mut builder = PipelineBuilder::new();
        // Sequential stages are automatically connected
        let source = builder.add_stage(None);
        let sink = builder.add_stage(None);
        
        let topology = Arc::new(builder.build().unwrap());
        let lifecycle = PipelineLifecycle::new(topology);
        
        let source_life = lifecycle.stage_lifecycle(source);
        let sink_life = lifecycle.stage_lifecycle(sink);
        
        // Signal ready (must be concurrent due to barrier)
        tokio::join!(
            source_life.signal_ready(),
            sink_life.signal_ready()
        );
        
        // Begin shutdown
        lifecycle.begin_shutdown().await.unwrap();
        
        // Verify sink should wait
        assert!(sink_life.should_wait_for_upstream_drain());
        
        // Track drain order
        let drain_order = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let order_clone1 = drain_order.clone();
        let order_clone2 = drain_order.clone();
        
        // Spawn concurrent drain attempts
        let source_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            source_life.signal_drained().await;
            order_clone1.lock().await.push("source");
        });
        
        let sink_task = tokio::spawn(async move {
            // Sink tries to drain immediately but should wait
            sink_life.signal_drained().await;
            order_clone2.lock().await.push("sink");
        });
        
        tokio::join!(source_task, sink_task);
        
        // Verify order - source should drain before sink
        let order = drain_order.lock().await;
        assert_eq!(order.len(), 2);
        assert_eq!(order[0], "source");
        assert_eq!(order[1], "sink");
    }
    
    #[tokio::test]
    async fn test_multiple_shutdown_signals() {
        let mut builder = PipelineBuilder::new();
        // Sequential stages are automatically connected
        let source = builder.add_stage(None);
        let sink = builder.add_stage(None);
        
        let topology = Arc::new(builder.build().unwrap());
        let lifecycle = PipelineLifecycle::new(topology);
        
        let mut source_life1 = lifecycle.stage_lifecycle(source);
        let mut source_life2 = lifecycle.stage_lifecycle(source); // Same stage, different handle
        let sink_life = lifecycle.stage_lifecycle(sink);
        
        // Signal ready (must be concurrent due to barrier)
        tokio::join!(
            source_life1.signal_ready(),
            sink_life.signal_ready()
        );
        
        // Send shutdown
        lifecycle.begin_shutdown().await.unwrap();
        
        // Both handles should receive the signal
        assert!(matches!(
            source_life1.is_shutdown_requested(),
            Some(ShutdownSignal::BeginDrain)
        ));
        assert!(matches!(
            source_life2.is_shutdown_requested(),
            Some(ShutdownSignal::BeginDrain)
        ));
    }
}