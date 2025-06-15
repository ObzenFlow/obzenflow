//! Pipeline shutdown coordination
//! 
//! Ensures graceful shutdown with proper draining of in-flight events

use crate::topology::{PipelineTopology, StageId};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, Semaphore};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Coordinates graceful pipeline shutdown
pub struct ShutdownCoordinator {
    topology: Arc<PipelineTopology>,
    /// Tracks which stages have completed draining
    drained_stages: Arc<RwLock<HashSet<StageId>>>,
    /// Tracks in-flight events per stage
    in_flight: Arc<RwLock<HashMap<StageId, usize>>>,
    /// Shutdown signal broadcaster
    shutdown_tx: broadcast::Sender<ShutdownSignal>,
    /// Drain completion semaphore
    drain_semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub enum ShutdownSignal {
    /// Begin graceful shutdown - drain in-flight events
    BeginDrain,
    /// Force immediate shutdown
    ForceShutdown(String),
}

#[derive(Clone)]
pub struct StageShutdownHandle {
    stage_id: StageId,
    topology: Arc<PipelineTopology>,
    drained_stages: Arc<RwLock<HashSet<StageId>>>,
    in_flight: Arc<RwLock<HashMap<StageId, usize>>>,
    shutdown_rx: broadcast::Receiver<ShutdownSignal>,
    drain_semaphore: Arc<Semaphore>,
}

impl ShutdownCoordinator {
    pub fn new(topology: Arc<PipelineTopology>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);
        let num_stages = topology.num_stages();
        
        Self {
            topology,
            drained_stages: Arc::new(RwLock::new(HashSet::new())),
            in_flight: Arc::new(RwLock::new(HashMap::new())),
            shutdown_tx,
            drain_semaphore: Arc::new(Semaphore::new(0)),
        }
    }
    
    /// Get a shutdown handle for a specific stage
    pub fn stage_handle(&self, stage_id: StageId) -> StageShutdownHandle {
        // Initialize in-flight counter
        {
            let mut in_flight = self.in_flight.blocking_write();
            in_flight.insert(stage_id, 0);
        }
        
        StageShutdownHandle {
            stage_id,
            topology: self.topology.clone(),
            drained_stages: self.drained_stages.clone(),
            in_flight: self.in_flight.clone(),
            shutdown_rx: self.shutdown_tx.subscribe(),
            drain_semaphore: self.drain_semaphore.clone(),
        }
    }
    
    /// Initiate graceful shutdown
    pub async fn begin_shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Beginning graceful pipeline shutdown");
        self.shutdown_tx.send(ShutdownSignal::BeginDrain)?;
        Ok(())
    }
    
    /// Force immediate shutdown
    pub async fn force_shutdown(&self, reason: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::warn!("Forcing pipeline shutdown: {}", reason);
        self.shutdown_tx.send(ShutdownSignal::ForceShutdown(reason.to_string()))?;
        Ok(())
    }
    
    /// Wait for all stages to drain (with timeout)
    pub async fn wait_for_drain(&self, timeout: Duration) -> Result<DrainReport, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let deadline = start + timeout;
        
        // Wait for all stages to signal drain complete
        let total_stages = self.topology.num_stages();
        
        for _ in 0..total_stages {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, self.drain_semaphore.acquire()).await {
                Ok(Ok(_permit)) => {
                    // One more stage drained
                }
                Ok(Err(e)) => return Err(format!("Semaphore error: {}", e).into()),
                Err(_) => {
                    // Timeout - return partial drain report
                    break;
                }
            }
        }
        
        // Generate drain report
        let drained = self.drained_stages.read().await;
        let in_flight = self.in_flight.read().await;
        
        let mut remaining_events = 0;
        let mut undrained_stages = Vec::new();
        
        for (stage_id, &count) in in_flight.iter() {
            if count > 0 && !drained.contains(stage_id) {
                remaining_events += count;
                undrained_stages.push(*stage_id);
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
}

impl StageShutdownHandle {
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
    
    /// Check if shutdown has been requested
    pub fn is_shutdown_requested(&mut self) -> Option<ShutdownSignal> {
        match self.shutdown_rx.try_recv() {
            Ok(signal) => Some(signal),
            Err(_) => None,
        }
    }
    
    /// Wait for shutdown signal
    pub async fn wait_for_shutdown(&mut self) -> ShutdownSignal {
        self.shutdown_rx.recv().await.unwrap_or(ShutdownSignal::ForceShutdown("Channel closed".to_string()))
    }
    
    /// Signal that this stage has drained
    pub async fn signal_drained(&self) {
        // Check if we should wait for upstream stages first
        if self.should_wait_for_upstream().await {
            self.wait_for_upstream_drain().await;
        }
        
        // Mark ourselves as drained
        {
            let mut drained = self.drained_stages.write().await;
            drained.insert(self.stage_id);
            
            let stage_name = self.topology.stage_name(self.stage_id).unwrap_or("unknown");
            let in_flight = self.in_flight.read().await;
            let remaining = in_flight.get(&self.stage_id).copied().unwrap_or(0);
            
            tracing::info!(
                "Stage '{}' ({}) drained - {} events remaining in pipeline",
                stage_name,
                self.stage_id,
                remaining
            );
        }
        
        // Signal drain complete
        self.drain_semaphore.add_permits(1);
    }
    
    /// Check if this stage should wait for upstream stages to drain first
    async fn should_wait_for_upstream(&self) -> bool {
        // Sinks should wait for all upstream stages
        self.topology.downstream_stages(self.stage_id).is_empty()
    }
    
    /// Wait for all upstream stages to drain
    async fn wait_for_upstream_drain(&self) {
        let upstream = self.topology.upstream_stages(self.stage_id);
        
        loop {
            let drained = self.drained_stages.read().await;
            if upstream.iter().all(|&id| drained.contains(&id)) {
                break;
            }
            drop(drained);
            
            // Check periodically
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    
    /// Get current in-flight count for this stage
    pub async fn in_flight_count(&self) -> usize {
        let in_flight = self.in_flight.read().await;
        in_flight.get(&self.stage_id).copied().unwrap_or(0)
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
    async fn test_shutdown_coordination() {
        // Build topology
        let mut builder = PipelineBuilder::new();
        let source = builder.add_stage(Some("source".to_string()));
        let transform = builder.add_stage(Some("transform".to_string()));
        let sink = builder.add_stage(Some("sink".to_string()));
        
        let topology = Arc::new(builder.build().unwrap());
        let coordinator = ShutdownCoordinator::new(topology.clone());
        
        // Get handles for each stage
        let mut source_handle = coordinator.stage_handle(source);
        let mut transform_handle = coordinator.stage_handle(transform);
        let mut sink_handle = coordinator.stage_handle(sink);
        
        // Simulate some in-flight events
        source_handle.event_entered().await;
        source_handle.event_entered().await;
        
        // Begin shutdown
        coordinator.begin_shutdown().await.unwrap();
        
        // Stages should receive shutdown signal
        assert!(matches!(
            source_handle.wait_for_shutdown().await,
            ShutdownSignal::BeginDrain
        ));
        
        // Simulate draining
        source_handle.event_completed().await;
        source_handle.event_completed().await;
        source_handle.signal_drained().await;
        
        transform_handle.signal_drained().await;
        sink_handle.signal_drained().await;
        
        // Wait for complete drain
        let report = coordinator.wait_for_drain(Duration::from_secs(1)).await.unwrap();
        assert_eq!(report.drained_stages, 3);
        assert_eq!(report.remaining_events, 0);
    }
}