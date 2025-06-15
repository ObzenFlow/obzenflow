//! Pipeline initialization coordination
//! 
//! Ensures all stages are properly initialized before event processing begins

use crate::topology::{PipelineTopology, StageId};
use std::sync::Arc;
use tokio::sync::{Barrier, RwLock};
use std::collections::HashSet;

/// Coordinates pipeline initialization to prevent race conditions
pub struct InitializationCoordinator {
    topology: Arc<PipelineTopology>,
    init_barrier: Arc<Barrier>,
    ready_stages: Arc<RwLock<HashSet<StageId>>>,
    total_stages: usize,
}

impl InitializationCoordinator {
    /// Create a new coordinator for the given topology
    pub fn new(topology: Arc<PipelineTopology>) -> Self {
        let total_stages = topology.num_stages();
        Self {
            topology,
            init_barrier: Arc::new(Barrier::new(total_stages)),
            ready_stages: Arc::new(RwLock::new(HashSet::new())),
            total_stages,
        }
    }
    
    /// Get a stage initializer for a specific stage
    pub fn stage_initializer(&self, stage_id: StageId) -> StageInitializer {
        StageInitializer {
            stage_id,
            init_barrier: self.init_barrier.clone(),
            ready_stages: self.ready_stages.clone(),
            topology: self.topology.clone(),
        }
    }
    
    /// Check if all stages are ready
    pub async fn all_stages_ready(&self) -> bool {
        let ready = self.ready_stages.read().await;
        ready.len() == self.total_stages
    }
    
    /// Wait for all stages to be ready
    pub async fn wait_for_all_stages(&self) {
        // This will unblock when all stages have called signal_ready()
        self.init_barrier.wait().await;
    }
}

/// Per-stage initialization coordinator
pub struct StageInitializer {
    stage_id: StageId,
    init_barrier: Arc<Barrier>,
    ready_stages: Arc<RwLock<HashSet<StageId>>>,
    topology: Arc<PipelineTopology>,
}

impl StageInitializer {
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
        
        // Wait at barrier - this will unblock all stages simultaneously
        // when the last stage signals ready
        self.init_barrier.wait().await;
    }
    
    /// Check if this stage should wait before processing
    pub fn should_wait_before_processing(&self) -> bool {
        // Sources should wait for all stages to be ready
        self.topology.upstream_stages(self.stage_id).is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::PipelineBuilder;
    
    #[tokio::test]
    async fn test_initialization_coordination() {
        // Build a simple topology
        let mut builder = PipelineBuilder::new();
        let source = builder.add_stage(Some("source".to_string()));
        let transform = builder.add_stage(Some("transform".to_string()));
        let sink = builder.add_stage(Some("sink".to_string()));
        
        let topology = Arc::new(builder.build().unwrap());
        let coordinator = InitializationCoordinator::new(topology.clone());
        
        // Simulate stages initializing concurrently
        let mut handles = vec![];
        
        for stage_id in [source, transform, sink] {
            let init = coordinator.stage_initializer(stage_id);
            let handle = tokio::spawn(async move {
                // Simulate some initialization work
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                
                // Signal ready
                init.signal_ready().await;
                
                if init.should_wait_before_processing() {
                    // Sources wait for coordination
                    println!("Source stage waiting for all stages to be ready");
                }
            });
            handles.push(handle);
        }
        
        // All handles should complete successfully
        for handle in handles {
            handle.await.unwrap();
        }
        
        // All stages should be ready
        assert!(coordinator.all_stages_ready().await);
    }
}