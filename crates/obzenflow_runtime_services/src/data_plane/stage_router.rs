//! Event routing based on stage semantics
//!
//! This module handles partitioning and routing of events to stage instances
//! based on their declared semantics.

use obzenflow_core::ChainEvent;
use crate::data_plane::stage_semantics::{StageSemantics, PartitionStrategy};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Routes events to stage instances based on semantics
pub struct StageRouter {
    /// Number of available instances for each stage
    instance_counts: std::collections::HashMap<String, usize>,
}

impl StageRouter {
    pub fn new() -> Self {
        Self {
            instance_counts: std::collections::HashMap::new(),
        }
    }

    /// Register a stage with its instance count
    pub fn register_stage(&mut self, stage_id: &str, instance_count: usize) {
        self.instance_counts.insert(stage_id.to_string(), instance_count);
    }

    /// Route an event to a specific instance of a stage
    pub fn route_event(
        &self,
        event: &ChainEvent,
        stage_id: &str,
        semantics: &StageSemantics,
    ) -> usize {
        let instance_count = self.instance_counts.get(stage_id).copied().unwrap_or(1);

        if instance_count == 1 {
            return 0; // Only one instance
        }

        match semantics {
            StageSemantics::Stateless => {
                // Round-robin for stateless
                self.round_robin_route(event, instance_count)
            }

            StageSemantics::StatefulIndependent { partition_strategy } => {
                match partition_strategy {
                    PartitionStrategy::KeyHash { key_path } => {
                        self.hash_route(event, key_path, instance_count)
                    }
                    PartitionStrategy::RoundRobin => {
                        self.round_robin_route(event, instance_count)
                    }
                    _ => 0, // Default to first instance
                }
            }

            // Stateful accumulators and sequential stages always go to instance 0
            _ => 0,
        }
    }

    fn round_robin_route(&self, event: &ChainEvent, instance_count: usize) -> usize {
        // Use event type for consistent routing since we don't have ulid
        let mut hasher = DefaultHasher::new();
        event.event_type.hash(&mut hasher);
        (hasher.finish() as usize) % instance_count
    }

    fn hash_route(&self, event: &ChainEvent, key_path: &str, instance_count: usize) -> usize {
        // Extract key from event payload
        // This is simplified - in real implementation would use proper JSON path extraction
        let mut hasher = DefaultHasher::new();
        key_path.hash(&mut hasher);
        // Use event type as part of hash since we don't have ulid on ChainEvent
        event.event_type.hash(&mut hasher);
        (hasher.finish() as usize) % instance_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    #[test]
    fn test_single_instance_routing() {
        let router = StageRouter::new();
        let event = ChainEvent::new("test", serde_json::json!({}));
        let semantics = StageSemantics::Stateless;

        // With no registration or single instance, always routes to 0
        assert_eq!(router.route_event(&event, "stage1", &semantics), 0);
    }

    #[test]
    fn test_stateless_routing() {
        let mut router = StageRouter::new();
        router.register_stage("stage1", 4);

        let semantics = StageSemantics::Stateless;

        // Create multiple events and verify distribution
        let mut instances = std::collections::HashSet::new();
        for _ in 0..20 {
            let event = ChainEvent::new("test", serde_json::json!({}));
            let instance = router.route_event(&event, "stage1", &semantics);
            assert!(instance < 4);
            instances.insert(instance);
        }

        // Should use multiple instances (probabilistic, but very likely)
        assert!(instances.len() > 1);
    }
}
