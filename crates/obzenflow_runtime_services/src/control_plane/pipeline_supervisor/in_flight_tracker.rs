//! In-flight event tracking for pipeline components
//!
//! This module tracks events as they flow through the pipeline, enabling:
//! - Graceful shutdown (wait for in-flight events to complete)
//! - Backpressure monitoring
//! - Performance metrics

use obzenflow_topology_services::stages::StageId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Tracks in-flight events across all pipeline components
#[derive(Clone)]
pub struct InFlightTracker {
    /// Per-stage event counts
    stage_counts: Arc<RwLock<HashMap<StageId, Arc<AtomicUsize>>>>,
    /// Total events in flight across all stages
    total_count: Arc<AtomicUsize>,
}

impl InFlightTracker {
    /// Create a new tracker for the given stages
    pub fn new(stage_ids: Vec<StageId>) -> Self {
        let mut stage_counts = HashMap::new();
        for stage_id in stage_ids {
            stage_counts.insert(stage_id, Arc::new(AtomicUsize::new(0)));
        }
        
        Self {
            stage_counts: Arc::new(RwLock::new(stage_counts)),
            total_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Record an event entering a stage
    pub async fn event_entered(&self, stage_id: StageId) -> Result<(), String> {
        let counts = self.stage_counts.read().await;
        
        if let Some(counter) = counts.get(&stage_id) {
            counter.fetch_add(1, Ordering::SeqCst);
            self.total_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        } else {
            Err(format!("Unknown stage: {:?}", stage_id))
        }
    }

    /// Record an event leaving a stage
    pub async fn event_completed(&self, stage_id: StageId) -> Result<(), String> {
        let counts = self.stage_counts.read().await;
        
        if let Some(counter) = counts.get(&stage_id) {
            // Saturating sub to prevent underflow
            let prev = counter.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| {
                Some(val.saturating_sub(1))
            }).unwrap_or(0);
            
            if prev > 0 {
                self.total_count.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(())
        } else {
            Err(format!("Unknown stage: {:?}", stage_id))
        }
    }

    /// Get the current count for a specific stage
    pub async fn stage_count(&self, stage_id: StageId) -> usize {
        let counts = self.stage_counts.read().await;
        counts.get(&stage_id)
            .map(|counter| counter.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Get the total count across all stages
    pub fn total_count(&self) -> usize {
        self.total_count.load(Ordering::SeqCst)
    }

    /// Get counts for all stages
    pub async fn all_counts(&self) -> HashMap<StageId, usize> {
        let counts = self.stage_counts.read().await;
        counts.iter()
            .map(|(stage_id, counter)| (*stage_id, counter.load(Ordering::SeqCst)))
            .collect()
    }

    /// Check if all stages have drained (no in-flight events)
    pub fn is_fully_drained(&self) -> bool {
        self.total_count.load(Ordering::SeqCst) == 0
    }

    /// Get a detailed drain report
    pub async fn drain_report(&self) -> DrainReport {
        let all_counts = self.all_counts().await;
        let stages_with_events: Vec<(StageId, usize)> = all_counts
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .collect();
        
        DrainReport {
            total_in_flight: self.total_count(),
            stages_with_events,
            is_drained: self.is_fully_drained(),
        }
    }

    /// Create a stage-specific handle for easier use
    pub fn stage_handle(&self, stage_id: StageId) -> StageInFlightHandle {
        StageInFlightHandle {
            stage_id,
            tracker: self.clone(),
        }
    }
}

/// Handle for tracking in-flight events for a specific stage
pub struct StageInFlightHandle {
    stage_id: StageId,
    tracker: InFlightTracker,
}

impl StageInFlightHandle {
    /// Record an event entering this stage
    pub async fn event_entered(&self) -> Result<(), String> {
        self.tracker.event_entered(self.stage_id).await
    }

    /// Record an event leaving this stage
    pub async fn event_completed(&self) -> Result<(), String> {
        self.tracker.event_completed(self.stage_id).await
    }

    /// Get the current count for this stage
    pub async fn count(&self) -> usize {
        self.tracker.stage_count(self.stage_id).await
    }

    /// Check if this stage is drained
    pub async fn is_drained(&self) -> bool {
        self.count().await == 0
    }
}

/// Report of current drain status
#[derive(Debug, Clone)]
pub struct DrainReport {
    /// Total events in flight across all stages
    pub total_in_flight: usize,
    /// Stages that still have events in flight
    pub stages_with_events: Vec<(StageId, usize)>,
    /// Whether all stages are fully drained
    pub is_drained: bool,
}

impl DrainReport {
    /// Get a human-readable summary
    pub fn summary(&self) -> String {
        if self.is_drained {
            "All stages drained".to_string()
        } else {
            format!(
                "{} events in flight across {} stages",
                self.total_in_flight,
                self.stages_with_events.len()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_flight_tracking() {
        let stages = vec![StageId::new(1), StageId::new(2), StageId::new(3)];
        let tracker = InFlightTracker::new(stages);
        
        // Initially drained
        assert!(tracker.is_fully_drained());
        assert_eq!(tracker.total_count(), 0);
        
        // Add events to stage 1
        tracker.event_entered(StageId::new(1)).await.unwrap();
        tracker.event_entered(StageId::new(1)).await.unwrap();
        assert_eq!(tracker.stage_count(StageId::new(1)).await, 2);
        assert_eq!(tracker.total_count(), 2);
        assert!(!tracker.is_fully_drained());
        
        // Add event to stage 2
        tracker.event_entered(StageId::new(2)).await.unwrap();
        assert_eq!(tracker.total_count(), 3);
        
        // Complete one event from stage 1
        tracker.event_completed(StageId::new(1)).await.unwrap();
        assert_eq!(tracker.stage_count(StageId::new(1)).await, 1);
        assert_eq!(tracker.total_count(), 2);
        
        // Complete all events
        tracker.event_completed(StageId::new(1)).await.unwrap();
        tracker.event_completed(StageId::new(2)).await.unwrap();
        assert!(tracker.is_fully_drained());
    }

    #[tokio::test]
    async fn test_stage_handle() {
        let stages = vec![StageId::new(1), StageId::new(2)];
        let tracker = InFlightTracker::new(stages);
        
        let stage1 = tracker.stage_handle(StageId::new(1));
        let stage2 = tracker.stage_handle(StageId::new(2));
        
        // Track events using handles
        stage1.event_entered().await.unwrap();
        stage2.event_entered().await.unwrap();
        stage2.event_entered().await.unwrap();
        
        assert_eq!(stage1.count().await, 1);
        assert_eq!(stage2.count().await, 2);
        assert!(!stage1.is_drained().await);
        
        // Complete stage 1 event
        stage1.event_completed().await.unwrap();
        assert!(stage1.is_drained().await);
        assert!(!tracker.is_fully_drained()); // Stage 2 still has events
    }

    #[tokio::test]
    async fn test_drain_report() {
        let stages = vec![StageId::new(1), StageId::new(2), StageId::new(3)];
        let tracker = InFlightTracker::new(stages);
        
        // Add events to stages 1 and 3
        tracker.event_entered(StageId::new(1)).await.unwrap();
        tracker.event_entered(StageId::new(1)).await.unwrap();
        tracker.event_entered(StageId::new(3)).await.unwrap();
        
        let report = tracker.drain_report().await;
        assert_eq!(report.total_in_flight, 3);
        assert_eq!(report.stages_with_events.len(), 2);
        assert!(!report.is_drained);
        
        // Verify the summary
        let summary = report.summary();
        assert!(summary.contains("3 events in flight across 2 stages"));
    }

    #[tokio::test]
    async fn test_unknown_stage() {
        let stages = vec![StageId::new(1)];
        let tracker = InFlightTracker::new(stages);
        
        // Try to track unknown stage
        let result = tracker.event_entered(StageId::new(99)).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown stage"));
    }

    #[tokio::test]
    async fn test_underflow_protection() {
        let stages = vec![StageId::new(1)];
        let tracker = InFlightTracker::new(stages);
        
        // Complete without entering (should not panic or underflow)
        tracker.event_completed(StageId::new(1)).await.unwrap();
        assert_eq!(tracker.stage_count(StageId::new(1)).await, 0);
        assert_eq!(tracker.total_count(), 0);
        
        // Add one event
        tracker.event_entered(StageId::new(1)).await.unwrap();
        assert_eq!(tracker.stage_count(StageId::new(1)).await, 1);
        
        // Complete twice (second should be safe)
        tracker.event_completed(StageId::new(1)).await.unwrap();
        tracker.event_completed(StageId::new(1)).await.unwrap();
        assert_eq!(tracker.stage_count(StageId::new(1)).await, 0);
        assert_eq!(tracker.total_count(), 0);
    }
}