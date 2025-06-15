use crate::step::Result;
use crate::event_store::EventEnvelope;
use crate::event_store::flow_log::FlowEventLog;
use crate::topology::StageId;
use std::sync::Arc;
use ulid::Ulid;

/// Reader that provides access to the flow event log
pub struct EventReader {
    flow_log: Arc<FlowEventLog>,
}

impl EventReader {
    pub(crate) fn new(flow_log: Arc<FlowEventLog>) -> Self {
        Self {
            flow_log,
        }
    }
    
    /// Read all events in causal order
    /// 
    /// Events are sorted by:
    /// 1. Vector clock partial order (causally related events)
    /// 2. ULID for tie-breaking (concurrent events)
    pub async fn read_causal_order(&self) -> Result<Vec<EventEnvelope>> {
        let mut all_events = self.flow_log.read_all().await?;
        
        // Sort by partial order with ULID tiebreaking
        all_events.sort_by(|a, b| {
            match a.vector_clock.partial_cmp(&b.vector_clock) {
                Some(ordering) => ordering,
                None => {
                    // Concurrent events - use ULID for deterministic ordering
                    a.event.ulid.cmp(&b.event.ulid)
                }
            }
        });
        
        Ok(all_events)
    }
    
    /// Read events from a specific stage
    pub async fn read_stage_events(&self, stage_id: StageId) -> Result<Vec<EventEnvelope>> {
        let mut stage_events = self.flow_log.read_stage_events(stage_id).await?;
        
        // Sort by causal order
        stage_events.sort_by(|a, b| {
            match a.vector_clock.partial_cmp(&b.vector_clock) {
                Some(ordering) => ordering,
                None => a.event.ulid.cmp(&b.event.ulid),
            }
        });
        
        Ok(stage_events)
    }
    
    /// Read events that happened after a given event
    /// Useful for resuming processing from a checkpoint
    pub async fn read_after(&self, after: &EventEnvelope) -> Result<Vec<EventEnvelope>> {
        let all_events = self.read_causal_order().await?;
        
        Ok(all_events
            .into_iter()
            .filter(|event| {
                // Include if it happened after OR is concurrent
                after.vector_clock.happened_before(&event.vector_clock) ||
                after.vector_clock.concurrent_with(&event.vector_clock)
            })
            .collect())
    }
    
    /// Read a specific event by ID - O(1) with index
    pub async fn read_event(&self, event_id: &Ulid) -> Result<EventEnvelope> {
        match self.flow_log.read_event(event_id).await? {
            Some(event) => Ok(event),
            None => Err(format!("Event {} not found", event_id).into()),
        }
    }
    
}