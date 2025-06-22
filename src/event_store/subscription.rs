//! Push-based event subscription system for FLOWIP-011

use crate::step::Result;
use crate::event_store::{EventReader, EventEnvelope};
use crate::event_store::constants::*;
use crate::topology::StageId;
use ulid::Ulid;
use std::collections::{VecDeque, HashSet};
use tokio::sync::mpsc;

/// Filter for subscriptions - extensible for future patterns
#[derive(Clone, Debug)]
pub struct SubscriptionFilter {
    /// Which stages to receive events from
    pub upstream_stages: Vec<StageId>,
    // Future: merge_strategy, routing_key, group_id
}

impl SubscriptionFilter {
    /// Create a filter that subscribes to all events (from all stages)
    pub fn all() -> Self {
        Self {
            upstream_stages: vec![], // Empty means all stages
        }
    }
}

/// Push-based event subscription with causal ordering
pub struct EventSubscription {
    pub(crate) id: Ulid,
    pub(crate) receiver: mpsc::Receiver<EventNotification>,
    /// Buffer for maintaining causal order
    pub(crate) pending_buffer: VecDeque<EventEnvelope>,
    /// Reader for fetching full events
    pub(crate) reader: EventReader,
    /// Filter defining which stages we're subscribed to
    pub(crate) filter: SubscriptionFilter,
    /// Track which upstream stages have completed (FLOWIP-058)
    pub(crate) completed_upstreams: HashSet<StageId>,
    /// Channel for receiving EOF notifications
    pub(crate) eof_receiver: mpsc::Receiver<EofNotification>,
}

/// Lightweight notification (not the full event)
#[derive(Debug, Clone)]
pub struct EventNotification {
    pub event_id: Ulid,
    pub stage_id: StageId,
}

/// Subscription event types for FLOWIP-058 deterministic shutdown
#[derive(Debug, Clone)]
pub enum SubscriptionEvent {
    /// Normal event batch
    Events(Vec<EventEnvelope>),
    
    /// Upstream stage has completed and will send no more events
    EndOfStream { 
        stage_id: StageId,
        final_sequence: u64,
        /// Whether this was natural completion (vs shutdown-induced)
        natural_completion: bool,
    },
    
    /// All upstream stages have completed
    AllUpstreamsComplete,
}

/// EOF notification for a completed stage
#[derive(Debug, Clone)]
pub struct EofNotification {
    pub stage_id: StageId,
    pub final_sequence: u64,
    pub natural_completion: bool,
}

impl EventSubscription {
    /// Receive events or EOF signals (FLOWIP-058)
    pub async fn recv_with_eof(&mut self) -> Result<SubscriptionEvent> {
        // Check if we've received EOF from all upstreams
        if self.all_upstreams_complete() {
            return Ok(SubscriptionEvent::AllUpstreamsComplete);
        }
        
        // Use select to wait for either events or EOF
        tokio::select! {
            // Try to receive EOF notification
            Some(eof) = self.eof_receiver.recv() => {
                self.mark_upstream_complete(eof.stage_id.clone());
                
                // Return the EOF event first
                let event = SubscriptionEvent::EndOfStream {
                    stage_id: eof.stage_id,
                    final_sequence: eof.final_sequence,
                    natural_completion: eof.natural_completion,
                };
                
                Ok(event)
            }
            
            // Try to receive regular events
            Some(notification) = self.receiver.recv() => {
                // We got a notification, fetch the event
                let mut events = vec![];
                
                // Try to get the event
                match self.reader.read_event(&notification.event_id).await {
                    Ok(envelope) => {
                        events.push(envelope);
                    }
                    Err(e) => {
                        tracing::error!("Failed to fetch event {}: {}", notification.event_id, e);
                    }
                }
                
                // Try to batch more if available
                while events.len() < MAX_BATCH_SIZE {
                    match self.receiver.try_recv() {
                        Ok(notification) => {
                            match self.reader.read_event(&notification.event_id).await {
                                Ok(envelope) => events.push(envelope),
                                Err(e) => {
                                    tracing::error!("Failed to fetch event {}: {}", notification.event_id, e);
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
                
                // Sort by vector clock
                events.sort_by(|a, b| {
                    match a.vector_clock.partial_cmp(&b.vector_clock) {
                        Some(ordering) => ordering,
                        None => a.event.ulid.cmp(&b.event.ulid),
                    }
                });
                
                Ok(SubscriptionEvent::Events(events))
            }
            
            // Both channels closed
            else => {
                Ok(SubscriptionEvent::AllUpstreamsComplete)
            }
        }
    }
    
    /// Track which upstreams have completed
    fn mark_upstream_complete(&mut self, stage_id: StageId) {
        self.completed_upstreams.insert(stage_id);
    }
    
    /// Check if all upstreams are done
    fn all_upstreams_complete(&self) -> bool {
        // If no specific upstreams defined, we can't track completion
        if self.filter.upstream_stages.is_empty() {
            return false;
        }
        
        self.completed_upstreams.len() == self.filter.upstream_stages.len()
    }

    /// Receive next batch of causally ordered events
    /// Blocks until at least one event is available
    pub async fn recv_causal_batch(&mut self) -> Result<Vec<EventEnvelope>> {
        
        // Wait for at least one notification (blocking)
        let Some(first_notification) = self.receiver.recv().await else {
            tracing::debug!("Subscription {} channel closed, returning empty batch", self.id);
            return Ok(vec![]); // Channel closed
        };
        tracing::trace!("Subscription {} received first notification for event {}", self.id, first_notification.event_id);
        
        // Collect additional ready notifications (non-blocking)
        let mut notifications = Vec::with_capacity(MAX_BATCH_SIZE);
        notifications.push(first_notification);
        
        // Try to collect more notifications without waiting
        // This allows batching when events arrive close together
        while notifications.len() < MAX_BATCH_SIZE {
            match self.receiver.try_recv() {
                Ok(notification) => notifications.push(notification),
                Err(_) => break, // No more ready
            }
        }
        
        // Fetch full events with retry
        let mut events = Vec::with_capacity(notifications.len());
        for notification in notifications {
            // Retry up to 3 times with small delay
            let mut retry_count = 0;
            loop {
                match self.reader.read_event(&notification.event_id).await {
                    Ok(envelope) => {
                        events.push(envelope);
                        break;
                    }
                    Err(e) if retry_count < 3 => {
                        retry_count += 1;
                        tracing::debug!(
                            "Retry {} reading event {}: {}", 
                            retry_count, notification.event_id, e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to fetch event {} after {} retries: {} - EVENT LOST!", 
                            notification.event_id, retry_count, e
                        );
                        // THIS IS WHERE EVENTS ARE LOST - notification received but event not readable
                        break; // Give up after retries
                    }
                }
            }
        }
        
        // Sort by vector clock for causal order
        events.sort_by(|a, b| {
            match a.vector_clock.partial_cmp(&b.vector_clock) {
                Some(ordering) => ordering,
                None => a.event.ulid.cmp(&b.event.ulid), // Concurrent: use ULID
            }
        });
        
        Ok(events)
    }
    
    /// Get subscription ID for debugging
    pub fn id(&self) -> Ulid {
        self.id
    }
    
    /// Check if subscription is still active
    pub fn is_active(&self) -> bool {
        !self.receiver.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_store::{EventStore, StageSemantics};
    use crate::chain_event::ChainEvent;
    
    #[tokio::test]
    async fn test_eof_signal_single_upstream() {
        // Create event store
        let store = EventStore::for_testing().await;
        
        // Create a subscription for stage_a
        let stage_1 = StageId::from_u32(1);
        let filter = SubscriptionFilter {
            upstream_stages: vec![stage_1], // Subscribe to stage 1
        };
        let mut subscription = store.subscribe(filter).await.unwrap();
        
        // Signal stage 1 completion
        store.signal_stage_complete(stage_1, true).await.unwrap();
        
        // Should receive EOF signal
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::EndOfStream { stage_id, natural_completion, .. } => {
                assert_eq!(stage_id, stage_1);
                assert!(natural_completion);
            }
            _ => panic!("Expected EndOfStream event"),
        }
        
        // Next call should return AllUpstreamsComplete
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::AllUpstreamsComplete => {},
            _ => panic!("Expected AllUpstreamsComplete"),
        }
    }
    
    #[tokio::test]
    async fn test_eof_signal_multiple_upstreams() {
        let store = EventStore::for_testing().await;
        
        // Subscribe to multiple stages
        let stage_1 = StageId::from_u32(1);
        let stage_2 = StageId::from_u32(2);
        let stage_3 = StageId::from_u32(3);
        let filter = SubscriptionFilter {
            upstream_stages: vec![stage_1, stage_2, stage_3],
        };
        let mut subscription = store.subscribe(filter).await.unwrap();
        
        // Signal stage 1 completion
        store.signal_stage_complete(stage_1, false).await.unwrap();
        
        // Should receive EOF for stage 1
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::EndOfStream { stage_id, natural_completion, .. } => {
                assert_eq!(stage_id, stage_1);
                assert!(!natural_completion);
            }
            _ => panic!("Expected EndOfStream for stage 1"),
        }
        
        // Signal stage 2 completion
        store.signal_stage_complete(stage_2, true).await.unwrap();
        
        // Should receive EOF for stage 2
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::EndOfStream { stage_id, natural_completion, .. } => {
                assert_eq!(stage_id, stage_2);
                assert!(natural_completion);
            }
            _ => panic!("Expected EndOfStream for stage 2"),
        }
        
        // Signal stage 3 completion
        store.signal_stage_complete(stage_3, false).await.unwrap();
        
        // Should receive EOF for stage 3
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::EndOfStream { stage_id, .. } => {
                assert_eq!(stage_id, stage_3);
            }
            _ => panic!("Expected EndOfStream for stage 3"),
        }
        
        // Now all upstreams are complete
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::AllUpstreamsComplete => {},
            _ => panic!("Expected AllUpstreamsComplete"),
        }
    }
    
    #[tokio::test]
    async fn test_eof_mixed_with_events() {
        let store = EventStore::for_testing().await;
        
        // Create writers and subscription
        let stage_1 = StageId::from_u32(1);
        let stage_2 = StageId::from_u32(2);
        let mut writer1 = store.create_writer(stage_1, StageSemantics::Stateless).await.unwrap();
        let mut writer2 = store.create_writer(stage_2, StageSemantics::Stateless).await.unwrap();
        
        let filter = SubscriptionFilter {
            upstream_stages: vec![stage_1, stage_2],
        };
        let mut subscription = store.subscribe(filter).await.unwrap();
        
        // Write some events
        let event1 = ChainEvent::new("test1", serde_json::json!({"data": 1}));
        writer1.append(event1, None).await.unwrap();
        
        // Should receive events
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::Events(events) => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].event.event_type, "test1");
            }
            _ => panic!("Expected Events"),
        }
        
        // Signal stage 1 completion
        store.signal_stage_complete(stage_1, true).await.unwrap();
        
        // Should receive EOF for stage 1
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::EndOfStream { stage_id, .. } => {
                assert_eq!(stage_id, stage_1);
            }
            _ => panic!("Expected EndOfStream"),
        }
        
        // Write more events from stage 2
        let event2 = ChainEvent::new("test2", serde_json::json!({"data": 2}));
        writer2.append(event2, None).await.unwrap();
        
        // Should still receive events from stage 2
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::Events(events) => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].event.event_type, "test2");
            }
            _ => panic!("Expected Events from stage 2"),
        }
        
        // Signal stage 2 completion
        store.signal_stage_complete(stage_2, false).await.unwrap();
        
        // Should receive EOF for stage 2
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::EndOfStream { stage_id, .. } => {
                assert_eq!(stage_id, stage_2);
            }
            _ => panic!("Expected EndOfStream for stage 2"),
        }
        
        // All complete now
        match subscription.recv_with_eof().await.unwrap() {
            SubscriptionEvent::AllUpstreamsComplete => {},
            _ => panic!("Expected AllUpstreamsComplete"),
        }
    }
}