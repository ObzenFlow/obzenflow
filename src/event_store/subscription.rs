//! Push-based event subscription system for FLOWIP-011

use crate::step::Result;
use crate::event_store::{EventReader, EventEnvelope};
use crate::event_store::constants::*;
use crate::topology::StageId;
use ulid::Ulid;
use std::collections::VecDeque;
use tokio::sync::mpsc;

/// Filter for subscriptions - extensible for future patterns
#[derive(Clone, Debug)]
pub struct SubscriptionFilter {
    /// Which stages to receive events from
    pub upstream_stages: Vec<StageId>,
    // Future: merge_strategy, routing_key, group_id
}

/// Push-based event subscription with causal ordering
pub struct EventSubscription {
    pub(crate) id: Ulid,
    pub(crate) receiver: mpsc::Receiver<EventNotification>,
    /// Buffer for maintaining causal order
    pub(crate) pending_buffer: VecDeque<EventEnvelope>,
    /// Reader for fetching full events
    pub(crate) reader: EventReader,
}

/// Lightweight notification (not the full event)
#[derive(Debug, Clone)]
pub struct EventNotification {
    pub event_id: Ulid,
    pub stage_id: StageId,
    pub sequence: u64,
}

impl EventSubscription {
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