//! Journal subscription layer for reactive event processing
//!
//! This module adds pub/sub capabilities on top of the core Journal trait,
//! enabling reactive event processing with subscriptions, notifications, and EOF handling.

use obzenflow_core::Result;
use obzenflow_topology_services::stages::StageId;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_core::JournalError;
use obzenflow_core::ChainEvent;
use obzenflow_core::EventEnvelope;
use obzenflow_core::EventId;
// MetricsObserver removed - using subscription pattern instead per FLOWIP-056-666
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;
use ulid::Ulid;
use async_trait::async_trait;

/// Constants for subscription management
const SUBSCRIPTION_CHANNEL_SIZE: usize = 1000;
const PENDING_BUFFER_CAPACITY: usize = 100;

/// Writer information for tracking active writers
#[derive(Debug, Clone)]
pub struct WriterInfo {
    pub writer_id: WriterId,
    pub stage_id: StageId,
    pub worker_index: Option<u32>,
    pub created_at: Instant,
}

/// Registry for tracking active writers and their stage associations
#[derive(Debug)]
pub struct WriterRegistry {
    writers: HashMap<WriterId, WriterInfo>,
    stage_writers: HashMap<StageId, Vec<WriterId>>,
}

impl WriterRegistry {
    fn new() -> Self {
        Self {
            writers: HashMap::new(),
            stage_writers: HashMap::new(),
        }
    }
}

/// Notification sent to subscribers when new events are available
#[derive(Debug, Clone)]
pub struct EventNotification {
    pub event_id: EventId,
    pub stage_id: StageId,
}


/// Subscription mode - explicit types for different subscription patterns
#[derive(Debug, Clone)]
pub enum SubscriptionFilter {
    /// Subscribe to all events - no filtering
    All,
    
    /// Subscribe to all events from specific upstream stages (data flow)
    UpstreamStages {
        stages: Vec<StageId>,
    },
    
    /// Subscribe to specific event types from all stages (control flow)
    EventTypes {
        event_types: Vec<String>,
    },
    
    /// Subscribe to specific event types from specific stages
    StagesAndEventTypes {
        stages: Vec<StageId>,
        event_types: Vec<String>,
    },
}

/// Internal subscription state
struct Subscription {
    id: Ulid,
    filter: SubscriptionFilter,
    /// Channel for event notifications
    sender: mpsc::Sender<EventNotification>,
    created_at: Instant,
}

/// Manages active subscriptions
struct SubscriptionManager {
    /// Active subscriptions by ID
    subscriptions: HashMap<Ulid, Subscription>,
    /// Stage → Subscription mappings for efficient routing
    stage_subscriptions: HashMap<StageId, Vec<Ulid>>,
}

impl SubscriptionManager {
    fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            stage_subscriptions: HashMap::new(),
        }
    }
}

/// A handle to an active subscription
pub struct JournalSubscription {
    pub id: Ulid,
    receiver: mpsc::Receiver<EventNotification>,
    pending_buffer: VecDeque<EventEnvelope>,
    filter: SubscriptionFilter,
    completed_upstreams: HashSet<StageId>,
    journal: Arc<dyn Journal>,
}

impl JournalSubscription {
    /// Receive next batch of events
    /// Returns empty vec when no events are immediately available
    pub async fn recv_batch(&mut self) -> Result<Vec<EventEnvelope>> {
        // Check for pending events first
        if !self.pending_buffer.is_empty() {
            let events: Vec<EventEnvelope> = self.pending_buffer.drain(..).collect();
            return Ok(events);
        }

        // Wait for notifications
        match self.receiver.recv().await {
            Some(notif) => {
                // Read initial event
                let event = self.journal.read_event(&notif.event_id).await?;
                let mut events = if let Some(event) = event {
                    // Track EOF events internally
                    if event.event.is_eof() {
                        // Extract stage_id from writer registry would be needed here
                        // For now, we'll let the stage handle tracking its upstreams
                        tracing::debug!("Subscription {} received EOF event", self.id);
                    }
                    vec![event]
                } else {
                    vec![]
                };
                
                // Try to batch more events if available (up to max_batch_size)
                const MAX_BATCH_SIZE: usize = 100;
                while events.len() < MAX_BATCH_SIZE {
                    // Try to receive more notifications without blocking
                    match self.receiver.try_recv() {
                        Ok(next_notif) => {
                            // Read the next event
                            if let Some(event) = self.journal.read_event(&next_notif.event_id).await? {
                                if event.event.is_eof() {
                                    tracing::debug!("Subscription {} received EOF event in batch", self.id);
                                }
                                events.push(event);
                            }
                        }
                        Err(_) => break, // No more notifications available
                    }
                }
                // Events are already returned in the order we read them
                // The journal handles causal ordering internally
                
                Ok(events)
            }
            None => {
                // Channel closed, return empty vec
                Ok(vec![])
            }
        }
    }
}


/// Journal wrapper that adds pub/sub capabilities
pub struct ReactiveJournal {
    /// The underlying journal for storage
    journal: Arc<dyn Journal>,
    /// Writer registry for tracking active writers
    writer_registry: Arc<RwLock<WriterRegistry>>,
    /// Subscription management
    subscriptions: Arc<RwLock<SubscriptionManager>>,
}

impl ReactiveJournal {
    /// Create a new reactive journal wrapping the given journal
    pub fn new(journal: Arc<dyn Journal>) -> Self {
        Self {
            journal,
            writer_registry: Arc::new(RwLock::new(WriterRegistry::new())),
            subscriptions: Arc::new(RwLock::new(SubscriptionManager::new())),
        }
    }
    
    /// Get writer info for a writer ID
    pub async fn get_writer_info(&self, writer_id: &WriterId) -> Option<WriterInfo> {
        let registry = self.writer_registry.read().await;
        registry.writers.get(writer_id).cloned()
    }

    /// Register a writer for a stage
    pub async fn register_writer(
        &self,
        stage_id: StageId,
        worker_index: Option<u32>,
    ) -> Result<WriterId> {
        let writer_id = WriterId::new();

        let mut registry = self.writer_registry.write().await;
        
        registry.writers.insert(writer_id.clone(), WriterInfo {
            writer_id: writer_id.clone(),
            stage_id,
            worker_index,
            created_at: Instant::now(),
        });

        registry.stage_writers
            .entry(stage_id)
            .or_insert_with(Vec::new)
            .push(writer_id.clone());

        Ok(writer_id)
    }

    /// Write an event and notify subscribers
    pub async fn write(
        &self,
        writer_id: &WriterId,
        event: ChainEvent,
        parent: Option<&EventEnvelope>,
    ) -> Result<EventEnvelope> {
        // Write to underlying journal
        let envelope = self.journal.append(writer_id, event, parent).await?;

        // Notify subscribers (metrics are now handled via subscriptions per FLOWIP-056-666)
        self.notify_subscribers(&envelope).await;

        Ok(envelope)
    }

    /// Create a subscription
    pub async fn subscribe(&self, filter: SubscriptionFilter) -> Result<JournalSubscription> {
        let (tx, rx) = mpsc::channel(SUBSCRIPTION_CHANNEL_SIZE);
        let id = Ulid::new();

        let sub = Subscription {
            id,
            filter: filter.clone(),
            sender: tx,
            created_at: Instant::now(),
        };

        let mut manager = self.subscriptions.write().await;

        // Add subscription
        manager.subscriptions.insert(id, sub);

        // Log subscription creation
        match &filter {
            SubscriptionFilter::All => {
                tracing::info!("Created subscription {} for ALL events", id);
            },
            SubscriptionFilter::EventTypes { event_types } => {
                tracing::info!(
                    "Created EventTypes subscription {} for event types: {:?}", 
                    id, event_types
                );
            },
            SubscriptionFilter::UpstreamStages { stages } => {
                tracing::debug!(
                    "Created UpstreamStages subscription {} for stages: {:?}", 
                    id, stages
                );
            },
            SubscriptionFilter::StagesAndEventTypes { stages, event_types } => {
                tracing::debug!(
                    "Created StagesAndEventTypes subscription {} for stages: {:?}, events: {:?}", 
                    id, stages, event_types
                );
            },
        }

        // Register stage mappings for O(1) routing (only for stage-based subscriptions)
        match &filter {
            SubscriptionFilter::UpstreamStages { stages } => {
                for &stage in stages {
                    manager.stage_subscriptions
                        .entry(stage)
                        .or_default()
                        .push(id);
                }
            },
            SubscriptionFilter::StagesAndEventTypes { stages, .. } => {
                for &stage in stages {
                    manager.stage_subscriptions
                        .entry(stage)
                        .or_default()
                        .push(id);
                }
            },
            SubscriptionFilter::EventTypes { .. } => {
                // No stage mapping needed - these subscriptions check all events
            }
            SubscriptionFilter::All => {
                // No stage mapping needed - these subscriptions see ALL events
            }
        }

        Ok(JournalSubscription {
            id,
            receiver: rx,
            pending_buffer: VecDeque::with_capacity(PENDING_BUFFER_CAPACITY),
            filter,
            completed_upstreams: HashSet::new(),
            journal: self.journal.clone(),
        })
    }

    /// Notify subscribers of new events
    async fn notify_subscribers(&self, envelope: &EventEnvelope) {
        let manager = self.subscriptions.read().await;
        
        // Get stage_id from writer registry
        let registry = self.writer_registry.read().await;
        let stage_id = match registry.writers.get(&envelope.writer_id) {
            Some(info) => info.stage_id,
            None => {
                tracing::warn!("Unknown writer_id: {:?}, skipping notification", envelope.writer_id);
                return;
            }
        };

        // Debug logging for system events
        if envelope.event.event_type.starts_with("system.") {
            tracing::debug!(
                "Processing system event: type={}, stage_id={:?}, writer_id={:?}, total_subscriptions={}", 
                envelope.event.event_type, stage_id, envelope.writer_id, manager.subscriptions.len()
            );
        }

        // Check each subscription to see if it should be notified
        if envelope.event.event_type.starts_with("system.") {
            tracing::debug!("Checking {} subscriptions for system event", manager.subscriptions.len());
            for (sub_id, sub) in &manager.subscriptions {
                tracing::debug!("  Subscription {}: filter={:?}", sub_id, sub.filter);
            }
        }
        
        for (sub_id, sub) in &manager.subscriptions {
            let should_notify = match &sub.filter {
                SubscriptionFilter::All => {
                    // Always notify for All filter
                    true
                },
                SubscriptionFilter::UpstreamStages { stages } => {
                    // Data flow: notify if event is from an upstream stage
                    stages.contains(&stage_id)
                },
                SubscriptionFilter::EventTypes { event_types } => {
                    // Control flow: notify if event type matches (from any stage)
                    let matches = event_types.contains(&envelope.event.event_type);
                    if envelope.event.event_type.starts_with("system.") {
                        tracing::debug!(
                            "EventTypes subscription {} checking event type '{}' against filter {:?}: matches={}", 
                            sub_id, envelope.event.event_type, event_types, matches
                        );
                    }
                    matches
                },
                SubscriptionFilter::StagesAndEventTypes { stages, event_types } => {
                    // Combined: notify if both stage and event type match
                    stages.contains(&stage_id) && event_types.contains(&envelope.event.event_type)
                },
            };

            if should_notify {
                let notification = EventNotification {
                    event_id: envelope.event.id,
                    stage_id,
                };

                // Try to send notification (non-blocking)
                match sub.sender.try_send(notification) {
                    Ok(_) => {
                        if envelope.event.event_type.starts_with("system.") {
                            tracing::debug!(
                                "Sent system event notification to subscription {}", 
                                sub_id
                            );
                        }
                    },
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        tracing::warn!("Subscription {} channel full", sub_id);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        tracing::debug!("Subscription {} closed", sub_id);
                    }
                }
            }
        }
    }


    /// Clean up dead subscriptions
    pub async fn cleanup_subscriptions(&self) {
        let mut manager = self.subscriptions.write().await;
        
        // Remove closed subscriptions
        manager.subscriptions.retain(|_id, sub| {
            !sub.sender.is_closed()
        });

        // Clean up stage mappings
        let valid_ids: HashSet<Ulid> = manager.subscriptions.keys().cloned().collect();
        manager.stage_subscriptions.retain(|_stage, subs| {
            subs.retain(|id| valid_ids.contains(id));
            !subs.is_empty()
        });
    }
}

/// Implement Journal trait for ReactiveJournal to make it a drop-in replacement
#[async_trait]
impl Journal for ReactiveJournal {
    async fn append(
        &self,
        writer_id: &WriterId,
        event: ChainEvent,
        parent: Option<&EventEnvelope>
    ) -> std::result::Result<EventEnvelope, JournalError> {
        // Write to underlying journal
        let envelope = self.journal.append(writer_id, event, parent).await?;
        
        // Notify subscribers (ignore notification errors)
        self.notify_subscribers(&envelope).await;
        
        Ok(envelope)
    }

    async fn read_causally_ordered(&self) -> std::result::Result<Vec<EventEnvelope>, JournalError> {
        // Direct delegation
        self.journal.read_causally_ordered().await
    }

    async fn read_causally_after(&self, after_event_id: &EventId) -> std::result::Result<Vec<EventEnvelope>, JournalError> {
        // Direct delegation
        self.journal.read_causally_after(after_event_id).await
    }

    async fn read_event(&self, event_id: &EventId) -> std::result::Result<Option<EventEnvelope>, JournalError> {
        // Direct delegation
        self.journal.read_event(event_id).await
    }
}

/// A writer handle that automatically notifies on writes
pub struct ReactiveWriter {
    writer_id: WriterId,
    journal: Arc<ReactiveJournal>,
}

impl ReactiveWriter {
    pub fn new(writer_id: WriterId, journal: Arc<ReactiveJournal>) -> Self {
        Self { writer_id, journal }
    }

    pub async fn write(
        &mut self,
        event: ChainEvent,
        parent: Option<&EventEnvelope>,
    ) -> Result<EventEnvelope> {
        self.journal.write(&self.writer_id, event, parent).await
    }
}

// Include tests
#[cfg(test)]
#[path = "reactive_journal_tests.rs"]
mod reactive_journal_tests;