//! Journal subscription layer for reactive event processing
//!
//! This module adds pub/sub capabilities on top of the core Journal trait,
//! enabling reactive event processing with subscriptions, notifications, and EOF handling.

use obzenflow_core::Result;
use obzenflow_topology_services::stages::StageId;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_core::ChainEvent;
use obzenflow_core::EventEnvelope;
use obzenflow_core::EventId;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;
use ulid::Ulid;

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

/// EOF notification sent when a stage completes
#[derive(Debug, Clone)]
pub struct EofNotification {
    pub stage_id: StageId,
    pub final_sequence: u64,
    pub natural_completion: bool,
}

/// Filter for subscriptions
#[derive(Debug, Clone)]
pub struct SubscriptionFilter {
    /// Which upstream stages to subscribe to
    pub upstream_stages: Vec<StageId>,
    /// Optional event type filters
    pub event_types: Option<Vec<String>>,
}

/// Internal subscription state
struct Subscription {
    id: Ulid,
    filter: SubscriptionFilter,
    /// Channel for event notifications
    sender: mpsc::Sender<EventNotification>,
    /// Channel for EOF notifications
    eof_sender: mpsc::Sender<EofNotification>,
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
    eof_receiver: mpsc::Receiver<EofNotification>,
    pending_buffer: VecDeque<EventEnvelope>,
    filter: SubscriptionFilter,
    completed_upstreams: HashSet<StageId>,
    journal: Arc<dyn Journal>,
}

impl JournalSubscription {
    /// Receive next batch of events (with EOF handling)
    pub async fn recv_batch(&mut self) -> Result<SubscriptionEvent> {
        // Check for pending events first
        if !self.pending_buffer.is_empty() {
            let events: Vec<EventEnvelope> = self.pending_buffer.drain(..).collect();
            return Ok(SubscriptionEvent::Events(events));
        }

        // Wait for notifications
        tokio::select! {
            notification = self.receiver.recv() => {
                match notification {
                    Some(notif) => {
                        // Read initial event
                        // Read specific event
                        let event = self.journal.read_event(&notif.event_id).await?;
                        let mut events = if let Some(event) = event {
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
                                        events.push(event);
                                    }
                                }
                                Err(_) => break, // No more notifications available
                            }
                        }
                        // Events are already returned in the order we read them
                        // The journal handles causal ordering internally
                        
                        Ok(SubscriptionEvent::Events(events))
                    }
                    None => Ok(SubscriptionEvent::AllUpstreamsComplete)
                }
            }
            
            eof = self.eof_receiver.recv() => {
                match eof {
                    Some(eof_notif) => {
                        self.completed_upstreams.insert(eof_notif.stage_id);
                        
                        // Check if all upstreams are complete
                        let all_complete = self.filter.upstream_stages.iter()
                            .all(|stage| self.completed_upstreams.contains(stage));
                            
                        if all_complete {
                            Ok(SubscriptionEvent::AllUpstreamsComplete)
                        } else {
                            Ok(SubscriptionEvent::EndOfStream {
                                stage_id: eof_notif.stage_id,
                                final_sequence: eof_notif.final_sequence,
                                natural_completion: eof_notif.natural_completion,
                            })
                        }
                    }
                    None => Ok(SubscriptionEvent::AllUpstreamsComplete)
                }
            }
        }
    }
}

/// Events returned by subscriptions
#[derive(Debug)]
pub enum SubscriptionEvent {
    /// Regular events
    Events(Vec<EventEnvelope>),
    /// A specific upstream has completed
    EndOfStream {
        stage_id: StageId,
        final_sequence: u64,
        natural_completion: bool,
    },
    /// All upstreams have completed
    AllUpstreamsComplete,
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

        // Notify subscribers
        self.notify_subscribers(&envelope).await;

        Ok(envelope)
    }

    /// Create a subscription
    pub async fn subscribe(&self, filter: SubscriptionFilter) -> Result<JournalSubscription> {
        let (tx, rx) = mpsc::channel(SUBSCRIPTION_CHANNEL_SIZE);
        let (eof_tx, eof_rx) = mpsc::channel(SUBSCRIPTION_CHANNEL_SIZE);
        let id = Ulid::new();

        let sub = Subscription {
            id,
            filter: filter.clone(),
            sender: tx,
            eof_sender: eof_tx,
            created_at: Instant::now(),
        };

        let mut manager = self.subscriptions.write().await;

        // Add subscription
        manager.subscriptions.insert(id, sub);

        // Register stage mappings for O(1) routing
        for &stage in &filter.upstream_stages {
            manager.stage_subscriptions
                .entry(stage)
                .or_default()
                .push(id);
        }

        Ok(JournalSubscription {
            id,
            receiver: rx,
            eof_receiver: eof_rx,
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
            None => return, // Unknown writer, skip notification
        };

        if let Some(sub_ids) = manager.stage_subscriptions.get(&stage_id) {
            for sub_id in sub_ids {
                if let Some(sub) = manager.subscriptions.get(sub_id) {
                    let notification = EventNotification {
                        event_id: envelope.event.id,
                        stage_id,
                    };

                    // Try to send notification (non-blocking)
                    match sub.sender.try_send(notification) {
                        Ok(_) => {},
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
    }

    /// Signal that a stage has completed
    pub async fn signal_stage_complete(&self, stage_id: StageId, natural: bool) -> Result<()> {
        let manager = self.subscriptions.read().await;

        if let Some(sub_ids) = manager.stage_subscriptions.get(&stage_id) {
            tracing::info!(
                "Notifying {} subscribers that stage {:?} has completed (natural: {})",
                sub_ids.len(), stage_id, natural
            );

            for sub_id in sub_ids {
                if let Some(sub) = manager.subscriptions.get(sub_id) {
                    let eof_notification = EofNotification {
                        stage_id,
                        final_sequence: 0, // TODO: Track sequences
                        natural_completion: natural,
                    };

                    // Send EOF notification
                    let _ = sub.eof_sender.try_send(eof_notification);
                }
            }
        }

        Ok(())
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