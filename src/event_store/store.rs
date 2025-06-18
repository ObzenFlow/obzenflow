//! Core EventStore implementation

use crate::step::Result;
use crate::event_store::constants::*;
use crate::event_store::{EventEnvelope, EventWriter, EventReader, EventSubscription, SubscriptionFilter, EventNotification, WriterId};
use crate::event_store::flow_log::FlowEventLog;
use crate::event_types::new_flow_id;
use crate::topology::StageId;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
use ulid::Ulid;
use serde_json::json;

/// Writer registry for tracking active writers
/// RwLock is appropriate here because:
/// - Writes are rare (only during scaling events)
/// - Reads are frequent (every read operation)
/// - Dynamic scaling requires runtime mutation
#[derive(Debug)]
pub(crate) struct WriterRegistry {
    pub writers: HashMap<WriterId, WriterInfo>,
    pub stage_workers: HashMap<StageId, Vec<WriterId>>,
}

#[derive(Debug, Clone)]
pub(crate) struct WriterInfo {
    pub writer_id: WriterId,
    pub stage_id: StageId,
    pub worker_index: Option<u32>,
    pub created_at: Instant,
    pub semantics: StageSemantics,
}

/// Event store statistics for shutdown summary
#[derive(Debug, Clone)]
pub struct EventStoreStatistics {
    pub segment_count: usize,
    pub total_size_bytes: u64,
    pub event_count: u64,
    pub path: PathBuf,
}

/// Subscription management
pub(crate) struct SubscriptionManager {
    /// Active subscriptions by ID
    subscriptions: HashMap<Ulid, Subscription>,
    /// Stage → Subscription mappings for efficient routing
    stage_subscriptions: HashMap<StageId, Vec<Ulid>>,
}

pub(crate) struct Subscription {
    id: Ulid,
    filter: SubscriptionFilter,
    /// Bounded channel to prevent memory issues
    sender: mpsc::Sender<EventNotification>,
    created_at: Instant,
}

impl SubscriptionManager {
    fn new() -> Self {
        Self {
            subscriptions: HashMap::with_capacity(PENDING_BUFFER_CAPACITY),
            stage_subscriptions: HashMap::with_capacity(PENDING_BUFFER_CAPACITY),
        }
    }
}

/// Main EventStore abstraction
/// 
/// Provides infrastructure transparency while supporting:
/// - Vector clock causal consistency
/// - Writer isolation (no locks needed)
/// - Dynamic worker scaling
/// - Stage semantics awareness
pub struct EventStore {
    pub(crate) store_path: PathBuf,
    pub(crate) isolation_mode: IsolationMode,
    pub(crate) retention_policy: RetentionPolicy,
    /// Registry uses RwLock for read-heavy, write-rare access pattern
    pub(crate) writer_registry: Arc<RwLock<WriterRegistry>>,
    /// Subscription management with async RwLock
    pub(crate) subscriptions: Arc<RwLock<SubscriptionManager>>,
    /// Single log per flow execution
    pub(crate) flow_log: Arc<FlowEventLog>,
}

impl EventStore {
    /// Create a new EventStore with configuration
    pub async fn new(config: EventStoreConfig) -> Result<Arc<Self>> {
        // Ensure store directory exists
        std::fs::create_dir_all(&config.path)?;
        
        // Create flow log with unique ID
        let flow_id = Ulid::new().to_string();
        let flow_log = Arc::new(FlowEventLog::new(config.path.clone(), &flow_id).await?);
        
        let store = Arc::new(Self {
            store_path: config.path,
            isolation_mode: IsolationMode::Shared,
            retention_policy: RetentionPolicy::default(),
            writer_registry: Arc::new(RwLock::new(WriterRegistry {
                writers: HashMap::new(),
                stage_workers: HashMap::new(),
            })),
            subscriptions: Arc::new(RwLock::new(SubscriptionManager::new())),
            flow_log,
        });
        
        // Spawn cleanup task
        store.clone().spawn_cleanup_task();
        
        Ok(store)
    }
    
    /// Create with default configuration
    pub async fn default() -> Arc<Self> {
        Self::new(EventStoreConfig::default()).await.expect("Failed to create default EventStore")
    }
    
    /// Create EventStore for testing - uses target/test-flows/ directory
    pub async fn for_testing() -> Arc<Self> {
        let test_id = ulid::Ulid::new().to_string();
        let store_path = PathBuf::from("target/test-flows")
            .join(&test_id)
            .join("event_store");
        
        // Ensure store directory exists
        std::fs::create_dir_all(&store_path).expect("Failed to create test event store directory");
        
        // Create flow log
        let flow_id = test_id.clone();
        let flow_log = Arc::new(FlowEventLog::new(store_path.clone(), &flow_id).await.expect("Failed to create test flow log"));
        
        println!("📁 Test event log: {}", store_path.display());
        
        let store = Arc::new(Self {
            store_path: store_path.clone(),
            isolation_mode: IsolationMode::Isolated,
            retention_policy: RetentionPolicy::default(), // No auto-cleanup!
            writer_registry: Arc::new(RwLock::new(WriterRegistry {
                writers: HashMap::new(),
                stage_workers: HashMap::new(),
            })),
            subscriptions: Arc::new(RwLock::new(SubscriptionManager::new())),
            flow_log,
        });
        
        // Spawn cleanup task
        store.clone().spawn_cleanup_task();
        
        store
    }
    
    /// Create EventStore for a named flow
    /// Automatically creates event log in target/flows/{flow_name}_{ulid}/event_store/
    pub async fn for_flow(flow_name: &str) -> Result<Arc<Self>> {
        let flow_id = new_flow_id(flow_name);
        let flow_dir = PathBuf::from("target/flows").join(&flow_id);
        let store_path = flow_dir.join("event_store");
        
        // Create event store directory
        std::fs::create_dir_all(&store_path)?;
        
        // Save event store metadata
        let metadata = json!({
            "flow_id": flow_id,
            "flow_name": flow_name,
            "started_at": chrono::Utc::now().to_rfc3339(),
            "event_store_version": "1.0"
        });
        std::fs::write(
            store_path.join("metadata.json"), 
            serde_json::to_string_pretty(&metadata)?
        )?;
        
        println!("📁 Event log: {}", store_path.display());
        
        // Create EventStore - it will NEVER auto-delete
        Self::new(EventStoreConfig {
            path: store_path,
            max_segment_size: 10 * 1024 * 1024, // 10MB segments
        }).await
    }
    
    /// Create a writer for a single-worker stage
    pub async fn create_writer(
        self: &Arc<Self>,
        stage_id: StageId,
        semantics: StageSemantics
    ) -> Result<EventWriter> {
        self.create_worker_writer(stage_id, None, semantics).await
    }
    
    /// Create a writer for a multi-worker stage
    pub async fn create_worker_writer(
        self: &Arc<Self>,
        stage_id: StageId,
        worker_index: Option<u32>,
        semantics: StageSemantics,
    ) -> Result<EventWriter> {
        let writer_id = match worker_index {
            Some(idx) => WriterId::with_worker(stage_id, idx),
            None => WriterId::new(stage_id),
        };
        
        // Register writer
        let mut registry = self.writer_registry.write().await;
        registry.writers.insert(writer_id.clone(), WriterInfo {
            writer_id: writer_id.clone(),
            stage_id,
            worker_index,
            created_at: Instant::now(),
            semantics,
        });
        
        // Track stage workers
        registry.stage_workers
            .entry(stage_id)
            .or_insert_with(Vec::new)
            .push(writer_id.clone());
        
        Ok(EventWriter::new(writer_id, self.flow_log.clone(), Arc::downgrade(self)))
    }
    
    /// Create a reader
    pub fn reader(&self) -> EventReader {
        EventReader::new(self.flow_log.clone())
    }
    
    /// Get the event store path
    pub fn path(&self) -> &PathBuf {
        &self.store_path
    }
    
    /// Get event store statistics for shutdown summary
    pub async fn get_statistics(&self) -> EventStoreStatistics {
        // Get segment files
        let mut segment_count = 0;
        let mut total_size = 0u64;
        
        if let Ok(entries) = std::fs::read_dir(&self.store_path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() && entry.path().extension().map_or(false, |ext| ext == "log") {
                        segment_count += 1;
                        total_size += metadata.len();
                    }
                }
            }
        }
        
        // Get event count from flow log
        let event_count = self.flow_log.total_events().await;
        
        EventStoreStatistics {
            segment_count,
            total_size_bytes: total_size,
            event_count,
            path: self.store_path.clone(),
        }
    }
    
    /// Get current writer count for a stage
    pub async fn stage_worker_count(&self, stage_id: StageId) -> usize {
        let registry = self.writer_registry.read().await;
        registry.stage_workers
            .get(&stage_id)
            .map(|workers| workers.len())
            .unwrap_or(0)
    }
    
    /// Scale up a stage by adding a worker
    /// This demonstrates why WriterRegistry needs to be mutable
    pub async fn scale_up_stage(self: &Arc<Self>, stage_id: StageId, semantics: StageSemantics) -> Result<EventWriter> {
        let current_count = self.stage_worker_count(stage_id).await;
        self.create_worker_writer(stage_id, Some(current_count as u32), semantics).await
    }
    
    /// Create a subscription for push-based delivery
    pub async fn subscribe(&self, filter: SubscriptionFilter) -> Result<EventSubscription> {
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
        
        // Register stage mappings for O(1) routing
        for &stage in &filter.upstream_stages {
            manager.stage_subscriptions
                .entry(stage)
                .or_default()
                .push(id);
        }
        
        Ok(EventSubscription {
            id,
            receiver: rx,
            pending_buffer: VecDeque::with_capacity(PENDING_BUFFER_CAPACITY),
            reader: self.reader(),
        })
    }
    
    
    /// Notify subscribers - async all the way down
    pub(crate) async fn notify_subscribers(&self, envelope: &EventEnvelope) {
        // Read lock - multiple notifiers can run concurrently
        let manager = self.subscriptions.read().await;
        
        // Get stage_id from writer_id
        let stage_id = envelope.writer_id.stage_id();
        
        if let Some(sub_ids) = manager.stage_subscriptions.get(&stage_id) {
            tracing::debug!("Notifying {} subscribers for stage {:?}", sub_ids.len(), stage_id);
            for sub_id in sub_ids {
                if let Some(sub) = manager.subscriptions.get(sub_id) {
                    let notification = EventNotification {
                        event_id: envelope.event.ulid,
                        stage_id,
                    };
                    
                    // Try to send notification
                    // If channel is full, it means the subscriber is too slow
                    match sub.sender.try_send(notification) {
                        Ok(_) => {
                            tracing::trace!("Sent notification for event {} to subscription {}", envelope.event.ulid, sub_id);
                        },
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            tracing::error!(
                                "Subscription {} channel full - subscriber too slow! Dropping event {}",
                                sub_id, envelope.event.ulid
                            );
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            tracing::debug!("Subscription {} closed", sub_id);
                        }
                    }
                }
            }
        }
    }
    
    /// Clean up dead subscriptions periodically
    async fn cleanup_subscriptions(&self) {
        let mut manager = self.subscriptions.write().await;
        let mut removed_count = 0;
        
        // Remove closed subscriptions
        manager.subscriptions.retain(|_id, sub| {
            let keep = !sub.sender.is_closed();
            if !keep {
                removed_count += 1;
            }
            keep
        });
        
        // Clean up stage mappings
        let valid_ids: std::collections::HashSet<Ulid> = manager.subscriptions.keys().cloned().collect();
        manager.stage_subscriptions.retain(|_stage, subs| {
            subs.retain(|id| valid_ids.contains(id));
            !subs.is_empty()
        });
        
        if removed_count > 0 {
            tracing::debug!("Cleaned up {} dead subscriptions", removed_count);
        }
    }
    
    /// Spawn background cleanup task
    fn spawn_cleanup_task(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SUBSCRIPTION_CLEANUP_INTERVAL);
            loop {
                interval.tick().await;
                self.cleanup_subscriptions().await;
                
                // Check if we should shutdown
                if let IsolationMode::Isolated = self.isolation_mode {
                    // TODO: Add proper shutdown signal
                }
            }
        });
    }
}

// Re-export types used by EventStore
pub use crate::event_store::{EventStoreConfig, IsolationMode, RetentionPolicy, StageSemantics};