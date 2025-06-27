//! Runtime context for sink handlers
//!
//! Contains the handler instance and all runtime resources needed
//! by a sink stage during execution.

use std::sync::Arc;
use tokio::sync::RwLock;
use crate::control_plane::stages::handler_traits::SinkHandler;
use crate::data_plane::journal_subscription::{ReactiveJournal, JournalSubscription};
use crate::message_bus::FsmMessageBus;
use obzenflow_topology_services::stages::StageId;

/// Context for sink handlers
pub struct SinkContext<H: SinkHandler> {
    /// The handler instance that implements sink logic
    pub handler: Arc<RwLock<H>>,  // RwLock - sinks are stateful
    
    /// This sink's stage ID
    pub stage_id: StageId,
    
    /// Human-readable stage name for logging
    pub stage_name: String,
    
    /// Journal for reading events
    pub journal: Arc<ReactiveJournal>,
    
    /// Message bus for pipeline communication
    pub bus: Arc<FsmMessageBus>,
    
    /// Subscription to upstream events
    pub subscription: Arc<RwLock<Option<JournalSubscription>>>,
    
    /// Track if we're currently flushing
    pub is_flushing: Arc<RwLock<bool>>,
}

impl<H: SinkHandler> SinkContext<H> {
    pub fn new(
        handler: H,
        stage_id: StageId,
        stage_name: String,
        journal: Arc<ReactiveJournal>,
        bus: Arc<FsmMessageBus>,
    ) -> Self {
        Self {
            handler: Arc::new(RwLock::new(handler)),
            stage_id,
            stage_name,
            journal,
            bus,
            subscription: Arc::new(RwLock::new(None)),
            is_flushing: Arc::new(RwLock::new(false)),
        }
    }
}