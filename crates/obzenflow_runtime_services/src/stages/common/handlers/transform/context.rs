//! Runtime context for transform handlers
//!
//! Contains the handler instance and all runtime resources needed
//! by a transform stage during execution.

use std::sync::Arc;
use tokio::sync::RwLock;
use super::traits::TransformHandler;
use crate::event_flow::reactive_journal::{ReactiveJournal, JournalSubscription};
use crate::message_bus::FsmMessageBus;
use obzenflow_topology_services::stages::StageId;
use obzenflow_core::journal::writer_id::WriterId;

/// Context for transform handlers
pub struct TransformContext<H: TransformHandler> {
    /// The handler instance that implements transform logic
    pub handler: Arc<H>,  // Not RwLock - transforms are stateless
    
    /// This transform's stage ID
    pub stage_id: StageId,
    
    /// Human-readable stage name for logging
    pub stage_name: String,
    
    /// Journal for reading/writing events
    pub journal: Arc<ReactiveJournal>,
    
    /// Message bus for pipeline communication
    pub bus: Arc<FsmMessageBus>,
    
    /// Writer ID for this transform (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,
    
    /// Subscription to upstream events
    pub subscription: Arc<RwLock<Option<JournalSubscription>>>,
    
    /// Upstream stage IDs
    pub upstream_stages: Vec<StageId>,
}

impl<H: TransformHandler> TransformContext<H> {
    pub fn new(
        handler: H,
        stage_id: StageId,
        stage_name: String,
        journal: Arc<ReactiveJournal>,
        bus: Arc<FsmMessageBus>,
        upstream_stages: Vec<StageId>,
    ) -> Self {
        Self {
            handler: Arc::new(handler),
            stage_id,
            stage_name,
            journal,
            bus,
            writer_id: Arc::new(RwLock::new(None)),
            subscription: Arc::new(RwLock::new(None)),
            upstream_stages,
        }
    }
}