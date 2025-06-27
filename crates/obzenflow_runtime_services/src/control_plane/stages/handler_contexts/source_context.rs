//! Runtime context for source handlers
//!
//! Contains the handler instance and all runtime resources needed
//! by a source stage during execution.

use std::sync::Arc;
use tokio::sync::RwLock;
use crate::control_plane::stages::handler_traits::{FiniteSourceHandler, InfiniteSourceHandler};
use crate::data_plane::journal_subscription::ReactiveJournal;
use crate::message_bus::FsmMessageBus;
use obzenflow_topology_services::stages::StageId;
use obzenflow_core::journal::writer_id::WriterId;

/// Context for finite source handlers
pub struct FiniteSourceContext<H: FiniteSourceHandler> {
    /// The handler instance that implements source logic
    pub handler: Arc<RwLock<H>>,
    
    /// This source's stage ID
    pub stage_id: StageId,
    
    /// Human-readable stage name for logging
    pub stage_name: String,
    
    /// Journal for writing events
    pub journal: Arc<ReactiveJournal>,
    
    /// Message bus for pipeline communication
    pub bus: Arc<FsmMessageBus>,
    
    /// Writer ID for this source (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,
    
    /// Flag indicating if source can emit (set to true after Start event)
    pub can_emit: Arc<RwLock<bool>>,
}

/// Context for infinite source handlers
pub struct InfiniteSourceContext<H: InfiniteSourceHandler> {
    /// The handler instance that implements source logic
    pub handler: Arc<RwLock<H>>,
    
    /// This source's stage ID
    pub stage_id: StageId,
    
    /// Human-readable stage name for logging
    pub stage_name: String,
    
    /// Journal for writing events
    pub journal: Arc<ReactiveJournal>,
    
    /// Message bus for pipeline communication
    pub bus: Arc<FsmMessageBus>,
    
    /// Writer ID for this source (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,
    
    /// Flag indicating if source can emit (set to true after Start event)
    pub can_emit: Arc<RwLock<bool>>,
    
    /// Flag to track if shutdown was requested
    pub shutdown_requested: Arc<RwLock<bool>>,
}

impl<H: FiniteSourceHandler> FiniteSourceContext<H> {
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
            writer_id: Arc::new(RwLock::new(None)),
            can_emit: Arc::new(RwLock::new(false)),
        }
    }
}

impl<H: InfiniteSourceHandler> InfiniteSourceContext<H> {
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
            writer_id: Arc::new(RwLock::new(None)),
            can_emit: Arc::new(RwLock::new(false)),
            shutdown_requested: Arc::new(RwLock::new(false)),
        }
    }
}