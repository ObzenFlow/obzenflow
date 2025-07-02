//! Runtime resources needed by stage supervisors

use std::sync::Arc;
use crate::event_flow::reactive_journal::ReactiveJournal;
use crate::message_bus::FsmMessageBus;
use obzenflow_topology_services::stages::StageId;

/// Runtime dependencies that stages need to operate
/// 
/// This is separate from StageConfig which describes WHAT to create.
/// StageResources provides WHAT TO CREATE IT WITH.
#[derive(Clone)]
pub struct StageResources {
    /// Journal for writing events
    pub journal: Arc<ReactiveJournal>,
    
    /// Message bus for inter-component communication
    pub message_bus: Arc<FsmMessageBus>,
    
    /// Upstream stage IDs (empty for sources)
    pub upstream_stages: Vec<StageId>,
}