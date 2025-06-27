//! Configuration shared by all stage supervisors

use std::sync::Arc;
use crate::data_plane::journal_subscription::ReactiveJournal;
use crate::message_bus::FsmMessageBus;
use obzenflow_topology_services::stages::StageId;

/// Configuration for creating any stage supervisor
pub struct StageConfig {
    pub stage_id: StageId,
    pub stage_name: String,
    /// Upstream stages this stage should subscribe to (empty for sources)
    pub upstream_stages: Vec<StageId>,
    pub journal: Arc<ReactiveJournal>,
    pub message_bus: Arc<FsmMessageBus>,
}