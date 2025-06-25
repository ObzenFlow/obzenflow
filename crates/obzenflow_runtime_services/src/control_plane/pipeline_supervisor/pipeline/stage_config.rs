use obzenflow_topology_services::stages::StageId;
use crate::control_plane::stage_supervisor::event_handler::EventHandler;
use std::sync::Arc;

/// Stage configuration data - everything needed to build a stage
pub struct StageConfig {
    pub stage_id: StageId,
    pub name: String,
    pub handler: Arc<dyn EventHandler>,
    pub is_source: bool,
}