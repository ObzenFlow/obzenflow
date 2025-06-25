use crate::control_plane::stage_supervisor::event_handler::EventHandler;
use std::sync::Arc;

/// Observer configuration data
pub struct ObserverConfig {
    pub name: String,
    pub handler: Arc<dyn EventHandler>,
}