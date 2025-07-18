//! Strong types for journal names
//!
//! No more stringly typed journal names - use proper enums!

use crate::{StageId, MetricsId};
use crate::event::flow_context::StageType;

/// Strongly typed journal name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JournalName {
    /// Control journal for system events
    Control,
    /// Stage journal for data events
    Stage {
        /// The unique stage ID
        id: StageId,
        /// The stage type (Source, Transform, Sink)
        stage_type: StageType,
        /// The user-provided name from the DSL
        name: String,
    },
    /// Metrics journal for metrics events
    Metrics(MetricsId),
}

impl JournalName {
    /// Convert to filename for disk storage
    pub fn to_filename(&self) -> String {
        match self {
            JournalName::Control => "control.log".to_string(),
            JournalName::Stage { id, stage_type, name } => {
                format!("{:?}_{}_{}.log", stage_type, name, id)
            },
            JournalName::Metrics(id) => format!("metrics_{}.log", id),
        }
    }
}