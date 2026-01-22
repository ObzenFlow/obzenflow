//! Strong types for journal names
//!
//! No more stringly typed journal names - use proper enums!

use crate::event::context::StageType;
use crate::StageId;

/// Strongly typed journal name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JournalName {
    /// System journal for orchestration events
    System,
    /// Stage journal for data events
    Stage {
        /// The unique stage ID
        id: StageId,
        /// The stage type (Source, Transform, Sink)
        stage_type: StageType,
        /// The user-provided name from the DSL
        name: String,
    },
}

impl JournalName {
    /// Convert to filename for disk storage
    pub fn to_filename(&self) -> String {
        match self {
            JournalName::System => "system.log".to_string(),
            JournalName::Stage {
                id,
                stage_type,
                name,
            } => {
                format!("{stage_type:?}_{name}_{id}.log")
            }
        }
    }
}
