//! Journal ownership types
//!
//! Every journal must have an owner for identification and debugging

use crate::id::{StageId, SystemId};
use serde::{Deserialize, Serialize};

/// Identifies who owns a journal
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JournalOwner {
    /// System component owns this journal (pipeline, metrics, etc.)
    System { system_id: SystemId },

    /// Stage owns this journal (for stage-local data events)
    Stage { stage_id: StageId },
}

impl JournalOwner {
    /// Create a system owner
    pub fn system(system_id: SystemId) -> Self {
        Self::System { system_id }
    }

    /// Create a stage owner
    pub fn stage(stage_id: StageId) -> Self {
        Self::Stage { stage_id }
    }

    /// Get a unique string representation for file/directory naming
    pub fn as_path_component(&self) -> String {
        match self {
            Self::System { system_id } => format!("{}", system_id),
            Self::Stage { stage_id } => format!("stage_{}", stage_id.as_u64()),
        }
    }
}

impl std::fmt::Display for JournalOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::System { system_id } => write!(f, "System({})", system_id),
            Self::Stage { stage_id } => write!(f, "Stage({})", stage_id),
        }
    }
}
