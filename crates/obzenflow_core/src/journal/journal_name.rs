//! Strong types for journal names
//!
//! No more stringly typed journal names - use proper enums!

use crate::{StageId, MetricsId};

/// Strongly typed journal name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JournalName {
    /// Control journal for system events
    Control,
    /// Stage journal for data events
    Stage(StageId),
    /// Metrics journal for metrics events
    Metrics(MetricsId),
}

impl JournalName {
    /// Convert to filename for disk storage
    pub fn to_filename(&self) -> String {
        match self {
            JournalName::Control => "control.log".to_string(),
            JournalName::Stage(id) => format!("stage_{}.log", id),
            JournalName::Metrics(id) => format!("metrics_{}.log", id),
        }
    }
}