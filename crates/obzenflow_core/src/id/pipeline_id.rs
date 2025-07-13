//! Pipeline identifier - globally unique identifier for pipeline instances

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Strongly typed pipeline identifier - globally unique
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PipelineId(Ulid);

impl PipelineId {
    /// Generate a new unique pipeline ID
    pub fn new() -> Self {
        PipelineId(Ulid::new())
    }
    
    /// Create from a specific ULID (mainly for testing/deserialization)
    pub fn from_ulid(ulid: Ulid) -> Self {
        PipelineId(ulid)
    }
    
    /// Get the underlying ULID
    pub fn as_ulid(&self) -> Ulid {
        self.0
    }
}

impl Default for PipelineId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PipelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pipeline_{}", self.0)
    }
}

impl From<Ulid> for PipelineId {
    fn from(ulid: Ulid) -> Self {
        PipelineId(ulid)
    }
}

impl From<PipelineId> for Ulid {
    fn from(pipeline_id: PipelineId) -> Self {
        pipeline_id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_pipeline_id_generation() {
        let id1 = PipelineId::new();
        let id2 = PipelineId::new();
        assert_ne!(id1, id2);
    }
    
    #[test]
    fn test_pipeline_id_display() {
        let ulid = Ulid::new();
        let id = PipelineId::from_ulid(ulid);
        assert_eq!(format!("{}", id), format!("pipeline_{}", ulid));
    }
}