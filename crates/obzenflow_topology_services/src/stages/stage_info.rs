use super::StageId;

/// Stage information - combines ID with human-readable name
#[derive(Debug, Clone)]
pub struct StageInfo {
    pub id: StageId,
    pub name: String,  // For debugging/logging only - never used for logic!
}

impl StageInfo {
    pub fn new(id: StageId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }
    
    /// Create with auto-generated name
    pub fn auto_named(id: StageId) -> Self {
        Self {
            name: id.to_string(), // Uses Display impl which formats as "stage_<ulid>"
            id,
        }
    }
}