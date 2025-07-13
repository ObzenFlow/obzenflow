//! Stage identifier - globally unique identifier for pipeline stages

use serde::{Deserialize, Serialize};
use ulid::Ulid;
use std::str::FromStr;

/// Strongly typed stage identifier - globally unique
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StageId(Ulid);

impl StageId {
    /// Generate a new unique stage ID
    pub fn new() -> Self {
        StageId(Ulid::new())
    }
    
    /// Create from a specific ULID (mainly for testing/deserialization)
    pub fn from_ulid(ulid: Ulid) -> Self {
        StageId(ulid)
    }
    
    /// Get the underlying ULID
    pub fn as_ulid(&self) -> Ulid {
        self.0
    }
    
    /// Get as u64 for compatibility (uses lower 64 bits of ULID)
    pub fn as_u64(&self) -> u64 {
        self.0.0 as u64
    }
    
    /// Create a const stage ID (for special stages like control)
    /// Only use this for compile-time constants!
    pub const fn new_const(val: u128) -> Self {
        StageId(Ulid(val))
    }
}

impl Default for StageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for StageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stage_{}", self.0)
    }
}

impl From<Ulid> for StageId {
    fn from(ulid: Ulid) -> Self {
        StageId(ulid)
    }
}

impl From<StageId> for Ulid {
    fn from(stage_id: StageId) -> Self {
        stage_id.0
    }
}

impl FromStr for StageId {
    type Err = ulid::DecodeError;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Handle both "stage_ULID" format and raw ULID
        let ulid_str = s.strip_prefix("stage_").unwrap_or(s);
        let ulid = Ulid::from_str(ulid_str)?;
        Ok(StageId(ulid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_stage_id_generation() {
        let id1 = StageId::new();
        let id2 = StageId::new();
        assert_ne!(id1, id2);
    }
    
    #[test]
    fn test_stage_id_display() {
        let ulid = Ulid::new();
        let id = StageId::from_ulid(ulid);
        assert_eq!(format!("{}", id), format!("stage_{}", ulid));
    }
    
    #[test]
    fn test_stage_id_serde() {
        let id = StageId::new();
        let json = serde_json::to_string(&id).unwrap();
        let id2: StageId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, id2);
    }
}