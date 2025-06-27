use ulid::Ulid;

/// Strongly typed stage identifier - globally unique
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
}

impl std::fmt::Display for StageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stage_{}", self.0)
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
}