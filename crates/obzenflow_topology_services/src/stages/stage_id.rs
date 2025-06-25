use std::sync::atomic::{AtomicU32, Ordering};

/// Strongly typed stage identifier - used for all lookups and references
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StageId(u32);

impl StageId {
    /// Generate next stage ID (atomic counter)
    pub fn next() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        StageId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
    
    /// Get the underlying u32 value (for serialization/debugging)
    pub fn as_u32(&self) -> u32 {
        self.0
    }
    
    /// Create from raw u32 (use with caution - mainly for deserialization)
    pub fn from_u32(value: u32) -> Self {
        StageId(value)
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
        let id1 = StageId::next();
        let id2 = StageId::next();
        assert_ne!(id1, id2);
        assert!(id1 < id2);
    }
    
    #[test]
    fn test_stage_id_display() {
        let id = StageId::from_u32(42);
        assert_eq!(format!("{}", id), "stage_42");
    }
}