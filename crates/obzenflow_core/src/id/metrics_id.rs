//! Metrics aggregator identifier - globally unique identifier for metrics aggregators

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Strongly typed metrics aggregator identifier - globally unique
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MetricsId(Ulid);

impl MetricsId {
    /// Generate a new unique metrics ID
    pub fn new() -> Self {
        MetricsId(Ulid::new())
    }
    
    /// Create from a specific ULID (mainly for testing/deserialization)
    pub fn from_ulid(ulid: Ulid) -> Self {
        MetricsId(ulid)
    }
    
    /// Get the underlying ULID
    pub fn as_ulid(&self) -> Ulid {
        self.0
    }
}

impl Default for MetricsId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MetricsId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics_{}", self.0)
    }
}

impl From<Ulid> for MetricsId {
    fn from(ulid: Ulid) -> Self {
        MetricsId(ulid)
    }
}

impl From<MetricsId> for Ulid {
    fn from(metrics_id: MetricsId) -> Self {
        metrics_id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metrics_id_generation() {
        let id1 = MetricsId::new();
        let id2 = MetricsId::new();
        assert_ne!(id1, id2);
    }
    
    #[test]
    fn test_metrics_id_display() {
        let ulid = Ulid::new();
        let id = MetricsId::from_ulid(ulid);
        assert_eq!(format!("{}", id), format!("metrics_{}", ulid));
    }
}