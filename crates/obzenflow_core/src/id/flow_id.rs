//! Flow ID type for identifying flow executions

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use ulid::Ulid;

/// Unique identifier for a flow execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlowId(Ulid);

impl FlowId {
    /// Create a new unique flow ID
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// Create a flow ID from a ULID
    pub fn from_ulid(ulid: Ulid) -> Self {
        Self(ulid)
    }

    /// Get the underlying ULID
    pub fn as_ulid(&self) -> &Ulid {
        &self.0
    }

    /// Convert to ULID
    pub fn into_ulid(self) -> Ulid {
        self.0
    }
}

impl Default for FlowId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for FlowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "flow_{}", self.0)
    }
}

impl From<Ulid> for FlowId {
    fn from(ulid: Ulid) -> Self {
        Self(ulid)
    }
}

impl From<FlowId> for Ulid {
    fn from(id: FlowId) -> Self {
        id.0
    }
}

impl FromStr for FlowId {
    type Err = ulid::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Handle both "flow_ULID" format and raw ULID
        let ulid_str = s.strip_prefix("flow_").unwrap_or(s);
        let ulid = Ulid::from_str(ulid_str)?;
        Ok(FlowId(ulid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flow_id_creation() {
        let id1 = FlowId::new();
        let id2 = FlowId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_flow_id_display() {
        let ulid = Ulid::new();
        let id = FlowId::from_ulid(ulid);
        assert_eq!(format!("{}", id), format!("flow_{}", ulid));
    }
}
