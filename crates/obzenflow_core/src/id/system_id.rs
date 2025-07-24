//! System identifier - globally unique identifier for system components
//! 
//! System components are self-supervised FSMs like Pipeline and MetricsAggregator
//! that handle system-level orchestration rather than data processing.

use serde::{Deserialize, Serialize};
use ulid::Ulid;
use std::str::FromStr;

/// Strongly typed system component identifier - globally unique
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SystemId(Ulid);

impl SystemId {
    /// Generate a new unique system ID
    pub fn new() -> Self {
        SystemId(Ulid::new())
    }
    
    /// Create from a specific ULID (mainly for testing/deserialization)
    pub fn from_ulid(ulid: Ulid) -> Self {
        SystemId(ulid)
    }
    
    /// Get the underlying ULID
    pub fn as_ulid(&self) -> Ulid {
        self.0
    }
    
    /// Get as u64 for compatibility (uses lower 64 bits of ULID)
    pub fn as_u64(&self) -> u64 {
        self.0.0 as u64
    }
    
    /// Create a const system ID (for special system components)
    /// Only use this for compile-time constants!
    pub const fn new_const(val: u128) -> Self {
        SystemId(Ulid(val))
    }
}

impl Default for SystemId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SystemId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "system_{}", self.0)
    }
}

impl From<Ulid> for SystemId {
    fn from(ulid: Ulid) -> Self {
        SystemId(ulid)
    }
}

impl From<SystemId> for Ulid {
    fn from(system_id: SystemId) -> Self {
        system_id.0
    }
}

impl FromStr for SystemId {
    type Err = ulid::DecodeError;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Handle both "system_ULID" format and raw ULID
        let ulid_str = s.strip_prefix("system_").unwrap_or(s);
        let ulid = Ulid::from_str(ulid_str)?;
        Ok(SystemId(ulid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_system_id_generation() {
        let id1 = SystemId::new();
        let id2 = SystemId::new();
        assert_ne!(id1, id2);
    }
    
    #[test]
    fn test_system_id_display() {
        let ulid = Ulid::new();
        let id = SystemId::from_ulid(ulid);
        assert_eq!(format!("{}", id), format!("system_{}", ulid));
    }
    
    #[test]
    fn test_system_id_serde() {
        let id = SystemId::new();
        let json = serde_json::to_string(&id).unwrap();
        let id2: SystemId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, id2);
    }
}