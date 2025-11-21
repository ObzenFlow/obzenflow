//! Unified writer ID that can represent either a Stage or System writer

use crate::id::{StageId, SystemId};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Unified writer identifier that can be either a Stage or System
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "id")]
pub enum WriterId {
    /// A stage writer
    Stage(StageId),
    /// A system component writer
    System(SystemId),
}

impl WriterId {
    /// Check if this is a stage writer
    pub fn is_stage(&self) -> bool {
        matches!(self, WriterId::Stage(_))
    }

    /// Check if this is a system writer
    pub fn is_system(&self) -> bool {
        matches!(self, WriterId::System(_))
    }

    /// Try to get as a stage ID
    pub fn as_stage(&self) -> Option<&StageId> {
        match self {
            WriterId::Stage(id) => Some(id),
            WriterId::System(_) => None,
        }
    }

    /// Try to get as a system ID
    pub fn as_system(&self) -> Option<&SystemId> {
        match self {
            WriterId::System(id) => Some(id),
            WriterId::Stage(_) => None,
        }
    }

    /// Get the underlying ULID regardless of type
    pub fn as_ulid(&self) -> ulid::Ulid {
        match self {
            WriterId::Stage(id) => id.as_ulid(),
            WriterId::System(id) => id.as_ulid(),
        }
    }
}

impl fmt::Display for WriterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriterId::Stage(id) => write!(f, "writer_{}", id),
            WriterId::System(id) => write!(f, "writer_{}", id),
        }
    }
}

// From implementations for construction
impl From<StageId> for WriterId {
    fn from(stage_id: StageId) -> Self {
        WriterId::Stage(stage_id)
    }
}

impl From<SystemId> for WriterId {
    fn from(system_id: SystemId) -> Self {
        WriterId::System(system_id)
    }
}

// TryFrom implementations for extraction
impl TryFrom<WriterId> for StageId {
    type Error = &'static str;

    fn try_from(writer_id: WriterId) -> Result<Self, Self::Error> {
        match writer_id {
            WriterId::Stage(id) => Ok(id),
            WriterId::System(_) => Err("WriterId is not a Stage"),
        }
    }
}

impl TryFrom<WriterId> for SystemId {
    type Error = &'static str;

    fn try_from(writer_id: WriterId) -> Result<Self, Self::Error> {
        match writer_id {
            WriterId::System(id) => Ok(id),
            WriterId::Stage(_) => Err("WriterId is not a System"),
        }
    }
}

// From<ulid::Ulid> requires knowing the type, so we provide constructors instead
impl WriterId {
    /// Create a stage writer from a ULID
    pub fn stage_from_ulid(ulid: ulid::Ulid) -> Self {
        WriterId::Stage(StageId::from(ulid))
    }

    /// Create a system writer from a ULID
    pub fn system_from_ulid(ulid: ulid::Ulid) -> Self {
        WriterId::System(SystemId::from(ulid))
    }
}

// Into<ulid::Ulid> for any WriterId
impl From<WriterId> for ulid::Ulid {
    fn from(writer_id: WriterId) -> Self {
        writer_id.as_ulid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer_id_from_stage() {
        let stage_id = StageId::new();
        let writer_id = WriterId::from(stage_id);
        assert!(writer_id.is_stage());
        assert!(!writer_id.is_system());
        assert_eq!(writer_id.as_stage(), Some(&stage_id));
        assert_eq!(writer_id.as_system(), None);
    }

    #[test]
    fn test_writer_id_from_system() {
        let system_id = SystemId::new();
        let writer_id = WriterId::from(system_id);
        assert!(!writer_id.is_stage());
        assert!(writer_id.is_system());
        assert_eq!(writer_id.as_stage(), None);
        assert_eq!(writer_id.as_system(), Some(&system_id));
    }

    #[test]
    fn test_try_from_writer_id() {
        let stage_id = StageId::new();
        let writer_id = WriterId::from(stage_id);

        // Should succeed for StageId
        let extracted_stage: StageId = writer_id.try_into().unwrap();
        assert_eq!(extracted_stage, stage_id);

        // Should fail for SystemId
        let result: Result<SystemId, _> = writer_id.try_into();
        assert!(result.is_err());

        let system_id = SystemId::new();
        let writer_id = WriterId::from(system_id);

        // Should succeed for SystemId
        let extracted_system: SystemId = writer_id.try_into().unwrap();
        assert_eq!(extracted_system, system_id);

        // Should fail for StageId
        let result: Result<StageId, _> = writer_id.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_writer_id_ulid_conversion() {
        let stage_id = StageId::new();
        let writer_id = WriterId::from(stage_id);
        let ulid: ulid::Ulid = writer_id.into();
        assert_eq!(ulid, stage_id.as_ulid());

        // Test constructors from ULID
        let ulid = ulid::Ulid::new();
        let stage_writer = WriterId::stage_from_ulid(ulid);
        assert!(stage_writer.is_stage());

        let system_writer = WriterId::system_from_ulid(ulid);
        assert!(system_writer.is_system());
    }

    #[test]
    fn test_writer_id_serde() {
        let stage_writer = WriterId::from(StageId::new());
        let json = serde_json::to_string(&stage_writer).unwrap();
        let deserialized: WriterId = serde_json::from_str(&json).unwrap();
        assert_eq!(stage_writer, deserialized);

        let system_writer = WriterId::from(SystemId::new());
        let json = serde_json::to_string(&system_writer).unwrap();
        let deserialized: WriterId = serde_json::from_str(&json).unwrap();
        assert_eq!(system_writer, deserialized);
    }
}
