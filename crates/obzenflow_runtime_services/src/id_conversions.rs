//! Conversion helpers between core StageId and Ulid
//! 
//! Since obzenflow-topology uses Ulid directly as StageId,
//! we need helpers to convert between obzenflow_core::StageId and Ulid.

use obzenflow_core::StageId;
use ulid::Ulid;

/// Extension trait for converting StageId to/from Ulid
pub trait StageIdExt {
    fn to_ulid(&self) -> Ulid;
    fn from_ulid(ulid: Ulid) -> Self;
}

impl StageIdExt for StageId {
    fn to_ulid(&self) -> Ulid {
        self.as_ulid()
    }
    
    fn from_ulid(ulid: Ulid) -> Self {
        StageId::from_ulid(ulid)
    }
}