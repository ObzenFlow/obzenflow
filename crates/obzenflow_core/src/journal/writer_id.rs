//! Writer identifier for the journal
//!
//! WriterId identifies who wrote an event using ULID.
//! This provides both uniqueness and timestamp information
//! about when the writer was created.

use serde::{Serialize, Deserialize};
use std::fmt;
use ulid::Ulid;

/// Writer identifier using ULID
/// 
/// Each writer in the system has a unique ID that also
/// encodes when the writer was created
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WriterId(Ulid);

impl WriterId {
    /// Create a new WriterId with current timestamp
    pub fn new() -> Self {
        WriterId(Ulid::new())
    }
    
    /// Create a WriterId from a string
    pub fn from_string(id: &str) -> Result<Self, ulid::DecodeError> {
        Ok(WriterId(Ulid::from_string(id)?))
    }
    
    /// Get the string representation
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }
    
    /// Extract the timestamp (milliseconds since Unix epoch)
    pub fn timestamp_ms(&self) -> u64 {
        self.0.timestamp_ms()
    }
    
    /// Get the inner ULID
    pub fn as_ulid(&self) -> Ulid {
        self.0
    }
}

impl Default for WriterId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WriterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Ulid> for WriterId {
    fn from(ulid: Ulid) -> Self {
        WriterId(ulid)
    }
}

impl TryFrom<String> for WriterId {
    type Error = ulid::DecodeError;
    
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_string(&s)
    }
}

impl TryFrom<&str> for WriterId {
    type Error = ulid::DecodeError;
    
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_string(s)
    }
}
