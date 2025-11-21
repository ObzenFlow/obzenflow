//! Event identifier type
//!
//! EventId uses ULID (Universally Unique Lexicographically Sortable Identifier)
//! for event identification. This provides both uniqueness and time-ordering.

use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

/// Event identifier using ULID
///
/// ULIDs provide:
/// - Lexicographic sortability
/// - Timestamp extraction
/// - 128-bit randomness
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventId(Ulid);

impl EventId {
    /// Create a new EventId with current timestamp
    pub fn new() -> Self {
        EventId(Ulid::new())
    }

    /// Create an EventId from a string
    pub fn from_string(id: &str) -> Result<Self, ulid::DecodeError> {
        Ok(EventId(Ulid::from_string(id)?))
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

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Ulid> for EventId {
    fn from(ulid: Ulid) -> Self {
        EventId(ulid)
    }
}

impl TryFrom<String> for EventId {
    type Error = ulid::DecodeError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_string(&s)
    }
}

impl TryFrom<&str> for EventId {
    type Error = ulid::DecodeError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_string(s)
    }
}
