//! Journal writer ID - identifies which journal wrote an envelope

use crate::id::JournalId;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Identifies the journal that wrote an event envelope
/// This wraps JournalId to make it clear this is about envelope authorship
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JournalWriterId(JournalId);

impl JournalWriterId {
    /// Create a new journal writer ID
    pub fn new() -> Self {
        JournalWriterId(JournalId::new())
    }

    /// Create from an existing JournalId
    pub fn from_journal_id(journal_id: JournalId) -> Self {
        JournalWriterId(journal_id)
    }

    /// Get the inner JournalId
    pub fn as_journal_id(&self) -> &JournalId {
        &self.0
    }

    /// Convert to JournalId
    pub fn into_journal_id(self) -> JournalId {
        self.0
    }
}

impl Default for JournalWriterId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JournalWriterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "journal_writer_{}", self.0)
    }
}

impl From<JournalId> for JournalWriterId {
    fn from(journal_id: JournalId) -> Self {
        JournalWriterId(journal_id)
    }
}

impl From<JournalWriterId> for JournalId {
    fn from(id: JournalWriterId) -> Self {
        id.0
    }
}

impl AsRef<JournalId> for JournalWriterId {
    fn as_ref(&self) -> &JournalId {
        &self.0
    }
}
