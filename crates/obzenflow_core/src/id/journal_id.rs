// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal identifier type

use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

/// Identifies a specific journal instance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JournalId(Ulid);

impl JournalId {
    /// Create a new JournalId
    pub fn new() -> Self {
        JournalId(Ulid::new())
    }

    /// Create from an existing Ulid
    pub fn from_ulid(ulid: Ulid) -> Self {
        JournalId(ulid)
    }

    /// Get the inner ULID
    pub fn as_ulid(&self) -> &Ulid {
        &self.0
    }

    /// Convert to Ulid
    pub fn into_ulid(self) -> Ulid {
        self.0
    }
}

impl Default for JournalId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JournalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "journal_{}", self.0)
    }
}

impl From<Ulid> for JournalId {
    fn from(ulid: Ulid) -> Self {
        JournalId(ulid)
    }
}

impl From<JournalId> for Ulid {
    fn from(id: JournalId) -> Self {
        id.0
    }
}

impl AsRef<Ulid> for JournalId {
    fn as_ref(&self) -> &Ulid {
        &self.0
    }
}
