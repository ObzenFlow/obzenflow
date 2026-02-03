// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! CorrelationId newtype wrapper

use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

/// Unique identifier for correlating events through a flow
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CorrelationId(Ulid);

impl CorrelationId {
    /// Create a new CorrelationId
    pub fn new() -> Self {
        CorrelationId(Ulid::new())
    }

    /// Create from an existing Ulid
    pub fn from_ulid(ulid: Ulid) -> Self {
        CorrelationId(ulid)
    }

    /// Get the inner Ulid
    pub fn as_ulid(&self) -> &Ulid {
        &self.0
    }

    /// Convert to Ulid
    pub fn into_ulid(self) -> Ulid {
        self.0
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Ulid> for CorrelationId {
    fn from(ulid: Ulid) -> Self {
        CorrelationId(ulid)
    }
}

impl From<CorrelationId> for Ulid {
    fn from(id: CorrelationId) -> Self {
        id.0
    }
}

impl AsRef<Ulid> for CorrelationId {
    fn as_ref(&self) -> &Ulid {
        &self.0
    }
}
