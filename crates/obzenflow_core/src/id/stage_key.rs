// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-stable stage key.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;

/// Replay-stable stage key: the stage's `StageConfig.name`, the key written to
/// `run_manifest.json` and carried in replay-sensitive coordinates (provenance,
/// refusal facts). Distinct from the runtime [`crate::StageId`] (a ULID). It is a
/// transparent string newtype mirroring [`crate::EventType`]; FLOWIP-114e is
/// converging the remaining raw-`String` stage names onto this type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StageKey(pub String);

impl StageKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for StageKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for StageKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl AsRef<str> for StageKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for StageKey {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for StageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for StageKey {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<StageKey> for &str {
    fn eq(&self, other: &StageKey) -> bool {
        *self == other.as_str()
    }
}
