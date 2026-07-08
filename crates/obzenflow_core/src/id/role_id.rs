// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Composite role identifier - the semantic function of a member inside a
//! composite (`chunk`, `map`, `collect`, `finalize`, `compensate`, ...).
//!
//! A stable string id with the locked grammar `[a-z][a-z0-9_]*`, unique per
//! composite (FLOWIP-128a D2). It names the member that a composite lifecycle
//! `Failed { at }` fact points at, resolved by the supervisor from manifest
//! membership.

use serde::{Deserialize, Serialize};

/// Strongly typed composite role identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RoleId(String);

impl RoleId {
    /// Create from a role string.
    pub fn new(role: impl Into<String>) -> Self {
        RoleId(role.into())
    }

    /// Borrow the underlying role string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RoleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for RoleId {
    fn from(s: String) -> Self {
        RoleId(s)
    }
}

impl From<&str> for RoleId {
    fn from(s: &str) -> Self {
        RoleId(s.to_string())
    }
}

impl AsRef<str> for RoleId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_serde_as_a_plain_string() {
        let role = RoleId::new("map");
        let json = serde_json::to_string(&role).unwrap();
        assert_eq!(json, "\"map\"");
        let back: RoleId = serde_json::from_str(&json).unwrap();
        assert_eq!(role, back);
    }
}
