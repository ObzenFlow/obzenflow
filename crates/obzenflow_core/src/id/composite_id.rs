// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Composite identifier - the manifest identity of a first-class composite.
//!
//! The value is the topology `subgraph_id` (`{kind}:{binding}`), one per
//! composite binding in a flow. It keys the composite's own lifecycle evidence
//! (`SystemEventType::CompositeLifecycle`) and its metrics on the rail. String
//! backed because the durable identity is a string at every boundary (topology
//! membership, Studio, diagnostics), per FLOWIP-128a D2/B1.

use serde::{Deserialize, Serialize};

/// Strongly typed composite identifier: the manifest `subgraph_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CompositeId(String);

impl CompositeId {
    /// Create from the manifest subgraph id (`{kind}:{binding}`).
    pub fn new(id: impl Into<String>) -> Self {
        CompositeId(id.into())
    }

    /// Borrow the underlying subgraph id.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CompositeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for CompositeId {
    fn from(s: String) -> Self {
        CompositeId(s)
    }
}

impl From<&str> for CompositeId {
    fn from(s: &str) -> Self {
        CompositeId(s.to_string())
    }
}

impl AsRef<str> for CompositeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_serde_as_a_plain_string() {
        let id = CompositeId::new("ai_map_reduce:digest");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"ai_map_reduce:digest\"");
        let back: CompositeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn display_is_the_bare_subgraph_id() {
        assert_eq!(
            CompositeId::from("ai_map_reduce:digest").to_string(),
            "ai_map_reduce:digest"
        );
    }
}
