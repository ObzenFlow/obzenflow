// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Provenance axes for resolved configuration values (FLOWIP-010 §6).
//!
//! Under scope-before-source composition, source alone cannot explain a
//! resolution, so every effective value carries both axes.

use crate::id::StageKey;
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Where a resolved value came from, within its winning scope.
///
/// Wire form is the snake_case string; unknown strings deserialize to
/// `Other` so manifest and API readers tolerate sources added later
/// (e.g. FLOWIP-010j's `profile`). The resolver never constructs `Other`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConfigSource {
    Default,
    Dsl,
    Env,
    File,
    Cli,
    RuntimeOverlay,
    Other(String),
}

impl ConfigSource {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Default => "default",
            Self::Dsl => "dsl",
            Self::Env => "env",
            Self::File => "file",
            Self::Cli => "cli",
            Self::RuntimeOverlay => "runtime_overlay",
            Self::Other(raw) => raw,
        }
    }

    fn from_wire(raw: &str) -> Self {
        match raw {
            "default" => Self::Default,
            "dsl" => Self::Dsl,
            "env" => Self::Env,
            "file" => Self::File,
            "cli" => Self::Cli,
            "runtime_overlay" => Self::RuntimeOverlay,
            other => Self::Other(other.to_string()),
        }
    }
}

impl fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for ConfigSource {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ConfigSource {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(Self::from_wire(&raw))
    }
}

/// The scope a resolved value won at (FLOWIP-010 §2 scope-before-source,
/// §4c edge scope). `Flow` is payload-less in the first pass: no `FlowKey`
/// newtype exists and the single-active-run posture makes every resolution
/// context one flow.
///
/// Display renders the wire spelling used by evidence and DTOs:
/// `global`, `flow`, `stage:<key>`, `edge:<up|>down>` (the FLOWIP-010e
/// edge-ref form).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfigScope {
    Global,
    Flow,
    Stage {
        stage: StageKey,
    },
    Edge {
        upstream: StageKey,
        downstream: StageKey,
    },
}

impl ConfigScope {
    pub fn stage(stage: impl Into<StageKey>) -> Self {
        Self::Stage {
            stage: stage.into(),
        }
    }

    pub fn edge(upstream: impl Into<StageKey>, downstream: impl Into<StageKey>) -> Self {
        Self::Edge {
            upstream: upstream.into(),
            downstream: downstream.into(),
        }
    }
}

impl fmt::Display for ConfigScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Global => f.write_str("global"),
            Self::Flow => f.write_str("flow"),
            Self::Stage { stage } => write!(f, "stage:{}", stage.as_str()),
            Self::Edge {
                upstream,
                downstream,
            } => write!(f, "edge:{}|>{}", upstream.as_str(), downstream.as_str()),
        }
    }
}

/// Both provenance axes plus the knob address, carried per resolved value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigValueMeta {
    pub source: ConfigSource,
    pub scope: ConfigScope,
    pub key_path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_source_round_trips_and_tolerates_unknown_strings() {
        for source in [
            ConfigSource::Default,
            ConfigSource::Dsl,
            ConfigSource::Env,
            ConfigSource::File,
            ConfigSource::Cli,
            ConfigSource::RuntimeOverlay,
        ] {
            let wire = serde_json::to_string(&source).unwrap();
            let back: ConfigSource = serde_json::from_str(&wire).unwrap();
            assert_eq!(back, source);
        }
        // Gap 13: a source added later (010j's `profile`) must not break readers.
        let back: ConfigSource = serde_json::from_str("\"profile\"").unwrap();
        assert_eq!(back, ConfigSource::Other("profile".to_string()));
        assert_eq!(back.as_str(), "profile");
    }

    #[test]
    fn config_scope_display_uses_edge_ref_form() {
        assert_eq!(ConfigScope::Global.to_string(), "global");
        assert_eq!(ConfigScope::Flow.to_string(), "flow");
        assert_eq!(ConfigScope::stage("enricher").to_string(), "stage:enricher");
        assert_eq!(
            ConfigScope::edge("enricher", "merger").to_string(),
            "edge:enricher|>merger"
        );
    }

    #[test]
    fn config_scope_orders_deterministically() {
        let mut scopes = vec![
            ConfigScope::edge("a", "b"),
            ConfigScope::Global,
            ConfigScope::stage("a"),
            ConfigScope::Flow,
        ];
        scopes.sort();
        assert_eq!(
            scopes,
            vec![
                ConfigScope::Global,
                ConfigScope::Flow,
                ConfigScope::stage("a"),
                ConfigScope::edge("a", "b"),
            ]
        );
    }
}
