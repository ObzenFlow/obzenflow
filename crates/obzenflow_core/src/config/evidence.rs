// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Run-manifest evidence for the resolved effective config (FLOWIP-010 §6a).
//!
//! "What configuration was this run executed under" is answered from the run
//! directory. Scope and source are wire strings so readers tolerate axes
//! added later (gap 13); values are redacted where the knob schema says so.

use serde::{Deserialize, Serialize};

/// Evidence-local schema version, independent of `RUN_MANIFEST_VERSION`.
pub const EVIDENCE_SCHEMA_VERSION: u32 = 1;

/// One resolved value with both provenance axes, in wire form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedValueDoc {
    pub key_path: String,
    /// Wire spelling of [`crate::config::ConfigScope`]: `global`, `flow`,
    /// `stage:<key>`, `edge:<up|>down>`.
    pub scope: String,
    /// Wire spelling of [`crate::config::ConfigSource`]; open string set.
    pub source: String,
    /// `"***redacted***"` when `redacted` is set.
    pub value: serde_json::Value,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub redacted: bool,
}

/// The redacted effective config recorded at flow build.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveConfigEvidence {
    pub schema_version: u32,
    /// Sorted by `(key_path, scope)` for deterministic golden tests.
    pub values: Vec<ResolvedValueDoc>,
}

impl EffectiveConfigEvidence {
    pub fn new(mut values: Vec<ResolvedValueDoc>) -> Self {
        values.sort_by(|a, b| {
            a.key_path
                .cmp(&b.key_path)
                .then_with(|| a.scope.cmp(&b.scope))
        });
        Self {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_sorts_by_key_path_then_scope() {
        let doc = |key: &str, scope: &str| ResolvedValueDoc {
            key_path: key.to_string(),
            scope: scope.to_string(),
            source: "file".to_string(),
            value: serde_json::json!(1),
            redacted: false,
        };
        let evidence = EffectiveConfigEvidence::new(vec![
            doc("runtime.b", "global"),
            doc("runtime.a", "stage:x"),
            doc("runtime.a", "global"),
        ]);
        let order: Vec<(String, String)> = evidence
            .values
            .iter()
            .map(|d| (d.key_path.clone(), d.scope.clone()))
            .collect();
        assert_eq!(
            order,
            vec![
                ("runtime.a".to_string(), "global".to_string()),
                ("runtime.a".to_string(), "stage:x".to_string()),
                ("runtime.b".to_string(), "global".to_string()),
            ]
        );
    }

    #[test]
    fn redacted_flag_is_omitted_when_false_and_kept_when_true() {
        let plain = ResolvedValueDoc {
            key_path: "runtime.a".to_string(),
            scope: "global".to_string(),
            source: "default".to_string(),
            value: serde_json::json!(7),
            redacted: false,
        };
        let wire = serde_json::to_string(&plain).unwrap();
        assert!(!wire.contains("redacted"));

        let secret = ResolvedValueDoc {
            redacted: true,
            value: serde_json::json!("***redacted***"),
            ..plain
        };
        let wire = serde_json::to_string(&secret).unwrap();
        assert!(wire.contains("\"redacted\":true"));
        // An evidence blob written by a future version with an unknown source
        // still deserializes (gap 13).
        let future = r#"{"key_path":"a","scope":"global","source":"profile","value":1}"#;
        let doc: ResolvedValueDoc = serde_json::from_str(future).unwrap();
        assert_eq!(doc.source, "profile");
    }
}
