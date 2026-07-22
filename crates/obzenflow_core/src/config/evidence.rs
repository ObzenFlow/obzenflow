// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Run-manifest evidence for the resolved effective config (FLOWIP-010 §6a).
//!
//! "What configuration was this run executed under" is answered from the run
//! directory. Scope and source are wire strings so readers tolerate axes
//! added later (gap 13); values are redacted where the knob schema says so.

use super::ConfigSubject;
use serde::{Deserialize, Serialize};

/// Evidence-local schema version, independent of `RUN_MANIFEST_VERSION`.
pub const EVIDENCE_SCHEMA_VERSION: u32 = 2;

/// The concrete point that received an effective configuration value.
///
/// This is intentionally optional on [`ResolvedValueDoc`]: base and offline
/// views exist before a flow topology is known. Effect-targeted per-flow rows
/// always carry the `Effect` variant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResolvedForDoc {
    Effect { stage: String, effect_type: String },
}

/// One resolved value with both provenance axes, in wire form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedValueDoc {
    pub key_path: String,
    /// Concrete application point. Required for flow-materialised effect
    /// values and absent from version-1 and topology-free rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_for: Option<ResolvedForDoc>,
    /// Wire spelling of [`crate::config::ConfigScope`]: `global`, `flow`,
    /// `stage:<key>`, `edge:<up|>down>`.
    pub scope: String,
    /// Wire spelling of [`crate::config::ConfigSource`]; open string set.
    pub source: String,
    /// The winning protected subject. Missing version-1 values are
    /// interpreted as [`ConfigSubject::Unqualified`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub winner_subject: Option<ConfigSubject>,
    /// `"***redacted***"` when `redacted` is set.
    pub value: serde_json::Value,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub redacted: bool,
}

impl ResolvedValueDoc {
    /// Semantic winning subject. Version-1 rows omitted the field, which is
    /// defined to mean an unqualified broadcast rather than an unknown value.
    pub fn winning_subject(&self) -> ConfigSubject {
        self.winner_subject
            .clone()
            .unwrap_or(ConfigSubject::Unqualified)
    }
}

/// The redacted effective config recorded at flow build.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveConfigEvidence {
    pub schema_version: u32,
    /// Sorted by `(key_path, resolved_for, scope)` for deterministic golden
    /// tests without collapsing equal inherited effect values.
    pub values: Vec<ResolvedValueDoc>,
}

impl EffectiveConfigEvidence {
    pub fn new(mut values: Vec<ResolvedValueDoc>) -> Self {
        values.sort_by(|a, b| {
            a.key_path
                .cmp(&b.key_path)
                .then_with(|| a.resolved_for.cmp(&b.resolved_for))
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
    fn evidence_sorts_by_key_path_resolution_point_then_scope() {
        let doc = |key: &str, scope: &str| ResolvedValueDoc {
            key_path: key.to_string(),
            resolved_for: None,
            scope: scope.to_string(),
            source: "file".to_string(),
            winner_subject: None,
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
            resolved_for: None,
            scope: "global".to_string(),
            source: "default".to_string(),
            winner_subject: None,
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
        assert_eq!(doc.resolved_for, None);
        assert_eq!(doc.winner_subject, None);
    }

    #[test]
    fn effect_rows_sort_and_remain_distinct_by_resolution_point() {
        let row = |effect_type: &str| ResolvedValueDoc {
            key_path: "effects.resilience.breaker.minimum_calls".to_string(),
            resolved_for: Some(ResolvedForDoc::Effect {
                stage: "authorize_payment".to_string(),
                effect_type: effect_type.to_string(),
            }),
            scope: "stage:authorize_payment".to_string(),
            source: "file".to_string(),
            winner_subject: Some(ConfigSubject::Unqualified),
            value: serde_json::json!(8),
            redacted: false,
        };
        let evidence =
            EffectiveConfigEvidence::new(vec![row("payments.refund"), row("payments.authorize")]);
        assert_eq!(evidence.schema_version, 2);
        assert_eq!(evidence.values.len(), 2);
        assert!(matches!(
            evidence.values[0].resolved_for.as_ref(),
            Some(ResolvedForDoc::Effect { effect_type, .. }) if effect_type == "payments.authorize"
        ));
    }

    #[test]
    fn version_one_evidence_remains_readable_as_unqualified() {
        let wire = r#"{
            "schema_version": 1,
            "values": [{
                "key_path": "runtime.max_lineage_depth",
                "scope": "global",
                "source": "file",
                "value": 7
            }]
        }"#;
        let evidence: EffectiveConfigEvidence = serde_json::from_str(wire).unwrap();
        assert_eq!(evidence.schema_version, 1);
        assert_eq!(evidence.values[0].resolved_for, None);
        assert_eq!(evidence.values[0].winner_subject, None);
        assert_eq!(
            evidence.values[0].winning_subject(),
            ConfigSubject::Unqualified
        );
    }
}
