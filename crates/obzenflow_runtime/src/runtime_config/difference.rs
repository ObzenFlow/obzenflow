// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The configuration a replayed or resumed run changed from a run whose
//! history it rebuilds (FLOWIP-010t B6, B9). A projection over two runs'
//! recorded evidence: it adds no event and never refuses or alters a run.

use super::app::toml_key;
use super::model::diff;
use super::schema::{knob, KnobRole};
use obzenflow_core::config::{EffectiveConfigEvidence, ResolvedForDoc, ResolvedValueDoc};
use serde_json::Value;
use std::collections::BTreeMap;

/// One recorded configuration value that differs between two runs.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigDifference {
    /// `app.<path>` for an application setting, else the knob's key path.
    pub key: String,
    /// Where a knob value applied, when not across the whole run.
    pub point: Option<String>,
    /// The compared run's value; `None` when it recorded none.
    pub recorded: Option<Value>,
    /// This run's value; `None` when it records none.
    pub current: Option<Value>,
}

/// Every recorded configuration value that differs, in key order. Invocation
/// knobs differ by construction and are skipped; a key the registry no
/// longer knows counts as configuration, as does every `[app]` key.
pub fn config_differences(
    recorded: &EffectiveConfigEvidence,
    current: &EffectiveConfigEvidence,
) -> Vec<ConfigDifference> {
    let mut by_point: BTreeMap<(String, Option<String>), ConfigDifference> = BTreeMap::new();
    for entry in diff(
        &configuration(&recorded.values),
        &configuration(&current.values),
    ) {
        let point = point(entry.resolved_for.as_ref(), &entry.scope);
        // A winning scope that moved under one effect point is one change.
        let difference = by_point
            .entry((entry.key_path.clone(), point.clone()))
            .or_insert_with(|| ConfigDifference {
                key: entry.key_path,
                point,
                recorded: None,
                current: None,
            });
        difference.recorded = difference.recorded.take().or(entry.base);
        difference.current = difference.current.take().or(entry.effective);
    }

    let recorded_app = app_leaves(recorded.app.as_ref());
    let current_app = app_leaves(current.app.as_ref());
    for key in recorded_app.keys().chain(current_app.keys()) {
        let (old, new) = (recorded_app.get(key), current_app.get(key));
        if old != new {
            by_point.insert(
                (key.clone(), None),
                ConfigDifference {
                    key: key.clone(),
                    point: None,
                    recorded: old.cloned(),
                    current: new.cloned(),
                },
            );
        }
    }
    by_point.into_values().collect()
}

fn configuration(docs: &[ResolvedValueDoc]) -> Vec<ResolvedValueDoc> {
    docs.iter()
        .filter(|doc| knob(&doc.key_path).is_none_or(|spec| spec.role == KnobRole::Configuration))
        .cloned()
        .collect()
}

fn point(resolved_for: Option<&ResolvedForDoc>, scope: &str) -> Option<String> {
    match resolved_for {
        Some(ResolvedForDoc::Effect { stage, effect_type }) => {
            Some(format!("stage:{stage} (effect:{effect_type})"))
        }
        None if scope == "global" => None,
        None => Some(scope.to_string()),
    }
}

/// Each leaf of the recorded settings under its `app.` path; arrays and
/// empty tables are leaves.
pub fn app_leaves(app: Option<&Value>) -> BTreeMap<String, Value> {
    fn walk(path: String, value: &Value, leaves: &mut BTreeMap<String, Value>) {
        match value {
            Value::Object(entries) if !entries.is_empty() => {
                for (key, value) in entries {
                    walk(format!("{path}.{}", toml_key(key)), value, leaves);
                }
            }
            leaf => {
                leaves.insert(path, leaf.clone());
            }
        }
    }

    let mut leaves = BTreeMap::new();
    if let Some(app) = app {
        walk("app".to_string(), app, &mut leaves);
    }
    leaves
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(key: &str, scope: &str, value: Value) -> ResolvedValueDoc {
        ResolvedValueDoc {
            key_path: key.to_string(),
            resolved_for: None,
            scope: scope.to_string(),
            source: "file".to_string(),
            winner_subject: None,
            value,
            redacted: false,
        }
    }

    fn evidence(values: Vec<ResolvedValueDoc>, app: Option<Value>) -> EffectiveConfigEvidence {
        EffectiveConfigEvidence::new(values).with_app(app)
    }

    #[test]
    fn launch_flags_differ_by_construction_and_are_never_reported() {
        let recorded = evidence(
            vec![row("runtime.max_lineage_depth", "global", json!(100))],
            Some(json!({"accounts_file": "accounts.yaml"})),
        );
        let resumed = evidence(
            vec![
                row("runtime.max_lineage_depth", "global", json!(100)),
                row("runtime.resume.resume_from", "global", json!("/runs/r0")),
                row(
                    "runtime.resume.allow_incomplete_archive",
                    "global",
                    json!(true),
                ),
            ],
            Some(json!({"accounts_file": "accounts.yaml"})),
        );
        assert_eq!(config_differences(&recorded, &resumed), []);
    }

    #[test]
    fn app_and_knob_changes_name_each_key_with_both_recorded_values() {
        let recorded = evidence(
            vec![row("runtime.max_lineage_depth", "global", json!(100))],
            Some(json!({
                "accounts_file": "accounts.yaml",
                "partner": {"api_key_env": "PARTNER_KEY", "tags": ["a"]},
            })),
        );
        let current = evidence(
            vec![
                row("runtime.max_lineage_depth", "global", json!(100)),
                row("runtime.max_lineage_depth", "stage:posted", json!(5)),
                row("runtime.retired_knob", "global", json!(1)),
            ],
            Some(json!({
                "accounts_file": "deploy/staging/accounts.yaml",
                "partner": {"api_key_env": "STAGING_PARTNER_KEY", "tags": ["a"]},
            })),
        );
        assert_eq!(
            config_differences(&recorded, &current),
            [
                ConfigDifference {
                    key: "app.accounts_file".into(),
                    point: None,
                    recorded: Some(json!("accounts.yaml")),
                    current: Some(json!("deploy/staging/accounts.yaml")),
                },
                ConfigDifference {
                    key: "app.partner.api_key_env".into(),
                    point: None,
                    recorded: Some(json!("PARTNER_KEY")),
                    current: Some(json!("STAGING_PARTNER_KEY")),
                },
                ConfigDifference {
                    key: "runtime.max_lineage_depth".into(),
                    point: Some("stage:posted".into()),
                    recorded: None,
                    current: Some(json!(5)),
                },
                ConfigDifference {
                    key: "runtime.retired_knob".into(),
                    point: None,
                    recorded: None,
                    current: Some(json!(1)),
                },
            ]
        );
    }

    #[test]
    fn settings_a_run_did_not_record_compare_as_absent() {
        let recorded = evidence(Vec::new(), None);
        let current = evidence(Vec::new(), Some(json!({"mode": "live", "odd key": {}})));
        let keys: Vec<(String, Option<Value>)> = config_differences(&recorded, &current)
            .into_iter()
            .map(|difference| (difference.key, difference.recorded))
            .collect();
        assert_eq!(
            keys,
            [
                ("app.\"odd key\"".to_string(), None),
                ("app.mode".to_string(), None)
            ]
        );
    }

    #[test]
    fn an_effect_point_whose_winning_scope_moved_is_one_change() {
        let effect = |scope: &str, value: u64| ResolvedValueDoc {
            resolved_for: Some(ResolvedForDoc::Effect {
                stage: "authorize".to_string(),
                effect_type: "payments.authorize".to_string(),
            }),
            ..row("middleware.retry.max_attempts", scope, json!(value))
        };
        let recorded = evidence(vec![effect("global", 3)], None);
        let current = evidence(vec![effect("stage:authorize", 5)], None);
        assert_eq!(
            config_differences(&recorded, &current),
            [ConfigDifference {
                key: "middleware.retry.max_attempts".into(),
                point: Some("stage:authorize (effect:payments.authorize)".into()),
                recorded: Some(json!(3)),
                current: Some(json!(5)),
            }]
        );
    }
}
