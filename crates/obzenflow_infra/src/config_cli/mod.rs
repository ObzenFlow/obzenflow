// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Library half of `obzenflow config get` (FLOWIP-010 §9): offline
//! resolution, live-response parsing, and rendering. The binary owns
//! argument parsing and the HTTP call; parity with the HTTP surface holds
//! by construction because both sides speak `ResolvedValueDoc`.
//!
//! Offline coverage is honest and limited (gap 7): the file, environment,
//! and default tiers at GLOBAL scope only. The DSL tier and flow, stage,
//! and edge scopes exist only in a running flow, so `--effective` and
//! `--base` are identical offline, and `--overlay` is a live-only view.

use crate::application::config::{autodiscover_config, load_file_config, FlowConfig};
use crate::application::runtime_config_sources::build_runtime_config_snapshot;
use clap::Parser;
use obzenflow_core::config::ResolvedValueDoc;
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

/// Errors from the offline reader, rendered for the CLI user.
#[derive(Debug)]
pub struct ConfigCliError(String);

impl fmt::Display for ConfigCliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigCliError {}

/// Which config projection to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigView {
    Base,
    Effective,
    Overlay,
    Schema,
    Diff,
}

impl ConfigView {
    fn live_path(&self) -> &'static str {
        match self {
            ConfigView::Base => "/api/config",
            ConfigView::Effective => "/api/config/effective",
            ConfigView::Overlay => "/api/config/overlay",
            ConfigView::Schema => "/api/config/schema",
            ConfigView::Diff => "/api/config/diff",
        }
    }
}

/// The live route for a view, optionally narrowed to a flow or stage.
/// Narrowing implies the effective projection (the flow routes serve it).
pub fn live_route(view: ConfigView, flow: Option<&str>, stage: Option<&str>) -> String {
    match (flow, stage) {
        (Some(flow), Some(stage)) => format!("/api/config/flows/{flow}/stages/{stage}"),
        (Some(flow), None) => format!("/api/config/flows/{flow}"),
        _ => view.live_path().to_string(),
    }
}

/// Resolve the offline projection: file (explicit path or `obzenflow.toml`
/// autodiscovery) + canonical environment + defaults, at global scope.
pub fn offline_docs(
    view: ConfigView,
    config_path: Option<&Path>,
) -> Result<Vec<ResolvedValueDoc>, ConfigCliError> {
    match view {
        ConfigView::Base | ConfigView::Effective => {}
        ConfigView::Overlay | ConfigView::Diff => {
            return Err(ConfigCliError(
                "this view exists only on a running flow; add --url to query one".to_string(),
            ));
        }
        ConfigView::Schema => {
            // Schema is registry metadata; no sources needed. Callers use
            // `schema_json` instead, but keep the error honest here.
            return Err(ConfigCliError(
                "use the schema renderer (no source resolution applies)".to_string(),
            ));
        }
    }

    let discovered: Option<PathBuf> = match config_path {
        Some(path) => Some(path.to_path_buf()),
        None => autodiscover_config(true),
    };
    let raw =
        load_file_config(discovered.as_deref()).map_err(|err| ConfigCliError(err.to_string()))?;
    // Bootstrap CLI flags do not apply to the offline reader; defaults only.
    let cli = FlowConfig::parse_from(["obzenflow"]);
    let snapshot =
        build_runtime_config_snapshot(&cli, &raw).map_err(|err| ConfigCliError(err.to_string()))?;
    Ok(snapshot.global_view())
}

/// The registry schema as JSON (shared projection with `/api/config/schema`).
pub fn schema_json() -> serde_json::Value {
    serde_json::json!({
        "schema_version": obzenflow_core::config::EVIDENCE_SCHEMA_VERSION,
        "knobs": obzenflow_runtime::runtime_config::schema_view()
    })
}

/// A parsed live response: value docs plus the stage route's edge and effect
/// objects (empty for every other route).
#[derive(Debug, Default)]
pub struct LiveValues {
    pub schema_version: u32,
    pub values: Vec<ResolvedValueDoc>,
    pub edges: BTreeMap<String, Vec<ResolvedValueDoc>>,
    pub effects: BTreeMap<String, Vec<ResolvedValueDoc>>,
}

/// Parse a live `/api/config*` response body into the shared doc shapes.
pub fn parse_live_values(body: &str) -> Result<LiveValues, String> {
    let parsed: serde_json::Value =
        serde_json::from_str(body).map_err(|err| format!("response is not JSON: {err}"))?;
    let schema_version = parsed
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1) as u32;
    let values = parsed
        .get("values")
        .cloned()
        .map(serde_json::from_value::<Vec<ResolvedValueDoc>>)
        .transpose()
        .map_err(|err| format!("response values do not match the doc shape: {err}"))?
        .unwrap_or_default();
    let edges = parsed
        .get("edges")
        .cloned()
        .map(serde_json::from_value::<BTreeMap<String, Vec<ResolvedValueDoc>>>)
        .transpose()
        .map_err(|err| format!("response edges do not match the doc shape: {err}"))?
        .unwrap_or_default();
    let effects = parsed
        .get("effects")
        .cloned()
        .map(serde_json::from_value::<BTreeMap<String, Vec<ResolvedValueDoc>>>)
        .transpose()
        .map_err(|err| format!("response effects do not match the doc shape: {err}"))?
        .unwrap_or_default();
    Ok(LiveValues {
        schema_version,
        values,
        edges,
        effects,
    })
}

/// Render docs with provenance and an unambiguous protected-unit point.
pub fn render_table(docs: &[ResolvedValueDoc]) -> String {
    let headers = ["KEY", "VALUE", "SOURCE", "SCOPE", "SUBJECT", "RESOLVED FOR"];
    let rows: Vec<[String; 6]> = docs
        .iter()
        .map(|doc| {
            [
                doc.key_path.clone(),
                doc.value.to_string(),
                doc.source.clone(),
                doc.scope.clone(),
                match doc.winning_subject() {
                    obzenflow_core::config::ConfigSubject::Unqualified => "unqualified".to_string(),
                    obzenflow_core::config::ConfigSubject::Effect { effect_type } => {
                        format!("effect:{}", effect_type.as_str())
                    }
                },
                doc.resolved_for
                    .as_ref()
                    .map(|point| match point {
                        obzenflow_core::config::ResolvedForDoc::Effect { stage, effect_type } => {
                            format!("effect:{stage}/{effect_type}")
                        }
                    })
                    .unwrap_or_else(|| "-".to_string()),
            ]
        })
        .collect();

    let mut widths = headers.map(str::len);
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row.iter()) {
            *width = (*width).max(cell.len());
        }
    }

    let mut out = String::new();
    let render_row = |cells: [&str; 6]| -> String {
        let mut line = String::new();
        for (i, (cell, width)) in cells.iter().zip(widths.iter()).enumerate() {
            if i > 0 {
                line.push_str("  ");
            }
            line.push_str(&format!("{cell:<width$}"));
        }
        line.trim_end().to_string()
    };
    out.push_str(&render_row(headers));
    out.push('\n');
    for row in &rows {
        out.push_str(&render_row([
            row[0].as_str(),
            row[1].as_str(),
            row[2].as_str(),
            row[3].as_str(),
            row[4].as_str(),
            row[5].as_str(),
        ]));
        out.push('\n');
    }
    out
}

/// Render a live response: the values table plus one table per edge and
/// protected-effect group.
pub fn render_live(live: &LiveValues) -> String {
    let mut out = render_table(&live.values);
    for (edge, docs) in &live.edges {
        out.push('\n');
        out.push_str(&format!("edge {edge}\n"));
        out.push_str(&render_table(docs));
    }
    for (effect, docs) in &live.effects {
        out.push('\n');
        out.push_str(&format!("effect {effect}\n"));
        out.push_str(&render_table(docs));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{env_lock, EnvGuard};

    /// Offline/live parity on the shared subset (the documented gap-7
    /// limit): the offline reader and the HTTP base route both project
    /// `global_view()` of a snapshot built from the same file and
    /// environment, so their doc sets agree at global scope. Live may
    /// additionally carry dsl-sourced and scoped entries; offline never
    /// does.
    #[test]
    fn offline_matches_live_base_projection_on_the_global_subset() {
        let _lock = env_lock();
        let _guard = EnvGuard::new(&["OBZENFLOW_RUNTIME_MAX_LINEAGE_DEPTH"]);
        std::env::set_var("OBZENFLOW_RUNTIME_MAX_LINEAGE_DEPTH", "17");

        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("obzenflow.toml");
        std::fs::write(&config_path, "[runtime]\ncycle_max_iterations = 9\n")
            .expect("write config");

        let offline = offline_docs(ConfigView::Base, Some(&config_path)).expect("offline resolves");

        // The live base route serves the same projection: build the same
        // snapshot and render it through the HTTP body shape, then parse it
        // back with the CLI parser.
        let raw = load_file_config(Some(&config_path)).expect("file parses");
        let cli = FlowConfig::parse_from(["obzenflow"]);
        let snapshot = build_runtime_config_snapshot(&cli, &raw).expect("snapshot builds");
        let body = serde_json::json!({
            "flow": "f",
            "flow_id": "f-1",
            "values": snapshot.global_view(),
        })
        .to_string();
        let live = parse_live_values(&body).expect("live body parses");

        assert_eq!(offline, live.values);
        let lineage = offline
            .iter()
            .find(|d| d.key_path == "runtime.max_lineage_depth")
            .expect("env-sourced doc present");
        assert_eq!(lineage.value, serde_json::json!(17));
        assert_eq!(lineage.source, "env");
        let cycles = offline
            .iter()
            .find(|d| d.key_path == "runtime.cycle_max_iterations")
            .expect("file-sourced doc present");
        assert_eq!(cycles.value, serde_json::json!(9));
        assert_eq!(cycles.source, "file");

        std::env::remove_var("OBZENFLOW_RUNTIME_MAX_LINEAGE_DEPTH");
    }

    #[test]
    fn overlay_and_diff_are_live_only_offline_errors() {
        let err = offline_docs(ConfigView::Overlay, None).expect_err("overlay is live-only");
        assert!(err.to_string().contains("--url"));
        let err = offline_docs(ConfigView::Diff, None).expect_err("diff is live-only");
        assert!(err.to_string().contains("--url"));
    }

    #[test]
    fn live_route_narrows_to_flow_and_stage() {
        assert_eq!(live_route(ConfigView::Base, None, None), "/api/config");
        assert_eq!(
            live_route(ConfigView::Effective, Some("f1"), None),
            "/api/config/flows/f1"
        );
        assert_eq!(
            live_route(ConfigView::Effective, Some("f1"), Some("s1")),
            "/api/config/flows/f1/stages/s1"
        );
    }

    #[test]
    fn render_table_aligns_columns() {
        let docs = vec![ResolvedValueDoc {
            key_path: "runtime.max_lineage_depth".to_string(),
            scope: "global".to_string(),
            source: "default".to_string(),
            value: serde_json::json!(100),
            redacted: false,
            resolved_for: None,
            winner_subject: None,
        }];
        let table = render_table(&docs);
        let mut lines = table.lines();
        assert!(lines.next().unwrap().starts_with("KEY"));
        let row: Vec<&str> = lines.next().unwrap().split_whitespace().collect();
        assert_eq!(
            row,
            vec![
                "runtime.max_lineage_depth",
                "100",
                "default",
                "global",
                "unqualified",
                "-"
            ]
        );
    }

    #[test]
    fn version_two_effect_rows_parse_and_render_without_losing_identity() {
        let body = serde_json::json!({
            "schema_version": 2,
            "stage": "authorize_payment",
            "values": [],
            "effects": {
                "payments.authorize": [{
                    "key_path": "effects.rate_limiter.events_per_second",
                    "resolved_for": {
                        "kind": "effect",
                        "stage": "authorize_payment",
                        "effect_type": "payments.authorize"
                    },
                    "scope": "stage:authorize_payment",
                    "source": "file",
                    "winner_subject": {
                        "kind": "unqualified"
                    },
                    "value": 8.0
                }],
                "payments.refund": [{
                    "key_path": "effects.rate_limiter.events_per_second",
                    "resolved_for": {
                        "kind": "effect",
                        "stage": "authorize_payment",
                        "effect_type": "payments.refund"
                    },
                    "scope": "stage:authorize_payment",
                    "source": "file",
                    "winner_subject": {
                        "kind": "unqualified"
                    },
                    "value": 8.0
                }]
            }
        })
        .to_string();

        let live = parse_live_values(&body).expect("schema-v2 response parses");
        assert_eq!(live.schema_version, 2);
        assert_eq!(live.effects.len(), 2);
        let rendered = render_live(&live);
        assert!(rendered.contains("effect payments.authorize"));
        assert!(rendered.contains("effect payments.refund"));
        assert!(rendered.contains("effect:authorize_payment/payments.authorize"));
        assert!(rendered.contains("effect:authorize_payment/payments.refund"));
    }

    #[test]
    fn version_one_live_rows_default_to_unqualified_subjects() {
        let body = r#"{
            "values": [{
                "key_path": "runtime.max_lineage_depth",
                "scope": "global",
                "source": "file",
                "value": 7
            }]
        }"#;
        let live = parse_live_values(body).expect("version-one response parses");
        assert_eq!(live.schema_version, 1);
        assert_eq!(
            live.values[0].winning_subject(),
            obzenflow_core::config::ConfigSubject::Unqualified
        );
        assert!(render_live(&live).contains("unqualified"));
    }
}
