// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Read-only configuration introspection endpoints (FLOWIP-010 Part 2).
//!
//! Seven `/api/config/*` routes serve the resolved snapshot and the
//! build-materialized per-flow view with full provenance (source AND
//! winning scope, gap 5). All DTO construction delegates to the snapshot
//! and flow-view projections so HTTP, the CLI live target, and the run
//! manifest serve one projection. Redaction is schema-driven (§13):
//! resolved secret values redact, reference names stay visible. The
//! overlay and diff routes are truthfully empty in this slice (gap 12;
//! FLOWIP-010b owns mutation).

use async_trait::async_trait;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, WebError};
use obzenflow_core::StageKey;
use obzenflow_runtime::runtime_config::{
    diff, schema_view, FlowEffectiveConfig, ResolvedRuntimeConfig,
};
use serde_json::json;
use std::sync::Arc;

/// Shared read model for the config routes: the startup snapshot, the
/// build-materialized flow view when a flow is running, and the flow
/// identity for route validation.
pub struct ConfigReadModel {
    snapshot: Arc<ResolvedRuntimeConfig>,
    flow_effective: Option<Arc<FlowEffectiveConfig>>,
    topology: Arc<obzenflow_topology::Topology>,
    flow_name: String,
    flow_id: String,
}

impl ConfigReadModel {
    pub fn new(
        snapshot: Arc<ResolvedRuntimeConfig>,
        flow_effective: Option<Arc<FlowEffectiveConfig>>,
        topology: Arc<obzenflow_topology::Topology>,
        flow_name: String,
        flow_id: String,
    ) -> Self {
        Self {
            snapshot,
            flow_effective,
            topology,
            flow_name,
            flow_id,
        }
    }

    fn base(&self) -> serde_json::Value {
        json!({
            "schema_version": obzenflow_core::config::EVIDENCE_SCHEMA_VERSION,
            "flow": self.flow_name,
            "flow_id": self.flow_id,
            "values": self.snapshot.global_view(),
        })
    }

    fn overlay(&self) -> serde_json::Value {
        // Truthfully empty: no write path exists in this slice (gap 12).
        // Entries render in the gap-13 string wire forms.
        let values: Vec<serde_json::Value> = self
            .snapshot
            .overlay()
            .entries()
            .iter()
            .map(|entry| {
                json!({
                    "key_path": entry.key_path,
                    "scope": entry.scope.to_string(),
                    "value": entry.value.to_json(),
                })
            })
            .collect();
        json!({
            "schema_version": obzenflow_core::config::EVIDENCE_SCHEMA_VERSION,
            "flow": self.flow_name,
            "flow_id": self.flow_id,
            "values": values,
        })
    }

    fn effective(&self) -> serde_json::Value {
        let values = match &self.flow_effective {
            Some(effective) => effective.manifest_evidence().values,
            None => self.snapshot.global_view(),
        };
        json!({
            "schema_version": obzenflow_core::config::EVIDENCE_SCHEMA_VERSION,
            "flow": self.flow_name,
            "flow_id": self.flow_id,
            "values": values,
        })
    }

    fn schema(&self) -> serde_json::Value {
        json!({
            "schema_version": obzenflow_core::config::EVIDENCE_SCHEMA_VERSION,
            "knobs": schema_view()
        })
    }

    fn diff(&self) -> serde_json::Value {
        // Base versus overlay-applied effective: with the overlay empty the
        // result is truthfully empty; computed, not assumed (gap 12).
        let base = self.snapshot.global_view();
        json!({
            "schema_version": obzenflow_core::config::EVIDENCE_SCHEMA_VERSION,
            "flow": self.flow_name,
            "flow_id": self.flow_id,
            "entries": diff(&base, &base),
        })
    }

    /// `Err` carries a not-found message rendered as a 404 JSON body.
    fn flow(&self, flow_id: &str) -> Result<serde_json::Value, String> {
        self.validate_flow(flow_id)?;
        let Some(effective) = &self.flow_effective else {
            return Err(format!(
                "flow '{flow_id}' has no build-materialized configuration"
            ));
        };
        Ok(json!({
            "schema_version": obzenflow_core::config::EVIDENCE_SCHEMA_VERSION,
            "flow": self.flow_name,
            "flow_id": self.flow_id,
            "values": effective.manifest_evidence().values,
            "warnings": effective.warnings(),
        }))
    }

    /// `Err` carries a not-found message rendered as a 404 JSON body.
    fn stage(&self, flow_id: &str, stage_key: &str) -> Result<serde_json::Value, String> {
        self.validate_flow(flow_id)?;
        let Some(effective) = &self.flow_effective else {
            return Err(format!(
                "flow '{flow_id}' has no build-materialized configuration"
            ));
        };
        let known = self
            .topology
            .stages()
            .any(|stage_info| stage_info.name == stage_key);
        if !known {
            return Err(format!(
                "stage '{stage_key}' is not in flow '{}'",
                self.flow_name
            ));
        }
        let key = StageKey::from(stage_key);
        Ok(json!({
            "schema_version": obzenflow_core::config::EVIDENCE_SCHEMA_VERSION,
            "flow": self.flow_name,
            "flow_id": self.flow_id,
            "stage": stage_key,
            "values": effective.stage_docs(&key),
            "edges": effective.edge_docs_for_upstream(&key),
            "effects": effective.effect_docs_for_stage(&key),
        }))
    }

    fn validate_flow(&self, flow_id: &str) -> Result<(), String> {
        if flow_id == self.flow_id || flow_id == self.flow_name {
            Ok(())
        } else {
            Err(format!(
                "unknown flow '{flow_id}' (this runtime serves '{}' / {})",
                self.flow_name, self.flow_id
            ))
        }
    }
}

/// Which of the seven config routes one endpoint instance serves.
#[derive(Debug, Clone, Copy)]
pub enum ConfigRoute {
    Base,
    Overlay,
    Effective,
    Schema,
    Diff,
    Flow,
    Stage,
}

impl ConfigRoute {
    pub const ALL: [ConfigRoute; 7] = [
        ConfigRoute::Base,
        ConfigRoute::Overlay,
        ConfigRoute::Effective,
        ConfigRoute::Schema,
        ConfigRoute::Diff,
        ConfigRoute::Flow,
        ConfigRoute::Stage,
    ];

    fn path(&self) -> &'static str {
        match self {
            ConfigRoute::Base => "/api/config",
            ConfigRoute::Overlay => "/api/config/overlay",
            ConfigRoute::Effective => "/api/config/effective",
            ConfigRoute::Schema => "/api/config/schema",
            ConfigRoute::Diff => "/api/config/diff",
            ConfigRoute::Flow => "/api/config/flows/:flow_id",
            ConfigRoute::Stage => "/api/config/flows/:flow_id/stages/:stage_key",
        }
    }
}

/// One read-only config route bound to the shared read model.
pub struct ConfigHttpEndpoint {
    model: Arc<ConfigReadModel>,
    route: ConfigRoute,
}

impl ConfigHttpEndpoint {
    pub fn new(model: Arc<ConfigReadModel>, route: ConfigRoute) -> Self {
        Self { model, route }
    }
}

#[async_trait]
impl HttpEndpoint for ConfigHttpEndpoint {
    fn path(&self) -> &str {
        self.route.path()
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
        let missing_param = |name: &str| WebError::RequestHandlingFailed {
            message: format!("missing {name} path parameter"),
            source: None,
        };
        let outcome: Result<serde_json::Value, String> = match self.route {
            ConfigRoute::Base => Ok(self.model.base()),
            ConfigRoute::Overlay => Ok(self.model.overlay()),
            ConfigRoute::Effective => Ok(self.model.effective()),
            ConfigRoute::Schema => Ok(self.model.schema()),
            ConfigRoute::Diff => Ok(self.model.diff()),
            ConfigRoute::Flow => {
                let flow_id = request
                    .path_params
                    .get("flow_id")
                    .ok_or_else(|| missing_param("flow_id"))?;
                self.model.flow(flow_id)
            }
            ConfigRoute::Stage => {
                let flow_id = request
                    .path_params
                    .get("flow_id")
                    .ok_or_else(|| missing_param("flow_id"))?;
                let stage_key = request
                    .path_params
                    .get("stage_key")
                    .ok_or_else(|| missing_param("stage_key"))?;
                self.model.stage(flow_id, stage_key)
            }
        };

        let (mut response, body) = match outcome {
            Ok(body) => (Response::ok(), body),
            Err(message) => (Response::not_found(), json!({ "error": message })),
        };

        let json_body =
            serde_json::to_string(&body).map_err(|e| WebError::RequestHandlingFailed {
                message: format!("Failed to serialize config response: {e}"),
                source: None,
            })?;
        response
            .headers
            .insert("Content-Type".to_string(), "application/json".to_string());
        response.body = json_body.into_bytes();
        Ok(response.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::config::{ConfigScope, ConfigSource};
    use obzenflow_core::event::EffectType;
    use obzenflow_runtime::id_conversions::StageIdExt;
    use obzenflow_runtime::runtime_config::{
        materialize_flow_config, CandidateSet, ConfigValue, DslCandidates, FlowResolutionContext,
        ScopedCandidate, RATE_LIMITER_EVENTS_PER_SECOND_KEY,
    };
    use obzenflow_topology::TopologyBuilder;
    use std::collections::{BTreeMap, BTreeSet};

    fn model_with_file_value() -> ConfigReadModel {
        let mut candidates = CandidateSet::default();
        candidates
            .admit(ScopedCandidate::unqualified(
                "runtime.max_lineage_depth",
                ConfigScope::Global,
                ConfigSource::File,
                ConfigValue::U64(7),
            ))
            .expect("candidate admits");
        let snapshot = Arc::new(ResolvedRuntimeConfig::new(candidates));

        let mut builder = TopologyBuilder::new();
        let src = obzenflow_core::StageId::new().to_topology_id();
        let snk = obzenflow_core::StageId::new().to_topology_id();
        builder.add_stage_with_id(
            src,
            Some("src".to_string()),
            obzenflow_topology::StageType::FiniteSource,
        );
        builder.reset_current();
        builder.add_stage_with_id(
            snk,
            Some("snk".to_string()),
            obzenflow_topology::StageType::Sink,
        );
        builder.reset_current();
        builder.add_edge(src, snk);
        let topology = Arc::new(builder.build().expect("topology builds"));

        let mut dsl = DslCandidates::default();
        for effect_type in ["payments.authorize", "payments.refund"] {
            dsl.declare_for_effect(
                RATE_LIMITER_EVENTS_PER_SECOND_KEY,
                "src",
                effect_type,
                ConfigValue::F64(8.0),
            );
        }
        let ctx = FlowResolutionContext {
            flow_name: "config_http".to_string(),
            stages: BTreeSet::from([StageKey::from("src"), StageKey::from("snk")]),
            edges: BTreeSet::from([(StageKey::from("src"), StageKey::from("snk"))]),
            declared_effects: BTreeMap::from([(
                StageKey::from("src"),
                BTreeSet::from([
                    EffectType::from("payments.authorize"),
                    EffectType::from("payments.refund"),
                ]),
            )]),
            dsl,
        };
        let effective =
            Arc::new(materialize_flow_config(&snapshot, ctx).expect("flow config materializes"));

        ConfigReadModel::new(
            snapshot,
            Some(effective),
            topology,
            "config_http".to_string(),
            "flow-1".to_string(),
        )
    }

    #[test]
    fn base_serves_global_view_with_provenance() {
        let model = model_with_file_value();
        let body = model.base();
        let values = body["values"].as_array().unwrap();
        let lineage = values
            .iter()
            .find(|d| d["key_path"] == "runtime.max_lineage_depth")
            .expect("lineage doc present");
        assert_eq!(lineage["value"], json!(7));
        assert_eq!(lineage["source"], "file");
        assert_eq!(lineage["scope"], "global");
    }

    #[test]
    fn every_successful_config_projection_declares_evidence_schema_v2() {
        let model = model_with_file_value();
        let bodies = [
            model.base(),
            model.overlay(),
            model.effective(),
            model.schema(),
            model.diff(),
            model.flow("flow-1").unwrap(),
            model.stage("flow-1", "src").unwrap(),
        ];
        assert!(bodies.iter().all(|body| body["schema_version"] == json!(2)));
    }

    #[test]
    fn overlay_and_diff_are_truthfully_empty() {
        let model = model_with_file_value();
        assert_eq!(model.overlay()["values"], json!([]));
        assert_eq!(model.diff()["entries"], json!([]));
    }

    #[test]
    fn effective_serves_flow_view_docs() {
        let model = model_with_file_value();
        let body = model.effective();
        let values = body["values"].as_array().unwrap();
        assert!(values
            .iter()
            .any(|d| d["key_path"] == "runtime.max_lineage_depth" && d["value"] == json!(7)));
    }

    #[test]
    fn schema_serves_registry_metadata() {
        let model = model_with_file_value();
        let body = model.schema();
        let knobs = body["knobs"].as_array().unwrap();
        let lineage = knobs
            .iter()
            .find(|k| k["key_path"] == "runtime.max_lineage_depth")
            .expect("lineage knob in schema");
        assert_eq!(lineage["target"], "stage");
        assert_eq!(lineage["env"], "OBZENFLOW_RUNTIME_MAX_LINEAGE_DEPTH");
    }

    #[test]
    fn stage_route_serves_stage_edge_and_effect_projections() {
        let model = model_with_file_value();
        let body = model.stage("flow-1", "src").expect("stage view");
        assert_eq!(body["stage"], "src");
        let values = body["values"].as_array().unwrap();
        assert!(values
            .iter()
            .any(|d| d["key_path"] == "runtime.max_lineage_depth" && d["value"] == json!(7)));
        // The edges object exists (keyed `up|>down`); the backpressure knob
        // is OptionalAbsent so it is empty unless a source supplied a value.
        assert!(body["edges"].is_object());

        let effects = body["effects"].as_object().expect("effects object");
        assert_eq!(effects.len(), 2);
        for effect_type in ["payments.authorize", "payments.refund"] {
            let docs = effects[effect_type].as_array().expect("effect docs");
            let limiter = docs
                .iter()
                .find(|doc| doc["key_path"] == RATE_LIMITER_EVENTS_PER_SECOND_KEY)
                .expect("limiter row");
            assert_eq!(limiter["value"], json!(8.0));
            assert_eq!(
                limiter["resolved_for"],
                json!({
                    "kind": "effect",
                    "stage": "src",
                    "effect_type": effect_type,
                })
            );
            assert_eq!(limiter["winner_subject"]["kind"], "effect");
        }
    }

    #[test]
    fn unknown_flow_and_stage_return_not_found() {
        let model = model_with_file_value();
        assert!(model.flow("other").is_err());
        assert!(model.stage("flow-1", "ghost").is_err());
    }
}
