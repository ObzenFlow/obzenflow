// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-010 §6a: the run manifest records the redacted effective config
//! with both provenance axes, so "what configuration was this run executed
//! under" is answerable from the run directory alone.

use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::journal::run_manifest::RunManifest;
use obzenflow_core::{TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::journal::RunSubstrateState;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, SinkHandler};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Item {
    index: u64,
}

impl TypedPayload for Item {
    const EVENT_TYPE: &'static str = "effective_config_manifest.item";
}

#[derive(Clone, Debug)]
struct OneShotSource {
    emitted: bool,
    writer_id: WriterId,
}

impl FiniteSourceHandler for OneShotSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                <Item as TypedPayload>::EVENT_TYPE,
                json!({ "index": 0 }),
            )]))
        }
    }
}

#[derive(Clone, Debug)]
struct NullSink;

#[async_trait]
impl SinkHandler for NullSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Null".to_string()),
            None,
        ))
    }
}

fn manifest_for(handle: &obzenflow_runtime::prelude::FlowHandle) -> RunManifest {
    let locator = match handle.run_substrate() {
        RunSubstrateState::Durable(locator) => locator.clone(),
        RunSubstrateState::Ephemeral => panic!("disk flow must report Durable"),
    };
    let raw = std::fs::read_to_string(locator.path().join("run_manifest.json"))
        .expect("run_manifest.json should be readable");
    serde_json::from_str(&raw).expect("run_manifest.json should parse")
}

fn build_flow_future(
    base: std::path::PathBuf,
    ctx: FlowBuildContext,
) -> impl std::future::Future<
    Output = Result<obzenflow_runtime::prelude::FlowHandle, obzenflow_dsl::dsl::FlowBuildFailure>,
> {
    flow! {
        name: "effective_config_manifest",
        journals: disk_journals(base),
        middleware: [],

        stages: {
            src = source!(Item => OneShotSource { emitted: false, writer_id: WriterId::from(StageId::new()) });
            snk = sink!(Item => NullSink);
        },

        topology: {
            src |> snk;
        }
    }
    .build(ctx)
}

#[tokio::test]
async fn manifest_records_file_sourced_values_with_both_provenance_axes() {
    let dir = tempfile::tempdir().expect("tempdir");

    // A file-sourced global value plus a stage-scoped override for `snk`.
    let mut candidates = CandidateSet::default();
    candidates
        .admit(ScopedCandidate {
            key_path: "runtime.max_lineage_depth".to_string(),
            scope: obzenflow_core::config::ConfigScope::Global,
            source: obzenflow_core::config::ConfigSource::File,
            value: ConfigValue::U64(7),
        })
        .expect("global candidate admits");
    candidates
        .admit(ScopedCandidate {
            key_path: "runtime.max_lineage_depth".to_string(),
            scope: obzenflow_core::config::ConfigScope::stage("snk"),
            source: obzenflow_core::config::ConfigSource::File,
            value: ConfigValue::U64(5),
        })
        .expect("stage candidate admits");
    let snapshot = Arc::new(ResolvedRuntimeConfig::new(candidates));

    let handle = build_flow_future(dir.path().to_path_buf(), FlowBuildContext::new(snapshot))
        .await
        .expect("flow must build");

    let manifest = manifest_for(&handle);
    let evidence = manifest
        .effective_config
        .expect("manifest must record effective config evidence");
    assert_eq!(evidence.schema_version, 1);

    let lineage: Vec<_> = evidence
        .values
        .iter()
        .filter(|d| d.key_path == "runtime.max_lineage_depth")
        .collect();
    assert_eq!(lineage.len(), 2, "global value plus the stage override");
    assert_eq!(lineage[0].scope, "global");
    assert_eq!(lineage[0].source, "file");
    assert_eq!(lineage[0].value, json!(7));
    assert_eq!(lineage[1].scope, "stage:snk");
    assert_eq!(lineage[1].source, "file");
    assert_eq!(lineage[1].value, json!(5));

    // Defaults for untouched knobs are recorded too, with default provenance.
    let heartbeat = evidence
        .values
        .iter()
        .find(|d| d.key_path == "runtime.heartbeat_interval")
        .expect("defaulted knobs appear in the evidence");
    assert_eq!(heartbeat.source, "default");
    assert_eq!(heartbeat.scope, "global");
}

#[tokio::test]
async fn manifest_records_default_provenance_for_a_hostless_build() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = build_flow_future(dir.path().to_path_buf(), FlowBuildContext::for_tests())
        .await
        .expect("flow must build");

    let manifest = manifest_for(&handle);
    let evidence = manifest
        .effective_config
        .expect("manifest must record effective config evidence");
    let lineage = evidence
        .values
        .iter()
        .find(|d| d.key_path == "runtime.max_lineage_depth")
        .expect("lineage depth is a defaulted knob");
    assert_eq!(lineage.value, json!(100));
    assert_eq!(lineage.source, "default");
    assert_eq!(lineage.scope, "global");
    assert_eq!(
        evidence
            .values
            .iter()
            .filter(|d| d.key_path == "runtime.max_lineage_depth")
            .count(),
        1,
        "identical per-stage resolutions collapse to one doc"
    );
}
