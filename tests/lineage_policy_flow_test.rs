// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-010 §7: the lineage-depth policy is build-resolved configuration
//! threaded as data into the deriving strategies, never an ambient env read.
//! A file-sourced `runtime.max_lineage_depth = 3` must cap `parent_ids` on
//! every derived event in the journal, and the run manifest must record the
//! value with file provenance, so the run directory alone answers both "what
//! was configured" and "did the data path obey it".

mod replay_testkit;

use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::transform::strategies::MapTyped;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Item {
    index: u64,
}

impl TypedPayload for Item {
    const EVENT_TYPE: &'static str = "lineage_policy_flow.item";
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

/// Five deriving hops (src event -> t1 -> t2 -> t3 -> t4). Uncapped, t4's
/// output would carry four parent ids; the policy caps it at three.
#[tokio::test]
async fn file_configured_lineage_depth_caps_journalled_parent_ids() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = dir.path().to_path_buf();

    let mut candidates = CandidateSet::default();
    candidates
        .admit(ScopedCandidate {
            key_path: "runtime.max_lineage_depth".to_string(),
            scope: obzenflow_core::config::ConfigScope::Global,
            source: obzenflow_core::config::ConfigSource::File,
            value: ConfigValue::U64(3),
        })
        .expect("global candidate admits");
    let snapshot = Arc::new(ResolvedRuntimeConfig::new(candidates));

    let journal_base = base.clone();
    let handle = flow! {
        name: "lineage_policy_flow",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            src = source!(Item => OneShotSource { emitted: false, writer_id: WriterId::from(StageId::new()) });
            t1 = transform!(Item -> Item => MapTyped::new(|i: Item| Item { index: i.index + 1 }));
            t2 = transform!(Item -> Item => MapTyped::new(|i: Item| Item { index: i.index + 1 }));
            t3 = transform!(Item -> Item => MapTyped::new(|i: Item| Item { index: i.index + 1 }));
            t4 = transform!(Item -> Item => MapTyped::new(|i: Item| Item { index: i.index + 1 }));
            snk = sink!(Item => NullSink);
        },

        topology: {
            src |> t1;
            t1 |> t2;
            t2 |> t3;
            t3 |> t4;
            t4 |> snk;
        }
    }
    .build(FlowBuildContext::new(snapshot))
    .await
    .expect("flow must build");

    tokio::time::timeout(Duration::from_secs(20), handle.run())
        .await
        .expect("flow run timed out")
        .expect("flow run failed");

    // Journal-first verification: read the run's stage journals, not stdout.
    let run_dir = replay_testkit::latest_run_dir(&base);

    // The manifest records the file-sourced value (the §6a acceptance).
    let manifest = replay_testkit::archive_manifest(&run_dir);
    let evidence = manifest["effective_config"]["values"]
        .as_array()
        .expect("manifest must record effective config evidence");
    let lineage_doc = evidence
        .iter()
        .find(|d| d["key_path"] == "runtime.max_lineage_depth")
        .expect("lineage depth doc present");
    assert_eq!(lineage_doc["value"], json!(3));
    assert_eq!(lineage_doc["source"], "file");
    assert_eq!(lineage_doc["scope"], "global");

    // Every derived data event obeys the cap; the deepest hop proves the cap
    // engaged (uncapped it would carry four parent ids).
    let mut deepest_parent_count = 0usize;
    for stage in ["t1", "t2", "t3", "t4"] {
        let envelopes = replay_testkit::read_stage_envelopes(&run_dir, stage).await;
        for envelope in envelopes.iter().filter(|e| e.event.is_data()) {
            let parents = envelope.event.causality.parent_ids.len();
            assert!(
                parents <= 3,
                "stage {stage} derived event exceeds the configured cap: {parents} parents"
            );
            deepest_parent_count = deepest_parent_count.max(parents);
        }
    }
    assert_eq!(
        deepest_parent_count, 3,
        "the five-hop chain must reach the cap exactly (4 ancestors truncated to 3)"
    );
}
