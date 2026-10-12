// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-010t: a build extracts the config file's `[app]` table at most once
//! and records it, and a non-empty table that no build extracts refuses
//! before any run directory, manifest or continuation claim exists (B10).

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::dsl::FlowBuildError;
use obzenflow_dsl::{flow, infinite_source, sink, source, FlowBuildFailure, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::journal::read::{read_run_lineage, Standing};
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{
    AppTable, ApplicationSettingsError, ResolvedRuntimeConfig,
};
use obzenflow_runtime::stages::common::handlers::TypedInfiniteSourceHandler;
use obzenflow_runtime::stages::SourceError;
use replay_testkit::{Discard, ReattachingSource};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TestEvent;

impl TypedPayload for TestEvent {
    const EVENT_TYPE: &'static str = "test.app_settings";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    accounts_file: PathBuf,
}

fn context_with(app: serde_json::Value) -> FlowBuildContext {
    let serde_json::Value::Object(table) = app else {
        panic!("[app] is a table");
    };
    FlowBuildContext::new(Arc::new(
        ResolvedRuntimeConfig::builtin_defaults()
            .with_app(AppTable::new("deploy/staging.toml", table)),
    ))
}

fn bare_flow(base: PathBuf) -> FlowDefinition {
    flow! {
        name: "app_settings",
        journals: disk_journals(base),

        stages: {
            src = source!(TestEvent => placeholder!());
            snk = sink!(TestEvent => placeholder!());
        },

        topology: {
            src |> snk;
        }
    }
}

fn materialized_flow(base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_| Ok(bare_flow(base)))
}

fn extracting_flow(base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize_with(move |_, settings: Settings| {
        assert_eq!(settings.accounts_file, Path::new("accounts.yaml"));
        Ok(bare_flow(base))
    })
}

async fn build_failure(flow: FlowDefinition, ctx: FlowBuildContext) -> FlowBuildFailure {
    match flow.build(ctx).await {
        Ok(_) => panic!("the build must refuse"),
        Err(failure) => failure,
    }
}

async fn run_to_completion(handle: FlowHandle) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), handle.run())
        .await
        .map_err(|_| anyhow!("the empty finite flow must complete promptly"))??;
    Ok(())
}

const REFUSAL: &str = "app: deploy/staging.toml sets accounts_file, but this flow extracts no \
                       application settings; remove [app] or extract it with \
                       FlowDefinition::materialize_with";

#[tokio::test]
async fn an_unextracted_app_table_refuses_before_any_run_directory() {
    for (label, flow) in [
        ("bare flow!", bare_flow as fn(PathBuf) -> FlowDefinition),
        ("materialize", materialized_flow),
    ] {
        let base = tempfile::tempdir().expect("tempdir");
        let failure = build_failure(
            flow(base.path().to_path_buf()),
            context_with(json!({"accounts_file": "accounts.yaml"})),
        )
        .await;
        assert!(
            matches!(
                failure.error,
                FlowBuildError::ApplicationSettings(ApplicationSettingsError::Unextracted { .. })
            ),
            "{label}: {failure:?}"
        );
        assert_eq!(failure.to_string(), REFUSAL, "{label}");
        assert!(failure.run.is_none(), "{label}");
        assert!(
            !base.path().join("flows").exists(),
            "{label}: no run directory exists"
        );
    }
}

#[tokio::test]
async fn an_absent_or_empty_table_builds_without_extraction() {
    for ctx in [FlowBuildContext::for_tests(), context_with(json!({}))] {
        let base = tempfile::tempdir().expect("tempdir");
        let handle = bare_flow(base.path().to_path_buf())
            .build(ctx)
            .await
            .expect("an absent or empty table builds");
        run_to_completion(handle).await.expect("the flow completes");
        let manifest =
            replay_testkit::archive_manifest(&replay_testkit::latest_run_dir(base.path()));
        assert!(manifest["effective_config"].get("app").is_none());
    }
}

#[tokio::test]
async fn an_extracted_table_is_recorded_in_the_run_manifest() {
    let base = tempfile::tempdir().expect("tempdir");
    let handle = extracting_flow(base.path().to_path_buf())
        .build(context_with(json!({"accounts_file": "accounts.yaml"})))
        .await
        .expect("an extracted table builds");
    run_to_completion(handle).await.expect("the flow completes");
    let manifest = replay_testkit::archive_manifest(&replay_testkit::latest_run_dir(base.path()));
    assert_eq!(
        manifest["effective_config"]["app"],
        json!({"accounts_file": "accounts.yaml"})
    );
}

/// Emits `count` events, then idles, so a stopped run can be resumed.
#[derive(Clone, Debug)]
struct Bounded(u64);

impl TypedInfiniteSourceHandler for Bounded {
    type Output = TestEvent;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        if self.0 == 0 {
            return Ok(Vec::new());
        }
        self.0 -= 1;
        Ok(vec![TestEvent])
    }
}

fn resumable_flow(base: PathBuf, events: u64) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let reader = ReattachingSource(Bounded(events));
        let discard = Discard::<TestEvent>::default();
        Ok(flow! {
            name: "app_settings_resume",
            journals: disk_journals(base),

            stages: {
                src = infinite_source!(TestEvent => reader);
                snk = sink!(TestEvent => discard);
            },

            topology: {
                src |> snk;
            }
        })
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resume_refused_for_an_unextracted_table_leaves_its_parent_unclaimed() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let base = temp.path().join("journals");
    let handle = resumable_flow(base.clone(), 2)
        .build(FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow!("the recording must build: {e:?}"))?;
    let mut state = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !matches!(*state.borrow(), PipelineState::Running) {
            state.changed().await.expect("pipeline state channel");
        }
    })
    .await?;
    handle.stop().await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion()).await??;
    // The journal writers' locks mark a live run until the handle is gone.
    drop(handle);
    let r0 = replay_testkit::latest_run_dir(&base);
    let runs = std::fs::read_dir(base.join("flows"))?.count();

    let failure = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: r0.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        build_failure(
            resumable_flow(base.clone(), 0),
            context_with(json!({"accounts_file": "accounts.yaml"})),
        )
        .await
    };
    assert_eq!(failure.to_string(), REFUSAL);
    assert!(failure.run.is_none());
    assert_eq!(std::fs::read_dir(base.join("flows"))?.count(), runs);
    assert_eq!(read_run_lineage(&r0)?.standing, Standing::Head);
    Ok(())
}
