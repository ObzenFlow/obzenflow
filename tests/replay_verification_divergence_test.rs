// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j divergence semantics on real journals.
//!
//! Within the certified region a divergence is signal, never noise: the only
//! ways a replay can differ are user nondeterminism outside the effect
//! boundary, changed code, or a runtime bug. The first fixture smuggles the
//! wall clock into a pure handler's emitted payload, and verification must
//! name the exact stage, journal position, and field, and exit 1. The second
//! check compares two independent live runs of a deterministic flow: they
//! share no replay lineage, so identity is excluded and the comparison runs
//! under the positional projection.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::effects::Effects;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{EffectfulTransformHandler, FiniteSourceHandler};
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    id: u64,
}

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "replay_verification_divergence.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Stamped {
    id: u64,
    stamp: u128,
}

impl TypedPayload for Stamped {
    const EVENT_TYPE: &'static str = "replay_verification_divergence.stamped";
}

#[derive(Clone, Debug)]
struct Numbers {
    next: u64,
    writer_id: WriterId,
}

impl Numbers {
    fn new() -> Self {
        Self {
            next: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for Numbers {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next > 3 {
            return Ok(None);
        }
        let id = self.next;
        self.next += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            Input::EVENT_TYPE,
            json!(Input { id }),
        )]))
    }
}

/// A pure handler that either derives its output deterministically or, in
/// the seeded-nondeterminism variant, reads the wall clock outside any
/// effect, the exact mistake the verifier exists to catch.
#[derive(Clone, Debug)]
struct Stamp {
    nondeterministic: bool,
}

#[async_trait]
impl EffectfulTransformHandler for Stamp {
    type Input = Input;

    async fn process(&self, input: Input, fx: &mut Effects) -> Result<(), HandlerError> {
        let stamp = if self.nondeterministic {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        } else {
            u128::from(input.id) * 10
        };
        fx.emit(Stamped {
            id: input.id,
            stamp,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "replay-verification-stamp-v1"
    }
}

/// The diff workflow: same stage, deliberately changed logic with a bumped
/// stage logic version, replayed against the v1 recording.
#[derive(Clone, Debug)]
struct StampV2;

#[async_trait]
impl EffectfulTransformHandler for StampV2 {
    type Input = Input;

    async fn process(&self, input: Input, fx: &mut Effects) -> Result<(), HandlerError> {
        fx.emit(Stamped {
            id: input.id,
            stamp: u128::from(input.id) * 1000,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "replay-verification-stamp-v2"
    }
}

fn discard<T>(
) -> impl FnMut(T, obzenflow_runtime::stages::sink::DeliveryContext) -> std::future::Ready<()>
       + Send
       + Sync
       + Clone
where
    T: Clone + Send + Sync + 'static,
{
    move |_payload: T, _delivery| std::future::ready(())
}

fn build_flow(journal_base: PathBuf, nondeterministic: bool) -> FlowDefinition {
    flow! {
        name: "replay_verification_divergence",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            numbers = source!(Input => Numbers::new());
            stamp = effectful_transform!(
                Input -> { Stamped } => Stamp { nondeterministic },
                effects: [],
                middleware: []
            );
            out = sink!(Stamped => SinkTyped::with_delivery(discard::<Stamped>()));
        },

        topology: {
            numbers |> stamp;
            stamp |> out;
        }
    }
}

fn build_flow_v2(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "replay_verification_divergence",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            numbers = source!(Input => Numbers::new());
            stamp = effectful_transform!(
                Input -> { Stamped } => StampV2,
                effects: [],
                middleware: []
            );
            out = sink!(Stamped => SinkTyped::with_delivery(discard::<Stamped>()));
        },

        topology: {
            numbers |> stamp;
            stamp |> out;
        }
    }
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries.pop().expect("run should have produced an archive")
}

async fn run_flow(journal_base: &Path, nondeterministic: bool, replay_from: Option<&Path>) {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_flow(journal_base.to_path_buf(), nondeterministic))
        .await
        .expect("flow should complete");
}

#[tokio::test(flavor = "multi_thread")]
async fn wall_clock_outside_the_effect_boundary_diverges_with_exact_coordinates() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    run_flow(&journal_base, true, None).await;
    let baseline = latest_run_dir(&journal_base);
    run_flow(&journal_base, true, Some(&baseline)).await;
    let candidate = latest_run_dir(&journal_base);

    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    assert_eq!(outcome.exit_code(), 1, "wall-clock payloads must diverge");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("divergence is a completed comparison, never a refusal");
    };

    let stage = &report.stages["stamp"];
    assert_eq!(stage.status, "diverged");
    let first = &stage.divergences[0];
    assert_eq!(first.journal, "data");
    assert_eq!(first.position, 0, "the first stamped row already differs");
    assert_eq!(first.field, "payload");
    assert_eq!(
        first.classification, "unexpected",
        "same stage logic version, so the divergence is not the diff workflow"
    );
    assert_eq!(
        report.stages["numbers"].status, "matched",
        "the re-injected source rows are identical; only the handler smuggled nondeterminism"
    );
}

/// The supported diff workflow: replay recorded inputs through changed code.
/// Verification reports the differences (that is the point of the workflow)
/// and the bumped stage logic version classifies them as `changed-code`.
#[tokio::test(flavor = "multi_thread")]
async fn changed_code_replay_diverges_and_is_classified_changed_code() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    run_flow(&journal_base, false, None).await;
    let baseline = latest_run_dir(&journal_base);

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            baseline.as_os_str().to_os_string(),
        ])
        .run_async(build_flow_v2(journal_base.clone()))
        .await
        .expect("changed-code replay should complete");
    let candidate = latest_run_dir(&journal_base);

    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    assert_eq!(outcome.exit_code(), 1, "the diff workflow reports its diff");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("expected a completed comparison");
    };
    let stage = &report.stages["stamp"];
    assert!(stage.logic_version_changed);
    assert_eq!(stage.divergences[0].field, "payload");
    assert_eq!(
        stage.divergences[0].classification, "changed-code",
        "a bumped stage logic version triages the divergence as the diff workflow"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn independent_live_runs_compare_under_the_positional_projection() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    run_flow(&journal_base, false, None).await;
    let first = latest_run_dir(&journal_base);
    run_flow(&journal_base, false, None).await;
    let second = latest_run_dir(&journal_base);
    assert_ne!(first, second);

    let outcome = verify_run_dirs(&first, &second, &VerifyOptions::default())
        .expect("verification should run");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("expected a completed comparison");
    };
    assert_eq!(
        report.identity_mode, "positional",
        "no replay lineage is asserted between independent live runs"
    );
    assert_eq!(outcome.exit_code(), 0, "deterministic flow, equal payloads");
}
