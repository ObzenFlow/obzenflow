// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j: error journals compare positionally on semantic content.
//!
//! A pure handler that fails deterministically routes error-marked events to
//! the stage's error journal. On strict replay the handler re-executes and
//! fails identically (semantic reasons are byte-stable per FLOWIP-120i), so
//! the error lane must verify with zero divergences. This fixture is what
//! freezes the projection's error-journal rule: it demonstrates the
//! deterministic error path carries no wall-clock-gated row class needing a
//! named exclusion. (Sink retry middleware lives on the delivery lane, whose
//! rows the projection already excludes.)

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

// Deliberately keyword-free event types for the error-lane assertions.
// marks any event whose type contains "error"/"failed"/"failure" as an
// error in live runs, and that live-only accounting is suppressed under
// strict replay, which is a real journal asymmetry outside this fixture's
// subject (see the suite doc comment).
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "replay_verification_rejects.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Accepted {
    id: u64,
}

impl TypedPayload for Accepted {
    const EVENT_TYPE: &'static str = "replay_verification_rejects.accepted";
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
        if self.next > 4 {
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

/// Odd ids fail with a deterministic semantic reason; even ids pass.
#[derive(Clone, Debug)]
struct RejectOdd;

#[async_trait]
impl EffectfulTransformHandler for RejectOdd {
    type Input = Input;

    async fn process(&self, input: Input, fx: &mut Effects) -> Result<(), HandlerError> {
        if input.id % 2 == 1 {
            return Err(HandlerError::Other(format!(
                "odd id rejected: {}",
                input.id
            )));
        }
        fx.emit(Accepted { id: input.id })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "replay-verification-reject-odd-v1"
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

fn build_flow(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "replay_verification_error_lane",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            numbers = source!(Input => Numbers::new());
            gate = effectful_transform!(
                Input -> { Accepted } => RejectOdd,
                effects: [],
                middleware: []
            );
            out = sink!(Accepted => SinkTyped::with_delivery(discard::<Accepted>()));
        },

        topology: {
            numbers |> gate;
            gate |> out;
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

#[tokio::test(flavor = "multi_thread")]
async fn deterministic_handler_failures_verify_on_the_error_lane() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(journal_base.clone()))
        .await
        .expect("live flow should complete");
    let baseline = latest_run_dir(&journal_base);

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            baseline.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(journal_base.clone()))
        .await
        .expect("replay flow should complete");
    let candidate = latest_run_dir(&journal_base);

    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("expected a completed comparison");
    };
    assert_eq!(
        outcome.exit_code(),
        0,
        "deterministic failures replay identically: {}\n{}",
        obzenflow_infra::verify::render_verdict(&outcome),
        serde_json::to_string_pretty(&report.stages["numbers"].divergences).unwrap_or_default()
    );

    // The gate's journals carried both lanes: two accepted facts on the data
    // journal and two error-marked rows on the error journal, all compared.
    let gate = &report.stages["gate"];
    assert_eq!(gate.status, "matched");
    assert!(
        gate.positional_rows_baseline >= 4,
        "both the accepted facts and the error rows enter the projection (got {})",
        gate.positional_rows_baseline
    );
    assert_eq!(
        gate.positional_rows_baseline,
        gate.positional_rows_candidate
    );
}
