// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j honesty at unordered fan-ins, on real journals.
//!
//! A multi-inbound stage with no effectful descendant keeps availability
//! driven delivery (FLOWIP-095d leaves it unordered), so its journal order is
//! timing dependent and two correct runs can legitimately interleave
//! differently. The verifier must neither report that interleaving as
//! divergence nor fold the stage silently into the headline verdict: the
//! stage is named `not_order_certified`, per-type row counts are an advisory,
//! and the run exits 2.

use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome, MATCHED_LINE};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reading {
    channel: String,
    n: u64,
}

impl TypedPayload for Reading {
    const EVENT_TYPE: &'static str = "replay_verification_honesty.reading";
}

#[derive(Clone, Debug)]
struct Channel {
    name: &'static str,
    next: u64,
}

impl Channel {
    fn new(name: &'static str) -> Self {
        Self { name, next: 1 }
    }
}

impl TypedFiniteSourceHandler for Channel {
    type Output = Reading;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next > 3 {
            return Ok(None);
        }
        let n = self.next;
        self.next += 1;
        Ok(Some(vec![Reading {
            channel: self.name.to_string(),
            n,
        }]))
    }
}

/// A pure passthrough with no effectful descendant anywhere below it, so the
/// fan-in above stays availability-driven and unordered.
#[derive(Clone, Debug)]
struct PassthroughMerge;

impl TypedTransformHandler for PassthroughMerge {
    type Input = Reading;
    type Output = Reading;

    fn process(&self, reading: Reading) -> Result<Reading, HandlerError> {
        Ok(reading)
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

/// Two channels converge on a plain transform with no effectful descendant:
/// the merge keeps availability-driven scheduling and stays unordered.
fn build_flow(journal_base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let channel_a_handler = Channel::new("a");
        let channel_b_handler = Channel::new("b");
        let merge_handler = PassthroughMerge;
        let out_handler = SinkTyped::with_delivery(discard::<Reading>()).idempotent();

        Ok(flow! {
            name: "replay_verification_honesty",
            journals: disk_journals(journal_base),

            stages: {
                channel_a = source!(Reading => channel_a_handler);
                channel_b = source!(Reading => channel_b_handler);
                merge = transform!(Reading -> Reading => merge_handler);
                out = sink!(Reading => out_handler);
            },

            topology: {
                channel_a |> merge;
                channel_b |> merge;
                merge |> out;
            }
        })
    })
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
async fn unordered_fan_in_is_named_not_misreported() {
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
    assert_eq!(
        outcome.exit_code(),
        2,
        "an unordered fan-in carrying data rows must downgrade the verdict, never fake certainty"
    );
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("expected a completed comparison");
    };

    let merge = &report.stages["merge"];
    assert_eq!(merge.status, "not_order_certified");
    assert!(!merge.vacuous);
    assert_eq!(merge.blocking, vec!["merge".to_string()]);
    assert_eq!(
        merge.divergence_count, 0,
        "interleaving is never reported as divergence"
    );
    let counts = merge
        .counts_by_type
        .as_ref()
        .expect("uncertified stages carry the count advisory");
    assert_eq!(
        counts.baseline, counts.candidate,
        "per-type counts are order-insensitive and must agree"
    );

    // The sources upstream of the merge stay certified and matched.
    assert!(report.stages["channel_a"].order_certified);
    assert_eq!(report.stages["channel_a"].status, "matched");

    let rendered = obzenflow_infra::verify::render_verdict(&outcome);
    assert!(
        !rendered.contains(MATCHED_LINE),
        "the headline line must not print over an uncertified remainder: {rendered}"
    );
    assert!(
        rendered.contains("merge"),
        "the verdict names the uncertified stage: {rendered}"
    );
}
