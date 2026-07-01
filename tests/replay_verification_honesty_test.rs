// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j/FLOWIP-095l honesty at unordered fan-ins, on real journals.
//!
//! A multi-inbound stage with no order-observing descendant keeps availability
//! driven delivery (FLOWIP-095d leaves it unordered), so its journal order is
//! timing dependent and two correct runs can legitimately interleave
//! differently. The verifier must neither report that interleaving as
//! divergence nor fake a positional certificate: it recognises the region as
//! order-insensitive (the build's `OrderInsensitive` verdict) and certifies it
//! by content, an order-insensitive multiset. A legitimate reordering is a
//! clean match; a content difference would still diverge. The run exits 0.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome, MATCHED_LINE};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, TransformHandler};
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
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
    writer_id: WriterId,
}

impl Channel {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            next: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for Channel {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next > 3 {
            return Ok(None);
        }
        let n = self.next;
        self.next += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            Reading::EVENT_TYPE,
            json!(Reading {
                channel: self.name.to_string(),
                n
            }),
        )]))
    }
}

/// A pure passthrough with no effectful descendant anywhere below it, so the
/// fan-in above stays availability-driven and unordered.
#[derive(Clone, Debug)]
struct PassthroughMerge;

#[async_trait]
impl TransformHandler for PassthroughMerge {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
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
    flow! {
        name: "replay_verification_honesty",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            channel_a = source!(Reading => Channel::new("a"));
            channel_b = source!(Reading => Channel::new("b"));
            merge = transform!(Reading -> Reading => PassthroughMerge);
            out = sink!(Reading => SinkTyped::with_delivery(discard::<Reading>()));
        },

        topology: {
            channel_a |> merge;
            channel_b |> merge;
            merge |> out;
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
async fn unordered_fan_in_certifies_by_content() {
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
        0,
        "an order-insensitive fan-in with reproducible content certifies by content (FLOWIP-095l Gap 1)"
    );
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("expected a completed comparison");
    };

    // The merge is order-insensitive: it is certified by content (an
    // order-insensitive multiset), so a legitimate reordering is a match, never a
    // divergence and never a faked positional certificate.
    let merge = &report.stages["merge"];
    assert_eq!(merge.status, "matched");
    assert_eq!(merge.certification, "content");
    assert!(merge.order_certified);
    assert_eq!(
        merge.divergence_count, 0,
        "interleaving is absorbed by the multiset, never reported as divergence"
    );

    // The sources upstream of the merge stay positionally certified and matched.
    assert!(report.stages["channel_a"].order_certified);
    assert_eq!(report.stages["channel_a"].status, "matched");
    assert_eq!(report.stages["channel_a"].certification, "positional");

    // A fully certified match now prints the headline.
    let rendered = obzenflow_infra::verify::render_verdict(&outcome);
    assert!(
        rendered.contains(MATCHED_LINE),
        "a content-certified order-insensitive region is a clean match: {rendered}"
    );
}
