// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j: the `obzenflow verify` subcommand's OS-level exit codes.
//!
//! The binary is a thin shell over `obzenflow_infra::verify`; this suite
//! asserts the process boundary: `0` certified match with the headline line
//! on stdout, `3` refusal for an unavailable archive. (Divergence and
//! uncertified verdicts are exercised in-process by the other verification
//! suites; the contract mapping itself is unit-tested in `verdict`.)

use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::FiniteSourceHandler;
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick {
    n: u64,
}

impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "cli_verify.tick";
}

#[derive(Clone, Debug)]
struct Ticks {
    next: u64,
    writer_id: WriterId,
}

impl Ticks {
    fn new() -> Self {
        Self {
            next: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for Ticks {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next > 3 {
            return Ok(None);
        }
        let n = self.next;
        self.next += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            Tick::EVENT_TYPE,
            json!(Tick { n }),
        )]))
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
        name: "cli_verify",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            ticks = source!(Tick => Ticks::new());
            out = sink!(Tick => SinkTyped::with_delivery(discard::<Tick>()));
        },

        topology: {
            ticks |> out;
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
async fn cli_verify_exit_codes_follow_the_contract() {
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

    // Certified match: exit 0 and the headline line on stdout.
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .args(["verify", "--baseline"])
        .arg(&baseline)
        .arg("--candidate")
        .arg(&candidate)
        .output()
        .expect("obzenflow binary should run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(0),
        "certified match exits 0: {stdout}"
    );
    assert!(
        stdout.contains("output matched the original run, 0 differences"),
        "the headline line prints on a certified match: {stdout}"
    );

    // Refusal: a missing baseline exits 3 with the reason on stdout.
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .args(["verify", "--baseline"])
        .arg(temp.path().join("no-such-run"))
        .arg("--candidate")
        .arg(&candidate)
        .output()
        .expect("obzenflow binary should run");
    assert_eq!(output.status.code(), Some(3), "refusals exit 3");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("verification refused"),
        "the refusal names its reason: {stdout}"
    );
}
