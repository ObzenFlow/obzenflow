// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095k: replay-exhaustion EOF fidelity on real journals.
//!
//! A cancelled archive committed no EOF, so its replay synthesizes a
//! `Truncated` terminal EOF, downstream finalization is suppressed (the
//! killed original never finalized), the fan-in authors the worst-wins join
//! of its inputs' kinds, and the replay-closed property holds: a replay of
//! the truncated replay reproduces the committed `Truncated` kind and
//! matches under whole-run verification. A `Completed` archive still replays
//! as `Natural` and finalizes.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{ChainEventContent, EventEnvelope};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, StatefulHandler};
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

mod replay_testkit;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick {
    n: u64,
}

impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "truncated_fidelity.tick";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SumResult {
    total: u64,
}

impl TypedPayload for SumResult {
    const EVENT_TYPE: &'static str = "truncated_fidelity.sum";
}

/// Emits `emit` ticks, then idles without sealing (the run only ends when the
/// test cancels it) or seals by returning `None` when `seal` is set.
#[derive(Clone, Debug)]
struct Ticks {
    next: u64,
    emit: u64,
    seal: bool,
    writer_id: WriterId,
}

impl Ticks {
    fn stalling(emit: u64) -> Self {
        Self {
            next: 1,
            emit,
            seal: false,
            writer_id: WriterId::from(StageId::new()),
        }
    }

    fn sealing(emit: u64) -> Self {
        Self {
            seal: true,
            ..Self::stalling(emit)
        }
    }
}

impl FiniteSourceHandler for Ticks {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next > self.emit {
            if self.seal {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_millis(10));
            return Ok(Some(vec![]));
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

#[derive(Debug, Clone)]
struct SumHandler {
    writer_id: WriterId,
}

impl SumHandler {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for SumHandler {
    type State = u64;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        *state += event.payload()["n"].as_u64().unwrap_or(0);
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            SumResult::EVENT_TYPE,
            json!(SumResult { total: *state }),
        )])
    }
}

fn counting<T>(
    counter: Arc<AtomicUsize>,
) -> impl FnMut(T, obzenflow_runtime::stages::sink::DeliveryContext) -> std::future::Ready<()>
       + Send
       + Sync
       + Clone
where
    T: Clone + Send + Sync + 'static,
{
    move |_payload: T, _delivery| {
        counter.fetch_add(1, Ordering::SeqCst);
        std::future::ready(())
    }
}

/// EOF kinds present in a stage journal, in append order.
async fn eof_kinds(run_dir: &Path, stage_key: &str) -> Vec<EofKind> {
    kinds_of(&replay_testkit::read_stage_envelopes_appended(run_dir, stage_key).await)
}

fn kinds_of(envelopes: &[EventEnvelope<ChainEvent>]) -> Vec<EofKind> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { kind, .. }) => Some(*kind),
            _ => None,
        })
        .collect()
}

/// Rows of `event_type` in a stage journal.
async fn data_rows(run_dir: &Path, stage_key: &str, event_type: &str) -> usize {
    replay_testkit::read_stage_envelopes_appended(run_dir, stage_key)
        .await
        .iter()
        .filter(|envelope| envelope.event.is_data() && envelope.event.event_type() == event_type)
        .count()
}

/// The replay run's `system.replay.completed` facts' synthesized kinds, read
/// through the typed system journal via the run manifest.
async fn synthesized_kinds(run_dir: &Path) -> Vec<Option<EofKind>> {
    use obzenflow_core::event::{ReplayLifecycleEvent, SystemEvent, SystemEventType};
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::Journal;
    use obzenflow_core::SystemId;

    let manifest = replay_testkit::archive_manifest(run_dir);
    let system_file = manifest["system_journal_file"]
        .as_str()
        .expect("manifest names the system journal")
        .to_string();
    let journal: obzenflow_infra::journal::DiskJournal<SystemEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            run_dir.join(system_file),
            JournalOwner::system(SystemId::new()),
        )
        .expect("system journal should open");
    journal
        .read_causally_ordered()
        .await
        .expect("system journal should read")
        .iter()
        .filter_map(|envelope| match &envelope.event.event {
            SystemEventType::ReplayLifecycle(ReplayLifecycleEvent::Completed {
                synthesized_eof_kind,
                ..
            }) => Some(*synthesized_eof_kind),
            _ => None,
        })
        .collect()
}

macro_rules! linear_flow {
    ($journal_base:expr, $delivered:expr, $source:expr) => {
        flow! {
            name: "truncated_fidelity_linear",
            journals: disk_journals($journal_base),
            middleware: [],

            stages: {
                ticks = source!(Tick => $source);
                summer = stateful!(Tick -> SumResult => SumHandler::new());
                out = sink!(SumResult => SinkTyped::with_delivery(counting::<SumResult>($delivered)).idempotent());
            },

            topology: {
                ticks |> summer;
                summer |> out;
            }
        }
    };
}

async fn replay_run(journal_base: &Path, archive: &Path, delivered: Arc<AtomicUsize>) {
    let base = journal_base.to_path_buf();
    let counter = delivered;
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive.as_os_str().to_os_string(),
        ])
        .run_async(linear_flow!(base, counter, Ticks::stalling(4)))
        .await
        .expect("replay should complete");
}

/// Record a cancelled linear run (the SIGINT shape: data delivered, no
/// committed EOF anywhere) and return its run directory.
async fn record_cancelled_linear(journal_base: &Path) -> std::path::PathBuf {
    let delivered = Arc::new(AtomicUsize::new(0));
    // The stateful sum emits only at drain, so sink deliveries cannot witness
    // progress; wait on the source journal's committed rows instead.
    let base = journal_base.to_path_buf();
    let counter = delivered.clone();
    let handle = linear_flow!(base, counter, Ticks::stalling(4))
        .await
        .expect("baseline flow should build");
    let run_dir = replay_testkit::latest_run_dir(journal_base);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "source rows were not committed in time"
        );
        let committed =
            std::panic::AssertUnwindSafe(data_rows(&run_dir, "ticks", Tick::EVENT_TYPE));
        if futures::FutureExt::catch_unwind(committed)
            .await
            .unwrap_or(0)
            >= 4
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.stop().await.expect("stop should be accepted");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("cancelled run should terminate")
        .expect("cancelled run should resolve");
    run_dir
}

#[tokio::test(flavor = "multi_thread")]
async fn truncated_replay_suppresses_finalization_and_records_the_kind() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let baseline = record_cancelled_linear(&journal_base).await;
    assert!(
        eof_kinds(&baseline, "ticks").await.is_empty(),
        "the cancelled baseline committed no source EOF"
    );

    let replay_delivered = Arc::new(AtomicUsize::new(0));
    replay_run(&journal_base, &baseline, replay_delivered.clone()).await;
    let candidate = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(baseline, candidate);

    // The synthesized source EOF reproduces the archive's missing completion.
    assert_eq!(
        eof_kinds(&candidate, "ticks").await,
        vec![EofKind::Truncated],
        "replay of a cancelled archive synthesizes exactly one Truncated EOF"
    );

    // End-of-input finalization is suppressed: no final aggregate, and the
    // stateful stage's authored EOF carries the folded Truncated kind.
    assert_eq!(
        data_rows(&candidate, "summer", SumResult::EVENT_TYPE).await,
        0,
        "the killed original never finalized, so the replay must not"
    );
    assert_eq!(
        replay_delivered.load(Ordering::SeqCst),
        0,
        "no final aggregate reaches the sink"
    );
    let summer_kinds = eof_kinds(&candidate, "summer").await;
    assert!(
        summer_kinds.iter().all(|kind| *kind == EofKind::Truncated),
        "every EOF the stateful stage journalled is Truncated: {summer_kinds:?}"
    );

    // The system journal records the fidelity decision as a fact.
    assert_eq!(
        synthesized_kinds(&candidate).await,
        vec![Some(EofKind::Truncated)],
        "the replay-completed lifecycle fact carries the synthesized kind"
    );

    // 095j: prefix mode, zero divergences, surplus at most informational.
    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!(
            "expected a completed comparison: {}",
            obzenflow_infra::verify::render_verdict(&outcome)
        );
    };
    assert_eq!(report.mode, "baseline_prefix_of_candidate");
    assert_eq!(outcome.exit_code(), 0, "{}", {
        obzenflow_infra::verify::render_verdict(&outcome)
    });
    assert_eq!(report.totals.divergences, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_closure_reproduces_the_truncated_kind() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let baseline = record_cancelled_linear(&journal_base).await;

    // Generation 1: replay of the cancelled archive commits a Truncated EOF.
    replay_run(&journal_base, &baseline, Arc::new(AtomicUsize::new(0))).await;
    let generation_one = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(
        eof_kinds(&generation_one, "ticks").await,
        vec![EofKind::Truncated]
    );

    // Generation 2: the committed Truncated kind is captured and reproduced.
    replay_run(
        &journal_base,
        &generation_one,
        Arc::new(AtomicUsize::new(0)),
    )
    .await;
    let generation_two = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(
        eof_kinds(&generation_two, "ticks").await,
        vec![EofKind::Truncated],
        "replay of a truncated replay reproduces the committed kind"
    );
    assert_eq!(
        synthesized_kinds(&generation_two).await,
        vec![Some(EofKind::Truncated)]
    );

    // Whole-run verification certifies the reproduction, Truncated evidence
    // and all: both runs completed, so evidence is compared, and it matches.
    let outcome = verify_run_dirs(&generation_one, &generation_two, &VerifyOptions::default())
        .expect("verification should run");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!(
            "expected a completed comparison: {}",
            obzenflow_infra::verify::render_verdict(&outcome)
        );
    };
    assert_eq!(report.mode, "whole_run");
    assert_eq!(
        outcome.exit_code(),
        0,
        "replay closure must certify: {}\n{}",
        obzenflow_infra::verify::render_verdict(&outcome),
        serde_json::to_string_pretty(&report.stages).unwrap_or_default()
    );
    assert_eq!(report.totals.divergences, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_archive_replay_still_finalizes_naturally() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // Record a naturally completed run: the source seals, the sum finalizes.
    let delivered = Arc::new(AtomicUsize::new(0));
    let base = journal_base.clone();
    let counter = delivered.clone();
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(linear_flow!(base, counter, Ticks::sealing(4)))
        .await
        .expect("baseline should complete");
    let baseline = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(eof_kinds(&baseline, "ticks").await, vec![EofKind::Natural]);
    assert_eq!(
        data_rows(&baseline, "summer", SumResult::EVENT_TYPE).await,
        1
    );
    assert_eq!(delivered.load(Ordering::SeqCst), 1);

    // Replay: the captured Natural kind reproduces and finalization runs.
    let replay_delivered = Arc::new(AtomicUsize::new(0));
    replay_run(&journal_base, &baseline, replay_delivered.clone()).await;
    let candidate = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(eof_kinds(&candidate, "ticks").await, vec![EofKind::Natural]);
    assert_eq!(
        data_rows(&candidate, "summer", SumResult::EVENT_TYPE).await,
        1,
        "a clean archive's replay still finalizes"
    );
    assert_eq!(replay_delivered.load(Ordering::SeqCst), 1);
    assert_eq!(
        synthesized_kinds(&candidate).await,
        vec![Some(EofKind::Natural)]
    );

    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    assert_eq!(
        outcome.exit_code(),
        0,
        "clean replay certifies unchanged: {}",
        obzenflow_infra::verify::render_verdict(&outcome)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_kind_fan_in_authors_the_worst_and_suppresses_finalization() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // Source A seals (commits a Natural EOF); source B stalls mid-stream.
    macro_rules! fan_in_flow {
        ($base:expr, $delivered:expr) => {
            flow! {
                name: "truncated_fidelity_fan_in",
                journals: disk_journals($base),
                middleware: [],

                stages: {
                    fast = source!(Tick => Ticks::sealing(3));
                    slow = source!(Tick => Ticks::stalling(4));
                    summer = stateful!(Tick -> SumResult => SumHandler::new());
                    out = sink!(SumResult => SinkTyped::with_delivery(counting::<SumResult>($delivered)).idempotent());
                },

                topology: {
                    fast |> summer;
                    slow |> summer;
                    summer |> out;
                }
            }
        };
    }

    let delivered = Arc::new(AtomicUsize::new(0));
    let base = journal_base.clone();
    let counter = delivered.clone();
    let handle = fan_in_flow!(base, counter)
        .await
        .expect("fan-in flow should build");
    let baseline = replay_testkit::latest_run_dir(&journal_base);

    // Wait until A's Natural EOF is committed and B is mid-stream, then kill.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "fast source did not commit its EOF in time"
        );
        let fast_kinds = std::panic::AssertUnwindSafe(eof_kinds(&baseline, "fast"));
        let fast_kinds = futures::FutureExt::catch_unwind(fast_kinds)
            .await
            .unwrap_or_default();
        let slow_rows =
            std::panic::AssertUnwindSafe(data_rows(&baseline, "slow", Tick::EVENT_TYPE));
        let slow_rows = futures::FutureExt::catch_unwind(slow_rows)
            .await
            .unwrap_or(0);
        if fast_kinds == vec![EofKind::Natural] && slow_rows >= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.stop().await.expect("stop should be accepted");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("cancelled run should terminate")
        .expect("cancelled run should resolve");

    // Replay: A reproduces Natural, B synthesizes Truncated, and the fan-in's
    // own authored EOF is the worst-wins join.
    let replay_delivered = Arc::new(AtomicUsize::new(0));
    let base = journal_base.clone();
    let counter = replay_delivered.clone();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            baseline.as_os_str().to_os_string(),
        ])
        .run_async(fan_in_flow!(base, counter))
        .await
        .expect("replay should complete");
    let candidate = replay_testkit::latest_run_dir(&journal_base);

    assert_eq!(eof_kinds(&candidate, "fast").await, vec![EofKind::Natural]);
    assert_eq!(
        eof_kinds(&candidate, "slow").await,
        vec![EofKind::Truncated]
    );

    // The fan-in journal carries both forwarded EOFs plus its authored one;
    // classify by author so the assertion targets the authored row.
    let summer_rows = replay_testkit::read_stage_envelopes_appended(&candidate, "summer").await;
    let fast_id = replay_testkit::read_stage_envelopes(&candidate, "fast")
        .await
        .iter()
        .find_map(|envelope| match &envelope.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_id, .. }) => {
                writer_id.as_ref().and_then(|w| w.as_stage().copied())
            }
            _ => None,
        })
        .expect("fast authored EOF names its stage");
    let slow_id = replay_testkit::read_stage_envelopes(&candidate, "slow")
        .await
        .iter()
        .find_map(|envelope| match &envelope.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_id, .. }) => {
                writer_id.as_ref().and_then(|w| w.as_stage().copied())
            }
            _ => None,
        })
        .expect("slow authored EOF names its stage");

    let authored_kinds: Vec<EofKind> = summer_rows
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof {
                kind, writer_id, ..
            }) => {
                let author = writer_id.as_ref().and_then(|w| w.as_stage().copied()).or(
                    match envelope.event.writer_id {
                        WriterId::Stage(id) => Some(id),
                        _ => None,
                    },
                );
                match author {
                    Some(id) if id != fast_id && id != slow_id => Some(*kind),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        authored_kinds,
        vec![EofKind::Truncated],
        "the fan-in authors worst(Natural, Truncated) = Truncated"
    );

    // Finalization suppressed below the mixed-kind fan-in.
    assert_eq!(
        data_rows(&candidate, "summer", SumResult::EVENT_TYPE).await,
        0
    );
    assert_eq!(replay_delivered.load(Ordering::SeqCst), 0);
}

/// Cycle entry transform: converged seeds emit a done row; everything else
/// re-enters the loop at the same depth.
#[derive(Debug, Clone)]
struct CycleEntry {
    writer_id: WriterId,
}

#[async_trait]
impl obzenflow_runtime::stages::common::handlers::TransformHandler for CycleEntry {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let ChainEventContent::Data { payload, .. } = &event.content else {
            return Ok(Vec::new());
        };
        let depth = payload["depth"].as_u64().unwrap_or(0);
        let kind = payload["kind"].as_str().unwrap_or("seed");
        if kind == "done" {
            return Ok(Vec::new());
        }
        let next_kind = if depth >= 2 { "done" } else { "iter" };
        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            Tick::EVENT_TYPE,
            json!({ "n": payload["n"], "depth": depth, "kind": next_kind }),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Cycle iteration transform: bumps depth on loop rows, ignores done rows.
#[derive(Debug, Clone)]
struct CycleIter {
    writer_id: WriterId,
}

#[async_trait]
impl obzenflow_runtime::stages::common::handlers::TransformHandler for CycleIter {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let ChainEventContent::Data { payload, .. } = &event.content else {
            return Ok(Vec::new());
        };
        if payload["kind"].as_str() != Some("iter") {
            return Ok(Vec::new());
        }
        let depth = payload["depth"].as_u64().unwrap_or(0);
        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            Tick::EVENT_TYPE,
            json!({ "n": payload["n"], "depth": depth + 1, "kind": "iter" }),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cycle_flow_truncated_replay_terminates_without_error() {
    // The SCC entry point buffers the external EOF until the cycle is
    // quiescent; the pre-resolution fold must still carry the Truncated kind
    // into the entry's authored EOF (the BufferAtEntryPoint path).
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    macro_rules! cycle_flow {
        ($base:expr, $delivered:expr) => {
            flow! {
                name: "truncated_fidelity_cycle",
                journals: disk_journals($base),
                middleware: [],

                stages: {
                    seeds = source!(Tick => Ticks::stalling(2));
                    entry = transform!(Tick -> Tick => CycleEntry { writer_id: WriterId::from(StageId::new()) });
                    iter = transform!(Tick -> Tick => CycleIter { writer_id: WriterId::from(StageId::new()) });
                    out = sink!(Tick => SinkTyped::with_delivery(counting::<Tick>($delivered)).idempotent());
                },

                topology: {
                    seeds |> entry;
                    entry |> iter;
                    entry <| iter;
                    entry |> out;
                }
            }
        };
    }

    let delivered = Arc::new(AtomicUsize::new(0));
    let base = journal_base.clone();
    let counter = delivered.clone();
    let handle = cycle_flow!(base, counter)
        .await
        .expect("cycle flow should build");
    let baseline = replay_testkit::latest_run_dir(&journal_base);

    // Wait until both seeds converged (two done rows in the entry journal).
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "the cycle did not converge in time"
        );
        let entry_rows = std::panic::AssertUnwindSafe(
            replay_testkit::read_stage_envelopes_appended(&baseline, "entry"),
        );
        let done = futures::FutureExt::catch_unwind(entry_rows)
            .await
            .map(|rows| {
                rows.iter()
                    .filter(|envelope| {
                        envelope.event.is_data()
                            && envelope.event.payload()["kind"].as_str() == Some("done")
                    })
                    .count()
            })
            .unwrap_or(0);
        if done >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.stop().await.expect("stop should be accepted");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("cancelled cycle run should terminate")
        .expect("cancelled cycle run should resolve");

    // Replay the cancelled cycle archive: it must terminate cleanly, with the
    // Truncated kind carried through the entry point's buffered-EOF path.
    let replay_delivered = Arc::new(AtomicUsize::new(0));
    let replay_base = journal_base.clone();
    let replay_counter = replay_delivered.clone();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            baseline.as_os_str().to_os_string(),
        ])
        .run_async(cycle_flow!(replay_base, replay_counter))
        .await
        .expect("replay of the cancelled cycle archive should complete");
    let candidate = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(baseline, candidate);

    assert_eq!(
        eof_kinds(&candidate, "seeds").await,
        vec![EofKind::Truncated]
    );
    let entry_kinds = eof_kinds(&candidate, "entry").await;
    assert!(
        entry_kinds.iter().all(|kind| *kind == EofKind::Truncated),
        "the cycle entry's journal carries only Truncated EOFs: {entry_kinds:?}"
    );
    assert!(
        !entry_kinds.is_empty(),
        "the cycle entry authored a terminal EOF"
    );
}
