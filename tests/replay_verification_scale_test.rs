// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j at scale: a five-figure-event run records, replays, and
//! verifies. The comparison is a streaming walk, so memory stays bounded by
//! row size rather than run size; this suite is the executable witness that
//! the posture holds on real journals.

use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
use obzenflow_runtime::stages::sink::SinkWriteFailure;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

#[derive(Debug, Default)]
struct Progress {
    produced: AtomicU64,
    sink_entered: AtomicU64,
    sink_returned: AtomicU64,
    blocked: tokio::sync::Notify,
}
impl Progress {
    fn snapshot(&self) -> String {
        format!(
            "produced={}, sink_entered={}, sink_returned={}",
            self.produced.load(Ordering::Relaxed),
            self.sink_entered.load(Ordering::Relaxed),
            self.sink_returned.load(Ordering::Relaxed)
        )
    }
}

// Independent hang protection. These snapshots never read a journal or wait
// for the stalled handler's lock; they describe fixture callbacks, not receipts.
async fn observe_phase<F: std::future::Future>(
    phase: &str,
    progress: &Progress,
    journals: &Path,
    budget: Duration,
    future: F,
) -> Result<F::Output, String> {
    let started = std::time::Instant::now();
    let deadline = tokio::time::sleep(budget);
    let period = Duration::from_secs(5);
    let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(future, deadline);
    eprintln!(
        "replay scale: phase={phase}, budget={budget:?}, journals={}",
        journals.display()
    );
    loop {
        tokio::select! {
            result = &mut future => return Ok(result),
            _ = &mut deadline => return Err(format!(
                "replay scale: phase={phase} exceeded {budget:?}; elapsed={:?}, {}, journals={}",
                started.elapsed(), progress.snapshot(), journals.display())),
            _ = heartbeat.tick() => eprintln!(
                "replay scale: phase={phase}, elapsed={:?}, {}", started.elapsed(), progress.snapshot()),
        }
    }
}

struct Evidence(Option<tempfile::TempDir>);
impl Evidence {
    fn new() -> Self {
        let base = std::env::var_os("OBZENFLOW_TEST_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/replay-test-evidence"));
        std::fs::create_dir_all(&base).unwrap();
        Self(Some(
            tempfile::Builder::new()
                .prefix("replay-scale-")
                .tempdir_in(base)
                .unwrap(),
        ))
    }
    fn path(&self) -> &Path {
        self.0.as_ref().unwrap().path()
    }
}
impl Drop for Evidence {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let retained = self.0.take().unwrap().keep();
            eprintln!(
                "replay scale: retained failure journals at {}",
                retained.display()
            );
        }
    }
}

const EVENTS: u64 = 10_000;
const BATCH: u64 = 500;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick {
    n: u64,
}

impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "replay_verification_scale.tick";
}

#[derive(Clone, Debug)]
struct Ticks {
    next: u64,
    count: u64,
    progress: Arc<Progress>,
}

impl TypedFiniteSourceHandler for Ticks {
    type Output = Tick;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next >= self.count {
            return Ok(None);
        }
        let batch: Vec<Tick> = (self.next..(self.next + BATCH).min(self.count))
            .map(|n| Tick { n })
            .collect();
        self.next += batch.len() as u64;
        self.progress
            .produced
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        Ok(Some(batch))
    }
}

fn build_flow(
    journal_base: PathBuf,
    count: u64,
    progress: Arc<Progress>,
    gate: Option<Arc<tokio::sync::Semaphore>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let ticks_handler = Ticks {
            next: 0,
            count,
            progress: progress.clone(),
        };
        let out_handler = GatedDelivery { progress, gate };

        Ok(flow! {
            name: "replay_verification_scale",
            journals: disk_journals(journal_base),

            stages: {
                ticks = source!(Tick => ticks_handler);
                out = sink!(Tick => out_handler);
            },

            topology: {
                ticks |> out;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_figure_run_verifies_with_streaming_comparison() {
    let temp = Evidence::new();
    let journal_base = temp.path().join("journals");
    let total_started = std::time::Instant::now();

    eprintln!("replay scale: phase=live, records={EVENTS}");
    let live_progress = Arc::new(Progress::default());
    observe_phase(
        "live",
        &live_progress,
        &journal_base,
        Duration::from_secs(120),
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_flow(
                journal_base.clone(),
                EVENTS,
                live_progress.clone(),
                None,
            )),
    )
    .await
    .unwrap_or_else(|diagnostic| panic!("{diagnostic}"))
    .expect("live flow should complete");
    assert_eq!(live_progress.produced.load(Ordering::Relaxed), EVENTS);
    assert_eq!(live_progress.sink_returned.load(Ordering::Relaxed), EVENTS);
    eprintln!(
        "replay scale: live complete, elapsed={:?}",
        total_started.elapsed()
    );
    let baseline = latest_run_dir(&journal_base);

    eprintln!(
        "replay scale: phase=replay, elapsed={:?}",
        total_started.elapsed()
    );
    let replay_progress = Arc::new(Progress::default());
    observe_phase(
        "replay",
        &replay_progress,
        &journal_base,
        Duration::from_secs(120),
        FlowApplication::builder()
            .with_cli_args(vec![
                OsString::from("obzenflow"),
                OsString::from("--replay-from"),
                baseline.as_os_str().to_os_string(),
            ])
            .run_async(build_flow(
                journal_base.clone(),
                EVENTS,
                replay_progress.clone(),
                None,
            )),
    )
    .await
    .unwrap_or_else(|diagnostic| panic!("{diagnostic}"))
    .expect("replay flow should complete");
    assert_eq!(
        replay_progress.produced.load(Ordering::Relaxed),
        0,
        "replay must not poll the live source"
    );
    assert_eq!(
        replay_progress.sink_returned.load(Ordering::Relaxed),
        EVENTS
    );
    eprintln!(
        "replay scale: replay complete, elapsed={:?}",
        total_started.elapsed()
    );
    let candidate = latest_run_dir(&journal_base);

    eprintln!(
        "replay scale: phase=comparison, elapsed={:?}",
        total_started.elapsed()
    );
    let started = std::time::Instant::now();
    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    let elapsed = started.elapsed();
    eprintln!(
        "replay scale: comparison={elapsed:?}, total={:?}",
        total_started.elapsed()
    );

    assert_eq!(
        outcome.exit_code(),
        0,
        "{}",
        obzenflow_infra::verify::render_verdict(&outcome)
    );
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("expected a completed comparison");
    };
    assert_eq!(report.stages["ticks"].positional_rows_baseline, EVENTS);
    assert_eq!(report.stages["ticks"].positional_rows_candidate, EVENTS);
    // Streaming-comparison performance belongs to the qualified Criterion gate
    // in `cargo xtask test --lane performance`. Nextest bounds a stalled proof;
    // this test certifies all 10,000 positions and the live/replay outcomes.
}

// This forces a real handler boundary to stall. It validates attribution and
// resumed completion; it does not claim to reproduce an earlier scale timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_sink_reports_live_phase_and_progress_before_recovery() {
    let temp = Evidence::new();
    let journals = temp.path().join("journals");
    let progress = Arc::new(Progress::default());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let application = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journals.clone(),
            1,
            progress.clone(),
            Some(gate.clone()),
        ));
    tokio::pin!(application);
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = progress.blocked.notified() => {},
            result = &mut application => panic!("flow settled before the controlled stall: {result:?}"),
        }
    }).await.expect("the real sink boundary must be reached");
    let diagnostic = observe_phase(
        "live",
        &progress,
        &journals,
        Duration::from_millis(20),
        &mut application,
    )
    .await
    .expect_err("the gated sink cannot complete");
    assert!(diagnostic.contains("phase=live"), "{diagnostic}");
    assert!(
        diagnostic.contains("produced=1, sink_entered=1, sink_returned=0"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains(&journals.display().to_string()),
        "{diagnostic}"
    );
    eprintln!("controlled stall: {diagnostic}");
    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), &mut application)
        .await
        .expect("releasing the gate must resume the same flow")
        .expect("flow should complete");
    assert_eq!(progress.sink_returned.load(Ordering::Relaxed), 1);
}
#[derive(Clone)]
struct GatedDelivery {
    progress: Arc<Progress>,
    gate: Option<Arc<tokio::sync::Semaphore>>,
}
#[async_trait::async_trait]
impl obzenflow_runtime::stages::sink::InlineSink for GatedDelivery {
    type Input = Tick;
    fn describe(&self) -> obzenflow_runtime::stages::sink::SinkDescription {
        obzenflow_runtime::stages::sink::SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                "gated_counter".into(),
            ),
        )
        .with_redelivery_safety(obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat)
    }
    async fn write(&mut self, input: Tick) -> Result<(), SinkWriteFailure> {
        let _ = input;
        self.progress.sink_entered.fetch_add(1, Ordering::Relaxed);
        if let Some(gate) = &self.gate {
            self.progress.blocked.notify_one();
            gate.acquire().await.unwrap().forget();
        }
        self.progress.sink_returned.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl std::fmt::Debug for GatedDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GatedDelivery")
    }
}
