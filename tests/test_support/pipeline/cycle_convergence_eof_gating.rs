// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::replay_testkit;

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::contracts::ContractPhase;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{
    ChainEvent, ChainPayload, PipelineLifecycleEvent, PipelineStopAdmission, SystemPayload,
};
use obzenflow_core::journal::JournalReader;
use obzenflow_core::TypedPayload;
use obzenflow_core::{CycleDepth, Outcomes, TransportContract};
use obzenflow_dsl::{flow, sink, source, test_flow, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{perform, EffectError, Port};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::testing::{BackpressureAckGate, JournalProbe};
use serde::{Deserialize, Serialize};

/// File-local payload for the cycle-convergence test. The JSON shape
/// fingerprints the stage contract per FLOWIP-114c. Individual cycle
/// phases are encoded in the payload `kind`, not as separate event types.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeedEvent {
    kind: String,
    depth: u64,
    target: u64,
}

impl TypedPayload for SeedEvent {
    const EVENT_TYPE: &'static str = "cycle.seed";
}

const KIND_SEED: &str = "seed";
const KIND_ITER: &str = "iter";
const KIND_DONE: &str = "done";
use obzenflow::stages::sources::{ChannelSource, ValuesSource};
use obzenflow_runtime::stages::common::handlers::source::{FiniteRead, SourceRead};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Notify;

fn seed(target: u64) -> SeedEvent {
    SeedEvent {
        kind: KIND_SEED.to_string(),
        depth: 0,
        target,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeedRead(u8);

#[obzenflow_runtime::effects::port]
#[async_trait]
trait SeedInput {
    #[query(type = "cycle_convergence.seed.read")]
    async fn read(
        &self,
        from: &SeedRead,
        ctx: &obzenflow::queries::QueryContext,
    ) -> Result<FiniteRead<SeedEvent, SeedRead>, obzenflow::queries::QueryError>;
}

struct SeedThenEof {
    target: u64,
    iteration_started: Arc<Notify>,
}

#[obzenflow_runtime::effects::system("seed-then-eof")]
#[async_trait]
impl SeedInput for SeedThenEof {
    async fn read(
        &self,
        from: &SeedRead,
        _ctx: &obzenflow::queries::QueryContext,
    ) -> Result<FiniteRead<SeedEvent, SeedRead>, obzenflow::queries::QueryError> {
        if from.0 == 0 {
            return Ok(FiniteRead::Batch {
                records: vec![seed(self.target)],
                next: SeedRead(1),
            });
        }
        self.iteration_started.notified().await;
        Ok(FiniteRead::EndOfInput)
    }
}

struct SeedThenEofSource(obzenflow::queries::QueryPort<dyn SeedInput>);

impl SeedThenEofSource {
    fn new(target: u64, iteration_started: Arc<Notify>) -> Self {
        Self(obzenflow::queries::QueryPort::to(SeedThenEof {
            target,
            iteration_started,
        }))
    }
}

impl TypedFiniteSourceHandler for SeedThenEofSource {
    type Output = SeedEvent;

    fn read(&self) -> SourceRead<SeedEvent> {
        SourceRead::finite(self.0.clone(), SeedRead(0), Ok)
    }
}

fn enforced_backpressure(
    window: u64,
    stall_timeout_ms: u64,
) -> obzenflow_runtime::run_context::FlowBuildContext {
    use obzenflow_core::config::{ConfigScope, ConfigSource};
    use obzenflow_runtime::runtime_config::{
        CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
    };
    let mut candidates = CandidateSet::default();
    for (key, value) in [
        (
            "runtime.backpressure.mode",
            ConfigValue::Text("enforce".to_owned()),
        ),
        ("runtime.backpressure.window", ConfigValue::U64(window)),
        (
            "runtime.backpressure.stall_timeout_ms",
            ConfigValue::U64(stall_timeout_ms),
        ),
    ] {
        candidates
            .admit(ScopedCandidate::unqualified(
                key,
                ConfigScope::Flow,
                ConfigSource::File,
                value,
            ))
            .expect("the backpressure setting admits");
    }
    replay_testkit::admit_unsynced(&mut candidates);
    obzenflow_runtime::run_context::FlowBuildContext::new(Arc::new(ResolvedRuntimeConfig::new(
        candidates,
    )))
}

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

fn single_flow_run_dir(base: &Path) -> Result<PathBuf> {
    let flows_dir = base.join("flows");
    let mut dirs: Vec<PathBuf> = fs::read_dir(&flows_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();

    anyhow::ensure!(
        dirs.len() == 1,
        "expected exactly one flow run dir under {:?}, got {}",
        flows_dir,
        dirs.len()
    );

    Ok(dirs.pop().expect("dirs is non-empty"))
}

async fn any_error_log_contains(run_dir: &Path, needle: &str) -> Result<bool> {
    for entry in fs::read_dir(run_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        if path.file_name().and_then(|n| n.to_str()) == Some("system.log") {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
            continue;
        }
        if !path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.contains("_error_"))
        {
            continue;
        }

        let mut reader =
            obzenflow_infra::journal::disk::reader::DiskJournalReader::<ChainEvent>::new(
                path.clone(),
                obzenflow_infra::journal::disk::identity::read_identity(&path)?.journal_id,
                Arc::new(tokio::sync::RwLock::new(())),
            )
            .await?;
        while let Some(record) = reader.next().await? {
            if serde_json::to_string(&record)?.contains(needle) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn final_contract_results(run_dir: &Path) -> BTreeMap<String, Vec<String>> {
    let manifest = replay_testkit::archive_manifest(run_dir);
    let stages = manifest["stages"].as_object().expect("manifest stages");
    let names: HashMap<&str, &str> = stages
        .iter()
        .map(|(name, stage)| (stage["stage_id"].as_str().expect("stage id"), name.as_str()))
        .collect();
    let mut finals: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for stage in stages.keys() {
        for record in replay_testkit::read_stage_envelopes_appended(run_dir, stage).await {
            if let ChainPayload::Execution(ExecutionPayload::ContractResult {
                upstream,
                reader,
                contract_name,
                phase: ContractPhase::Final,
                ..
            }) = &record.payload
            {
                finals
                    .entry(format!(
                        "{} -> {}",
                        names[upstream.to_string().as_str()],
                        names[reader.to_string().as_str()]
                    ))
                    .or_default()
                    .push(contract_name.to_string());
            }
        }
    }
    finals
}

async fn assert_entry_closes_its_cycle_before_completing(
    run_dir: &Path,
    entry: &str,
    member: &str,
) {
    let entry_writer = replay_testkit::stage_writer(run_dir, entry);
    let member_writer = replay_testkit::stage_writer(run_dir, member);
    let rows = replay_testkit::read_stage_envelopes_appended(run_dir, entry).await;
    let position = |describe: &str, found: &dyn Fn(&ChainPayload) -> bool| {
        rows.iter()
            .position(|row| found(&row.payload))
            .unwrap_or_else(|| panic!("{entry}'s journal records {describe}"))
    };
    let own_eof = position("its own EOF", &|payload| {
        matches!(
            payload,
            ChainPayload::FlowControl(FlowControlPayload::Eof {
                writer_id: Some(writer),
                ..
            }) if *writer == entry_writer
        )
    });
    let cycle_verdict = position("a verdict on the edge back from its cycle", &|payload| {
        matches!(
            payload,
            ChainPayload::Execution(ExecutionPayload::ContractStatus {
                upstream,
                pass: true,
                ..
            }) if obzenflow_core::WriterId::from(*upstream) == member_writer
        )
    });
    let completed = position("its completion", &|payload| {
        matches!(
            payload,
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                StageLifecycleFact::Completed { .. }
            ))
        )
    });
    assert!(
        own_eof < cycle_verdict && cycle_verdict < completed,
        "{entry} writes its EOF, then the verdict on the edge back from {member}, then completes \
         (positions {own_eof}, {cycle_verdict}, {completed})"
    );
}

#[derive(Clone, Debug)]
struct EntryConvergeTransform {
    processed_data: Arc<AtomicU64>,
}

impl EntryConvergeTransform {
    fn new(processed_data: Arc<AtomicU64>) -> Self {
        Self { processed_data }
    }
}

impl TypedTransformHandler for EntryConvergeTransform {
    type Input = SeedEvent;
    type Output = Outcomes<SeedEvent>;

    fn process(
        &self,
        mut event: SeedEvent,
    ) -> std::result::Result<Outcomes<SeedEvent>, HandlerError> {
        self.processed_data.fetch_add(1, Ordering::Relaxed);

        if event.depth >= event.target {
            event.kind = KIND_DONE.to_string();
            Ok(Outcomes::one(event))
        } else if event.kind == KIND_SEED || event.kind == KIND_ITER {
            event.kind = KIND_ITER.to_string();
            Ok(Outcomes::one(event))
        } else {
            Ok(Outcomes::none())
        }
    }
}

#[derive(Clone, Debug)]
struct IterationTransform {
    processed_iterations: Arc<AtomicU64>,
    iteration_started: Option<Arc<Notify>>,
}

impl IterationTransform {
    fn new(processed_iterations: Arc<AtomicU64>, iteration_started: Option<Arc<Notify>>) -> Self {
        Self {
            processed_iterations,
            iteration_started,
        }
    }
}

impl TypedTransformHandler for IterationTransform {
    type Input = SeedEvent;
    type Output = Outcomes<SeedEvent>;

    fn process(
        &self,
        mut event: SeedEvent,
    ) -> std::result::Result<Outcomes<SeedEvent>, HandlerError> {
        if event.kind != KIND_ITER {
            return Ok(Outcomes::none());
        }

        self.processed_iterations.fetch_add(1, Ordering::Relaxed);
        if let Some(iteration_started) = &self.iteration_started {
            iteration_started.notify_one();
        }

        event.depth = event.depth.saturating_add(1);
        Ok(Outcomes::one(event))
    }
}

#[derive(Clone, Debug, Serialize)]
struct SeedKind {
    kind: String,
}

#[obzenflow_runtime::effects::port]
trait DoneCounterPort: Send + Sync {
    #[effect(type = "cycle_convergence_eof_gating.count_done", at_least_once, records = reply)]
    fn count(&self, seed: &SeedKind) -> Result<(), EffectError>;
}

struct DoneCounter {
    done_events: Arc<AtomicU64>,
}

#[obzenflow_runtime::effects::system("done-counter")]
impl DoneCounterPort for DoneCounter {
    fn count(&self, seed: &SeedKind) -> Result<(), EffectError> {
        if seed.kind == KIND_DONE {
            self.done_events.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

fn seed_kind(event: &SeedEvent) -> SeedKind {
    SeedKind {
        kind: event.kind.clone(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cycle_buffers_external_eof_until_scc_quiescent() -> Result<()> {
    let journal_root = unique_journal_dir("cycle_buffers_external_eof");
    let journal_root_for_flow = journal_root.clone();
    let gate =
        BackpressureAckGate::install("eof_entry", "eof_iter", 0).map_err(anyhow::Error::msg)?;

    let target_iterations = 5u64;
    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();
    let done_count = Arc::new(AtomicU64::new(0));
    let sink = perform(seed_kind).through(Port::to(DoneCounter {
        done_events: done_count.clone(),
    }));
    let iteration_started = Arc::new(Notify::new());
    let source = SeedThenEofSource::new(target_iterations, iteration_started.clone());
    let entry = EntryConvergeTransform::new(entry_processed_for_flow);
    let iter = IterationTransform::new(iter_processed_for_flow, Some(iteration_started));

    let harness = test_flow! {
        build_context: enforced_backpressure(1, 5_000),
        name: "cycle_buffers_external_eof_until_scc_quiescent",
        journals: disk_journals(journal_root_for_flow.clone()),

        stages: {
            src = source!(SeedEvent => source);
            eof_entry = transform!(SeedEvent -> SeedEvent => entry);
            eof_iter = transform!(SeedEvent -> SeedEvent => iter);
            snk = sink!(SeedEvent => sink);
        },

        topology: {
            src |> eof_entry;
            eof_entry |> eof_iter;
            eof_entry <| eof_iter;
            eof_entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    let probe = JournalProbe::try_on_stage(&harness, "eof_entry")?;
    let (_, source_journal) = harness.stage_journal_for_test("src")?;
    let handle = harness.into_inner();
    handle.start().await?;
    eprintln!("cycle EOF: waiting for withheld internal-edge acknowledgement");
    tokio::time::timeout(Duration::from_secs(5), gate.wait_for_withheld(1)).await?;
    // The adjacent EOF caller also used virtual-time advances to pump physical
    // disk I/O. Establish the committed EOF with real cycle work held instead.
    eprintln!("cycle EOF: waiting for the source's committed EOF");
    let mut reader = source_journal.reader().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(record) = reader.next().await? {
                if record.is_eof() {
                    return Ok::<(), obzenflow_core::journal::JournalError>(());
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await??;
    assert_eq!(iter_processed.load(Ordering::Relaxed), 1);
    assert_eq!(done_count.load(Ordering::Relaxed), 0);
    gate.open();
    eprintln!("cycle EOF: released work; waiting for committed convergence");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion()).await??;

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        1,
        "expected one converged output"
    );
    assert_eq!(
        iter_processed.load(Ordering::Relaxed),
        target_iterations,
        "expected iter stage to execute the full multi-iteration loop"
    );
    assert_eq!(
        entry_processed.load(Ordering::Relaxed),
        target_iterations.saturating_add(1),
        "expected entry stage to see seed + each returned iteration event"
    );

    // Assert that the SCC entry stage observed the final cycle depth for the converging loop.
    let scc_id = probe
        .expect_event(1)
        .await?
        .envelope()
        .envelope
        .provenance
        .event
        .cycle_scc_id
        .expect("scc id");
    probe
        .expect_event_at_cycle_depth(
            scc_id,
            CycleDepth::new((target_iterations.saturating_add(1)) as u16),
            1,
        )
        .await?;

    let run_dir = replay_testkit::latest_run_dir(&journal_root);
    assert_eq!(
        replay_testkit::passing_verdicts(&run_dir).await,
        replay_testkit::one_verdict_per_edge(&[
            "eof_entry -> eof_iter",
            "eof_entry -> snk",
            "eof_iter -> eof_entry",
            "src -> eof_entry",
        ]),
        "every edge records one passing verdict, the edge back into the entry point included"
    );
    assert_eq!(
        final_contract_results(&run_dir).await,
        BTreeMap::from([
            (
                "eof_entry -> eof_iter".to_string(),
                vec![TransportContract::NAME.to_string()]
            ),
            (
                "eof_entry -> snk".to_string(),
                vec![TransportContract::NAME.to_string()]
            ),
            (
                "eof_iter -> eof_entry".to_string(),
                vec![TransportContract::NAME.to_string()]
            ),
            (
                "src -> eof_entry".to_string(),
                vec![TransportContract::NAME.to_string()]
            ),
        ]),
        "edges inside the cycle end with their transport result and no divergence result"
    );
    assert_entry_closes_its_cycle_before_completing(&run_dir, "eof_entry", "eof_iter").await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cycle_buffers_drain_until_scc_quiescent() -> Result<()> {
    let journal_root = unique_journal_dir("cycle_buffers_drain");
    // Withhold the first real internal-edge acknowledgement. A one-row credit
    // window holds the next publication, so drain admission must occur while
    // cycle work is still outstanding. The gate never blocks a runtime worker.
    let gate =
        BackpressureAckGate::install("drain_entry", "drain_iter", 0).map_err(anyhow::Error::msg)?;

    let target_iterations = 5u64;
    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();
    let done_count = Arc::new(AtomicU64::new(0));
    let sink = perform(seed_kind).through(Port::to(DoneCounter {
        done_events: done_count.clone(),
    }));
    let (seed_input, seeds) = obzenflow::stages::sources::source_channel(1);
    seed_input
        .try_send(seed(target_iterations))
        .expect("the channel holds the seed");
    let source = ChannelSource::new(seeds);
    let entry = EntryConvergeTransform::new(entry_processed_for_flow);
    let iter = IterationTransform::new(iter_processed_for_flow, None);

    let harness = test_flow! {
        build_context: enforced_backpressure(1, 5_000),
        name: "cycle_buffers_drain_until_scc_quiescent",
        journals: disk_journals(journal_root),

        stages: {
            src = source!(SeedEvent => source);
            drain_entry = transform!(SeedEvent -> SeedEvent => entry);
            drain_iter = transform!(SeedEvent -> SeedEvent => iter);
            snk = sink!(SeedEvent => sink);
        },

        topology: {
            src |> drain_entry;
            drain_entry |> drain_iter;
            drain_entry <| drain_iter;
            drain_entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    let probe = JournalProbe::try_on_stage(&harness, "drain_entry")?;
    let handle = harness.into_inner();
    let system_journal = handle.system_journal().expect("durable system journal");
    handle
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("flow start failed: {e}"))?;

    eprintln!("cycle drain: waiting for withheld internal-edge acknowledgement");
    tokio::time::timeout(Duration::from_secs(5), gate.wait_for_withheld(1))
        .await
        .map_err(|_| anyhow::anyhow!("cycle did not reach the acknowledgement gate"))?;
    assert_eq!(iter_processed.load(Ordering::Relaxed), 1);
    assert_eq!(done_count.load(Ordering::Relaxed), 0);
    handle
        .stop_graceful(Duration::from_secs(5))
        .await
        .map_err(|e| anyhow::anyhow!("graceful stop failed: {e}"))?;

    // Submission is not admission. Read the committed stop before releasing
    // work, rather than relying on a sleep or a number of scheduler yields.
    eprintln!("cycle drain: waiting for durable graceful-stop admission");
    let mut reader = system_journal.reader().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(record) = reader.next().await? {
                if matches!(
                    record.payload,
                    SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::StopAdmitted {
                        admission: PipelineStopAdmission::Graceful { .. }
                    })
                ) {
                    return Ok::<(), obzenflow_core::journal::JournalError>(());
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("graceful stop was not durably admitted"))??;
    assert_eq!(iter_processed.load(Ordering::Relaxed), 1);
    assert_eq!(done_count.load(Ordering::Relaxed), 0);
    gate.open();

    // Disk completion advances in real time. Deadline correctness has its own
    // controlled-clock test; this outer timeout is only a hang watchdog.
    eprintln!("cycle drain: released work; waiting for committed convergence");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow::anyhow!("cycle did not settle after gate release"))??;
    let facts = system_journal.read_all_unordered().await?;
    let terminal = facts.iter().find_map(|record| match &record.payload {
        SystemPayload::PipelineLifecycle(
            event @ (PipelineLifecycleEvent::Cancelled { .. }
            | PipelineLifecycleEvent::Completed { .. }
            | PipelineLifecycleEvent::Failed { .. }),
        ) => Some(event),
        _ => None,
    });
    assert!(
        matches!(terminal, Some(PipelineLifecycleEvent::Cancelled { reason, .. }) if reason == "user_stop"),
        "graceful cycle drain must finish as user_stop, not deadline cancellation: {terminal:?}"
    );

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        1,
        "expected one converged output even when drain arrives mid-cycle"
    );
    assert_eq!(
        iter_processed.load(Ordering::Relaxed),
        target_iterations,
        "expected iter stage to execute the full multi-iteration loop"
    );
    assert_eq!(
        entry_processed.load(Ordering::Relaxed),
        target_iterations.saturating_add(1),
        "expected entry stage to see seed + each returned iteration event"
    );

    // Assert the converging loop reached the final cycle depth even with a mid-flight drain.
    let scc_id = probe
        .expect_event(1)
        .await?
        .envelope()
        .envelope
        .provenance
        .event
        .cycle_scc_id
        .expect("scc id");
    probe
        .expect_event_at_cycle_depth(
            scc_id,
            CycleDepth::new((target_iterations.saturating_add(1)) as u16),
            1,
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn cycle_max_iterations_exceeded_routes_to_error_journal() -> Result<()> {
    let journal_root = unique_journal_dir("cycle_max_iterations_routing");
    let journal_root_for_flow = journal_root.clone();

    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();
    let done_count = Arc::new(AtomicU64::new(0));
    let sink = perform(seed_kind).through(Port::to(DoneCounter {
        done_events: done_count.clone(),
    }));

    // One correlation converges quickly; one correlation never converges and must be aborted
    // by CycleGuard max_iterations. Default max_iterations=30, so pick a target > 30.
    let converge_target = 2u64;
    let diverge_target = 1_000u64;

    let definition = FlowDefinition::materialize(move |_runtime_config| {
        let source = ValuesSource::new([seed(converge_target), seed(diverge_target)]);
        let entry = EntryConvergeTransform::new(entry_processed_for_flow);
        let iter = IterationTransform::new(iter_processed_for_flow, None);

        Ok(flow! {
            name: "cycle_max_iterations_exceeded_routes_to_error_journal",
            journals: disk_journals(journal_root_for_flow),

            stages: {
                src = source!(SeedEvent => source);
                entry = transform!(SeedEvent -> SeedEvent => entry);
                iter = transform!(SeedEvent -> SeedEvent => iter);
                snk = sink!(SeedEvent => sink);
            },

            topology: {
                src |> entry;
                entry |> iter;
                entry <| iter;
                entry |> snk;
            }
        })
    });
    let handle = definition
        .build(replay_testkit::unsynced_context())
        .await
        .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        1,
        "expected the converging correlation to produce one done event"
    );

    let run_dir = single_flow_run_dir(&journal_root)?;
    assert!(
        any_error_log_contains(&run_dir, "Cycle depth").await?,
        "expected cycle guard abort to be routed to an error journal; run_dir={run_dir:?}"
    );

    // Sanity: the non-converging correlation should not loop forever.
    assert!(
        iter_processed.load(Ordering::Relaxed) >= converge_target,
        "expected at least the converging loop to execute"
    );
    assert!(
        entry_processed.load(Ordering::Relaxed) > converge_target,
        "expected entry to process multiple events before aborting the diverging loop"
    );

    Ok(())
}

#[tokio::test]
async fn cycle_rejects_sccs_with_multiple_entry_points() {
    let definition = FlowDefinition::materialize(move |_runtime_config| {
        let source_a = ValuesSource::new([seed(1)]);
        let source_b = ValuesSource::new([seed(1)]);
        let transform_a = EntryConvergeTransform::new(Arc::new(AtomicU64::new(0)));
        let transform_b = EntryConvergeTransform::new(Arc::new(AtomicU64::new(0)));
        let sink = perform(seed_kind).through(Port::to(DoneCounter {
            done_events: Arc::default(),
        }));

        Ok(flow! {
            name: "cycle_reject_multi_entry_scc",
            journals: disk_journals(unique_journal_dir("cycle_reject_multi_entry_scc")),

            stages: {
                src1 = source!(SeedEvent => source_a);
                src2 = source!(SeedEvent => source_b);
                a = transform!(SeedEvent -> SeedEvent => transform_a);
                b = transform!(SeedEvent -> SeedEvent => transform_b);
                snk = sink!(SeedEvent => sink);
            },

            topology: {
                src1 |> a;
                src2 |> b;
                a |> b;
                a <| b;
                b |> snk;
            }
        })
    });
    let result = definition.build(replay_testkit::unsynced_context()).await;

    let err = match result {
        Ok(_) => panic!("expected multi-entry SCC validation to fail"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("must have exactly one entry point"),
        "error: {err}"
    );
}
