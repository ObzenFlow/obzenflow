// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-J: effect suppression across resume.
//!
//! Records an infinite-source flow whose effectful transform counts real
//! executions, then resumes it with two live inputs. Catch-up must re-execute
//! zero effects (outcomes read from recorded facts), and the live inputs must
//! execute exactly twice, recording outcome facts under the SAME root flow id
//! at `input_seq` beyond the recorded range. Assertions read the effect
//! provenance rows of the event-sourced journals.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord};
use obzenflow_core::journal::archive::certification::StageOrders;
use obzenflow_core::journal::archive::manifest::RunManifestDirectFactAdmission;
use obzenflow_core::journal::archive::{ReplayArchive, ReplayError};
use obzenflow_core::journal::factory::CurrentRunLocator;
use obzenflow_core::journal::{ArchiveStatus, JournalReader, StatusDerivation};
use obzenflow_core::source::RecordedSourceFrontier;
use obzenflow_core::{FlowId, JournalWriterId, ReaderGeneration, StageId, TypedPayload};
use obzenflow_dsl::{effectful_transform, flow, infinite_source, sink, FlowDefinition};
use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::{
    install_bootstrap_config, BootstrapConfig, ReplayBootstrap, ReplayVerb,
};
use obzenflow_runtime::effects::{EffectError, Effects, SinkRedeliverySafety, System};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkWriteFailure,
    TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use replay_testkit::ReattachingSource;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reading {
    value: u64,
}

impl TypedPayload for Reading {
    const EVENT_TYPE: &'static str = "resume_effects.reading";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EffectValue {
    effect_value: u64,
}

impl TypedPayload for EffectValue {
    const EVENT_TYPE: &'static str = "resume_effects.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Enriched {
    value: u64,
    effect_value: u64,
}

impl TypedPayload for Enriched {
    const EVENT_TYPE: &'static str = "resume_effects.enriched";
}

/// Emits `count` readings starting at `first_value`, then idles forever.
#[derive(Clone, Debug)]
struct BoundedReader {
    next_value: u64,
    remaining: u64,
}

impl BoundedReader {
    fn new(first_value: u64, count: u64) -> Self {
        Self {
            next_value: first_value,
            remaining: count,
        }
    }
}

impl TypedInfiniteSourceHandler for BoundedReader {
    type Output = Reading;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.remaining -= 1;
        let value = self.next_value;
        self.next_value += 1;
        Ok(vec![Reading { value }])
    }
}

/// Counts real executions via the shared counter; a suppressed catch-up
/// replay never reaches the counter.
struct CallCounter {
    calls: Arc<AtomicUsize>,
}

#[derive(Clone, Debug, Serialize)]
struct Counting {
    value: u64,
}

#[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
impl CallCounter {
    #[effect(type = "resume_effects.counting", exactly_once)]
    fn count(&self, request: &Counting) -> Result<EffectValue, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(EffectValue {
            effect_value: request.value + 100,
        })
    }
}

#[derive(Clone, Debug, obzenflow_runtime::effects::EffectHandler)]
#[effect_handler(effects = obzenflow_runtime::effects)]
struct EnrichTransform {
    #[system]
    counter: System<CallCounter>,
}

#[async_trait]
impl EffectfulTransformHandler for EnrichTransform {
    type Input = Reading;
    type Output = obzenflow_core::stage_fact_set![Enriched, EffectValue];
    type AllowedEffects = Counting;

    async fn process(
        &self,
        input: Reading,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let effect_value = fx
            .call(Counting { value: input.value })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(Enriched {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "resume-effects-v1"
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl InlineSink for CountingSink {
    type Input = Enriched;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _input: Enriched) -> Result<(), SinkWriteFailure> {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn build_flow(
    journal_base: PathBuf,
    first_value: u64,
    count: u64,
    calls: Arc<AtomicUsize>,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let reader = ReattachingSource(BoundedReader::new(first_value, count));
        let enrich_transform = EnrichTransform {
            counter: System::eager("call-counter", Arc::new(CallCounter { calls }))?,
        };
        let counting_sink = CountingSink { delivered };

        Ok(flow! {
            name: "resume_effects",
            journals: disk_journals(journal_base),

            stages: {
                src = infinite_source!(Reading => reader);
                enrich = effectful_transform!(
                    Reading -> { Enriched, EffectValue } => enrich_transform);
                snk = sink!(Enriched => counting_sink);
            },

            topology: {
                src |> enrich;
                enrich |> snk;
            }
        })
    })
}

async fn wait_for_running(handle: &FlowHandle) -> Result<()> {
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(*rx.borrow(), PipelineState::Running) {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for pipeline to reach Running"))?
}

async fn wait_for_count(counter: &Arc<AtomicU64>, target: u64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while counter.load(Ordering::SeqCst) < target {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "timeout waiting for the sink to consume {target} events (saw {})",
            counter.load(Ordering::SeqCst)
        )
    })
}

/// Run one flow instance until the sink consumed `expected` outputs, then
/// stop. Returns the effect executions this run's handlers performed.
async fn run_until_delivered(
    journal_base: &Path,
    first_value: u64,
    count: u64,
    expected: u64,
) -> Result<usize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_flow(
        journal_base.to_path_buf(),
        first_value,
        count,
        calls.clone(),
        delivered.clone(),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    wait_for_count(&delivered, expected).await?;
    handle.stop().await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    Ok(calls.load(Ordering::SeqCst))
}

/// The user-owned effect outcome facts of a stage journal, in physical append
/// order: `(recorded_flow_id, input_seq)` per fact.
fn outcome_fact_cursors(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<(String, u64)> {
    let outcome_type = EffectValue::event_type_name();
    envelopes
        .iter()
        .filter_map(|envelope| {
            let event_type = envelope.event_type();
            let ChainPayload::Fact(_) = &envelope.payload else {
                return None;
            };
            if event_type != outcome_type.as_str() {
                return None;
            }
            let provenance = envelope
                .envelope
                .provenance
                .event
                .effect_provenance
                .as_ref()
                .expect("an effect outcome fact must carry effect provenance");
            assert!(
                provenance.fact_owner.is_user(),
                "typed effect outcomes are user-owned domain facts"
            );
            Some((
                provenance.cursor.recorded_flow_id.as_str().to_string(),
                provenance.cursor.input_seq.get(),
            ))
        })
        .collect()
}

/// Index of the stage's authored catch-up watermark in append order.
fn watermark_index(envelopes: &[JournalRecord<ChainPayload>], stage_key: &str) -> usize {
    envelopes
        .iter()
        .position(|envelope| {
            matches!(
                &envelope.payload,
                ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
                    stage_key: key,
                    ..
                }) if key.as_str() == stage_key
            )
        })
        .expect("the effectful stage must author its catch-up watermark")
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_suppresses_recorded_effects_and_executes_the_live_tail_once() -> Result<()> {
    const RECORDED: u64 = 3; // readings 1..=3
    const LIVE: u64 = 2; // readings 4..=5

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    // Record: every effect executes live, once per input.
    let recorded_calls = run_until_delivered(&journal_base, 1, RECORDED, RECORDED).await?;
    assert_eq!(recorded_calls, RECORDED as usize);
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    // The recording's root flow id anchors every effect cursor.
    let recorded_manifest: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        recorded_run.join("run_manifest.json"),
    )?)?;
    let root_flow_id = recorded_manifest["flow_id"]
        .as_str()
        .expect("the recorded manifest names its flow id")
        .to_string();

    let recorded_facts = outcome_fact_cursors(
        &replay_testkit::read_stage_envelopes_appended(&recorded_run, "enrich").await,
    );
    assert_eq!(
        recorded_facts,
        (1..=RECORDED)
            .map(|seq| (root_flow_id.clone(), seq))
            .collect::<Vec<_>>(),
        "the recording anchors outcome facts at input_seq 1..=K under its own flow id"
    );

    // Resume with two live inputs. The counter is fresh, so any catch-up
    // re-execution would be visible in the final count.
    let resumed_calls = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        // Wait for prefix re-deliveries + live (the sink re-consumes the
        // recorded prefix during catch-up, F14).
        run_until_delivered(&journal_base, RECORDED + 1, LIVE, RECORDED + LIVE).await?
    };
    assert_eq!(
        resumed_calls, LIVE as usize,
        "catch-up re-executes ZERO effects; only the live inputs execute"
    );
    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, resumed_run);

    // The resumed run's effect ledger: the K recorded outcome facts are
    // re-committed from history during catch-up, then the stage's watermark,
    // then the two live outcome facts, all under the SAME root flow id, with
    // the live facts at input_seq strictly beyond the recorded range.
    let resumed_envelopes =
        replay_testkit::read_stage_envelopes_appended(&resumed_run, "enrich").await;
    let resumed_facts = outcome_fact_cursors(&resumed_envelopes);
    assert_eq!(
        resumed_facts,
        (1..=RECORDED + LIVE)
            .map(|seq| (root_flow_id.clone(), seq))
            .collect::<Vec<_>>(),
        "every outcome fact carries the root flow id; live facts sit at \
         input_seq {}..={}",
        RECORDED + 1,
        RECORDED + LIVE
    );

    // The live facts land after the stage's authored catch-up watermark, the
    // structural witness that they were recorded in the live phase.
    let boundary = watermark_index(&resumed_envelopes, "enrich");
    let outcome_type = EffectValue::event_type_name();
    let outcome_positions: Vec<usize> = resumed_envelopes
        .iter()
        .enumerate()
        .filter_map(|(index, envelope)| match &envelope.payload {
            ChainPayload::Fact(_) if envelope.event_type() == outcome_type.as_str() => Some(index),
            _ => None,
        })
        .collect();
    let (catch_up_facts, live_facts): (Vec<usize>, Vec<usize>) = outcome_positions
        .into_iter()
        .partition(|index| *index < boundary);
    assert_eq!(
        catch_up_facts.len() as u64,
        RECORDED,
        "the recorded outcome facts are re-committed during catch-up, before the boundary"
    );
    assert_eq!(
        live_facts.len() as u64,
        LIVE,
        "the live outcome facts are recorded after the boundary"
    );

    Ok(())
}

/// Tear R1 as a kill during the effectful stage's catch-up would: the stage
/// keeps its first `kept` re-committed outcomes, the sink no receipt, and the
/// system log nothing from any reconstruction completion on.
fn tear_effect_catch_up(r1: &Path, kept: usize) -> Result<()> {
    let manifest = replay_testkit::archive_manifest(r1);
    let journal = |key: &str| {
        r1.join(
            manifest["stages"][key]["data_journal_file"]
                .as_str()
                .unwrap(),
        )
    };
    let (enrich, snk) = (journal("enrich"), journal("snk"));
    let system = r1.join(manifest["system_journal_file"].as_str().unwrap());
    let outcome_type = EffectValue::event_type_name();
    let event_type = |record: &serde_json::Value| {
        record
            .pointer("/envelope/provenance/event/event_type")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    let outcomes = std::cell::Cell::new(0usize);
    let receipt_seen = std::cell::Cell::new(false);
    let completion_seen = std::cell::Cell::new(false);
    obzenflow_infra::testing::journal::retain_archive_frames(r1, |path, records| {
        if path == enrich {
            let committed = records
                .iter()
                .filter(|record| event_type(record).as_deref() == Some(outcome_type.as_str()))
                .count();
            outcomes.set(outcomes.get() + committed);
            outcomes.get() <= kept
        } else if path == snk {
            receipt_seen.set(
                receipt_seen.get()
                    || records.iter().any(|record| {
                        record.pointer("/envelope/provenance/event/event_kind")
                            == Some(&json!("delivery"))
                    }),
            );
            !receipt_seen.get()
        } else if path == system {
            completion_seen.set(
                completion_seen.get()
                    || records.iter().any(|record| {
                        event_type(record)
                            .is_some_and(|name| name.ends_with("reconstruction_completed"))
                    }),
            );
            !completion_seen.get()
        } else {
            true
        }
    })
    .map_err(anyhow::Error::from_boxed)?;
    Ok(())
}

/// FLOWIP-134i B6: a resume torn while its effectful stage re-committed the
/// recorded history merges with the run it resumed from, so outcomes that
/// only the ancestor still holds are reused and never executed again.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_resume_reuses_effects_only_its_ancestor_still_holds() -> Result<()> {
    const RECORDED: u64 = 6; // readings 1..=6
    const KEPT: usize = 2; // outcomes the torn resume re-committed
    const LIVE: u64 = 2; // readings 7..=8

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let recorded_calls = run_until_delivered(&journal_base, 1, RECORDED, RECORDED).await?;
    assert_eq!(recorded_calls, RECORDED as usize);
    let r0 = replay_testkit::latest_run_dir(&journal_base);

    let resume_of = |archive: PathBuf| ReplayBootstrap {
        archive_path: archive,
        allow_incomplete_archive: true,
        allow_duplicate_sink_delivery: false,
        verb: ReplayVerb::Resume,
    };
    let r1_calls = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(resume_of(r0.clone())).await,
        );
        run_until_delivered(&journal_base, RECORDED + 1, 0, RECORDED).await?
    };
    assert_eq!(r1_calls, 0, "R1 reuses every recorded outcome");
    let r1 = replay_testkit::latest_run_dir(&journal_base);
    tear_effect_catch_up(&r1, KEPT)?;
    assert_eq!(
        outcome_fact_cursors(&replay_testkit::read_stage_envelopes_appended(&r1, "enrich").await)
            .len(),
        KEPT
    );

    // Without the merge, R2 would trust R1's two outcomes and execute
    // readings 3..=6 again.
    let r2_calls = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(resume_of(r1.clone())).await,
        );
        run_until_delivered(&journal_base, RECORDED + 1, LIVE, RECORDED + LIVE).await?
    };
    assert_eq!(
        r2_calls, LIVE as usize,
        "outcomes committed only in R0 are reused; only the live inputs execute"
    );
    let r2 = replay_testkit::latest_run_dir(&journal_base);
    let merged_from = replay_testkit::archive_manifest(&r2)["resume"]["merged_from"].clone();
    assert_eq!(merged_from, json!([std::fs::canonicalize(&r0)?]));
    Ok(())
}

/// Tear R1's source mid-prefix: keep its first `kept` re-admitted readings.
fn tear_source_after(r1: &Path, kept: usize) -> Result<()> {
    let manifest = replay_testkit::archive_manifest(r1);
    let src = r1.join(
        manifest["stages"]["src"]["data_journal_file"]
            .as_str()
            .unwrap(),
    );
    let seen = std::cell::Cell::new(0usize);
    obzenflow_infra::testing::journal::retain_archive_frames(r1, |path, records| {
        if path != src {
            return true;
        }
        let facts = records
            .iter()
            .filter(|record| {
                record.pointer("/envelope/provenance/event/event_kind") == Some(&json!("fact"))
            })
            .count();
        seen.set(seen.get() + facts);
        seen.get() <= kept
    })
    .map_err(anyhow::Error::from_boxed)?;
    Ok(())
}

/// Replay to completion: the source ends at the end of its recording and the
/// flow drains. Returns the effect executions the replay's handlers made.
async fn replay_to_completion(journal_base: &Path) -> Result<usize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let handle = build_flow(
        journal_base.to_path_buf(),
        1,
        0,
        calls.clone(),
        Arc::new(AtomicU64::new(0)),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("replay flow failed to build: {e:?}"))?;
    tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the replay to drain"))??;
    Ok(calls.load(Ordering::SeqCst))
}

/// FLOWIP-134i B13: a resume torn mid-prefix, replayed to completion, then
/// resumed through the replay continues the torn run and merges its ancestor,
/// so outcomes committed only in R0 are reused.
#[tokio::test(flavor = "multi_thread")]
async fn resuming_through_a_replay_of_a_torn_resume_reuses_ancestor_effects() -> Result<()> {
    const RECORDED: u64 = 6; // readings 1..=6
    const KEPT: usize = 2; // readings and outcomes the torn resume kept
    const LIVE: u64 = 2; // readings 7..=8

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    run_until_delivered(&journal_base, 1, RECORDED, RECORDED).await?;
    let r0 = replay_testkit::latest_run_dir(&journal_base);
    let archive_of = |archive: PathBuf, verb| ReplayBootstrap {
        archive_path: archive,
        allow_incomplete_archive: true,
        allow_duplicate_sink_delivery: false,
        verb,
    };
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive_of(r0.clone(), ReplayVerb::Resume))
                .await,
        );
        run_until_delivered(&journal_base, RECORDED + 1, 0, RECORDED).await?;
    }
    let r1 = replay_testkit::latest_run_dir(&journal_base);
    tear_source_after(&r1, KEPT)?;
    tear_effect_catch_up(&r1, KEPT)?;

    let replay_calls = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive_of(r1.clone(), ReplayVerb::Replay))
                .await,
        );
        replay_to_completion(&journal_base).await?
    };
    assert_eq!(replay_calls, 0, "a replay never executes an effect");
    let replayed = replay_testkit::latest_run_dir(&journal_base);

    let r2_calls = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive_of(replayed, ReplayVerb::Resume)).await,
        );
        run_until_delivered(&journal_base, RECORDED + 1, LIVE, RECORDED + LIVE).await?
    };
    assert_eq!(
        r2_calls, LIVE as usize,
        "outcomes committed only in R0 are reused; only the live inputs execute"
    );
    let resume = replay_testkit::archive_manifest(&replay_testkit::latest_run_dir(&journal_base))
        ["resume"]
        .clone();
    assert_eq!(resume["resumed_from"], json!(std::fs::canonicalize(&r1)?));
    assert_eq!(resume["merged_from"], json!([std::fs::canonicalize(&r0)?]));
    Ok(())
}

struct HistoryReads {
    inner: DiskReplayArchive,
    opened: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl ReplayArchive for HistoryReads {
    async fn open_source_reader(
        &self,
        stage_key: &str,
        expected_type: StageType,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
        self.inner
            .open_source_reader(stage_key, expected_type)
            .await
    }

    async fn open_effect_history(
        &self,
        stage_key: &str,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
        self.opened
            .lock()
            .expect("history reads lock")
            .push(stage_key.to_string());
        self.inner.open_effect_history(stage_key).await
    }

    fn source_data_journal_path(&self, stage_key: &str) -> Result<PathBuf, ReplayError> {
        self.inner.source_data_journal_path(stage_key)
    }

    fn archive_flow_id(&self) -> &str {
        self.inner.archive_flow_id()
    }

    fn recorded_flow_id(&self) -> Option<&str> {
        self.inner.recorded_flow_id()
    }

    fn stage_orders(&self) -> StageOrders {
        self.inner.stage_orders()
    }

    fn journal_stages(&self) -> HashMap<JournalWriterId, String> {
        self.inner.journal_stages()
    }

    fn archived_stage_id(&self, stage_key: &str) -> Result<StageId, ReplayError> {
        self.inner.archived_stage_id(stage_key)
    }

    fn archive_status(&self) -> ArchiveStatus {
        self.inner.archive_status()
    }

    fn status_derivation(&self) -> StatusDerivation {
        ReplayArchive::status_derivation(&self.inner)
    }

    fn allow_incomplete_archive(&self) -> bool {
        self.inner.allow_incomplete_archive()
    }

    fn source_stage_keys(&self) -> Vec<String> {
        self.inner.source_stage_keys()
    }

    fn recorded_source_eof(&self, stage_key: &str) -> Option<EofKind> {
        self.inner.recorded_source_eof(stage_key)
    }

    fn recorded_source_frontier(&self, stage_key: &str) -> RecordedSourceFrontier {
        self.inner.recorded_source_frontier(stage_key)
    }

    fn archive_path(&self) -> &Path {
        self.inner.archive_path()
    }

    fn claim_continuation(
        &self,
        successor: FlowId,
        location: &CurrentRunLocator,
    ) -> Result<(), ReplayError> {
        self.inner.claim_continuation(successor, location)
    }

    fn ancestors(&self) -> Vec<Arc<dyn ReplayArchive>> {
        self.inner.ancestors()
    }

    fn manifest_capability(&self, name: &str) -> Option<u32> {
        self.inner.manifest_capability(name)
    }

    fn bounded_direct_fact_admission(&self) -> &[RunManifestDirectFactAdmission] {
        self.inner.bounded_direct_fact_admission()
    }

    fn max_recorded_generation(&self) -> ReaderGeneration {
        self.inner.max_recorded_generation()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replay_reads_only_the_journals_of_stages_with_an_effect_runtime() -> Result<()> {
    const RECORDED: u64 = 3;

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    run_until_delivered(&journal_base, 1, RECORDED, RECORDED).await?;
    let recorded = replay_testkit::latest_run_dir(&journal_base);
    let archive = DiskReplayArchive::open(recorded.clone(), true).await?;
    let opened = Arc::new(Mutex::new(Vec::new()));

    let bootstrap = install_bootstrap_config(BootstrapConfig {
        replay: Some(ReplayBootstrap {
            archive_path: recorded,
            allow_incomplete_archive: true,
            allow_duplicate_sink_delivery: false,
            verb: ReplayVerb::Replay,
        }),
        archive_evidence: Some(archive.evidence()),
        replay_archive: Some(Arc::new(HistoryReads {
            inner: archive,
            opened: opened.clone(),
        })),
        ..Default::default()
    });
    let replay_calls = replay_to_completion(&journal_base).await?;
    drop(bootstrap);

    assert_eq!(replay_calls, 0, "a replay never executes an effect");
    assert_eq!(
        *opened.lock().expect("history reads lock"),
        vec!["enrich".to_string()],
        "only the effectful transform reads its archived journal; the sink reads none"
    );
    Ok(())
}
