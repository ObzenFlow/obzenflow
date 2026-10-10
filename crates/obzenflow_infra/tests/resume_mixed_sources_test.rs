// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-J: mixed finite and infinite sources under resume.
//!
//! One finite source (authors EOF at natural exhaustion) and one infinite
//! source feed a deterministic fan-in. Under resume the finite source behaves
//! as replay: it re-admits its rows, re-authors EOF, and authors no catch-up
//! watermark. The infinite source authors its watermark and continues live.
//! The fan-in must still author its own watermark, counting the EOF-exhausted
//! input as vacuously crossed (the F17 disjunction), and go live. All
//! assertions read the event-sourced journals.
//!
//! FLOWIP-134i: a finite source continues by its connector's declaration. An
//! unfinished `Refuse` source refuses at admission; an unfinished CSV source
//! (`Seek`) continues strictly after its last committed row, refuses a changed
//! input before any live row, and a source that never pinned starts on resume.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_adapters::sources::{CsvDecoder, CsvSource};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourceCleanupPhase, SourceCleanupTrigger, SourceContinuationRefusal,
    SourceOpenIntent,
};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{
    ChainEvent, ChainPayload, JournalRecord, PipelineLifecycleEvent, SystemEvent, SystemPayload,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{
    effectful_transform, flow, infinite_source, sink, source, transform, FlowDefinition,
};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey, SinkRedeliverySafety,
};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkWriteFailure,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChannelTick {
    channel: String,
    value: u64,
}

impl TypedPayload for ChannelTick {
    const EVENT_TYPE: &'static str = "resume_mixed.tick";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Merged {
    channel: String,
    value: u64,
}

impl TypedPayload for Merged {
    const EVENT_TYPE: &'static str = "resume_mixed.merged";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EffectValue {
    effect_value: u64,
}

impl TypedPayload for EffectValue {
    const EVENT_TYPE: &'static str = "resume_mixed.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MixedOutput {
    channel: String,
    value: u64,
    effect_value: u64,
}

impl TypedPayload for MixedOutput {
    const EVENT_TYPE: &'static str = "resume_mixed.output";
}

/// Finite source: emits `count` values starting at `first_value`, then
/// exhausts naturally (authored EOF).
#[derive(Clone, Debug)]
struct FiniteChannelSource {
    channel: &'static str,
    next_value: u64,
    remaining: u64,
}

impl FiniteChannelSource {
    fn new(channel: &'static str, first_value: u64, count: u64) -> Self {
        Self {
            channel,
            next_value: first_value,
            remaining: count,
        }
    }
}

impl TypedFiniteSourceHandler for FiniteChannelSource {
    type Output = ChannelTick;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let value = self.next_value;
        self.next_value += 1;
        Ok(Some(vec![ChannelTick {
            channel: self.channel.to_string(),
            value,
        }]))
    }
}

/// Infinite source: emits `count` values starting at `first_value`, then
/// idles forever.
#[derive(Clone, Debug)]
struct InfiniteChannelSource {
    channel: &'static str,
    next_value: u64,
    remaining: u64,
}

impl InfiniteChannelSource {
    fn new(channel: &'static str, first_value: u64, count: u64) -> Self {
        Self {
            channel,
            next_value: first_value,
            remaining: count,
        }
    }
}

impl TypedInfiniteSourceHandler for InfiniteChannelSource {
    type Output = ChannelTick;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.remaining -= 1;
        let value = self.next_value;
        self.next_value += 1;
        Ok(vec![ChannelTick {
            channel: self.channel.to_string(),
            value,
        }])
    }
}

#[derive(Clone, Debug)]
struct MergeTransform;

impl MergeTransform {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for MergeTransform {
    type Input = ChannelTick;
    type Output = Merged;

    fn process(&self, tick: ChannelTick) -> Result<Merged, HandlerError> {
        Ok(Merged {
            channel: tick.channel,
            value: tick.value,
        })
    }
}

/// The effectful descendant that makes the fan-in a deterministic orderer.
#[derive(Clone, Debug)]
struct CountingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for CountingEffect {
    const EFFECT_TYPE: &'static str = "resume_mixed.counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = obzenflow_runtime::effects::Portless;

    type Outcome = EffectValue;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "counting"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(EffectValue {
            effect_value: self.value + 1000,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("resume-mixed:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct EffectfulTail {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for EffectfulTail {
    type Input = Merged;
    type Output = obzenflow_core::stage_fact_set![MixedOutput, EffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![CountingEffect];

    async fn process(
        &self,
        input: Merged,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let effect_value = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(MixedOutput {
            channel: input.channel,
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "resume-mixed-effectful-v1"
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl InlineSink for CountingSink {
    type Input = MixedOutput;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _input: MixedOutput) -> Result<(), SinkWriteFailure> {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Ranges for one run: the finite channel f and the infinite channel i.
struct SourceRanges {
    fin_first: u64,
    fin_count: u64,
    inf_first: u64,
    inf_count: u64,
}

fn build_flow(
    journal_base: PathBuf,
    ranges: &SourceRanges,
    calls: Arc<AtomicUsize>,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    let fin_first = ranges.fin_first;
    let fin_count = ranges.fin_count;
    let inf_first = ranges.inf_first;
    let inf_count = ranges.inf_count;
    FlowDefinition::materialize(move |_runtime_config| {
        let finite_handler = FiniteChannelSource::new("f", fin_first, fin_count);
        let infinite_handler = InfiniteChannelSource::new("i", inf_first, inf_count);
        let merge_handler = MergeTransform::new();
        let effectful_handler = EffectfulTail { calls };
        let collect_handler = CountingSink { delivered };

        Ok(flow! {
            name: "resume_mixed",
            journals: disk_journals(journal_base),

            stages: {
                fin = source!(ChannelTick => finite_handler);
                inf = infinite_source!(ChannelTick => infinite_handler);
                merge = transform!(ChannelTick -> Merged => merge_handler);
                effectful = effectful_transform!(
                    Merged -> { MixedOutput, EffectValue } uses CountingEffect => effectful_handler);
                collect = sink!(MixedOutput => collect_handler);
            },

            topology: {
                fin |> merge;
                inf |> merge;
                merge |> effectful;
                effectful |> collect;
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

async fn run_until_delivered(
    journal_base: &Path,
    ranges: &SourceRanges,
    expected: u64,
) -> Result<usize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_flow(
        journal_base.to_path_buf(),
        ranges,
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

/// A journal reduced to data payloads, catch-up watermarks, and authored EOF,
/// in physical append order.
#[derive(Debug, Clone, PartialEq)]
enum MixedRow {
    Data(serde_json::Value),
    CatchUp { stage_key: String, generation: u64 },
    Eof,
}

fn mixed_rows(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<MixedRow> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            payload if payload.consumes_data_credit() => {
                Some(MixedRow::Data(envelope.payload().clone()))
            }
            ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
                generation,
                stage_key,
            }) => Some(MixedRow::CatchUp {
                stage_key: stage_key.to_string(),
                generation: generation.0,
            }),
            ChainPayload::FlowControl(FlowControlPayload::Eof { .. }) => Some(MixedRow::Eof),
            _ => None,
        })
        .collect()
}

fn tick_rows(channel: &str, values: std::ops::RangeInclusive<u64>) -> Vec<MixedRow> {
    values
        .map(|value| {
            MixedRow::Data(json!(ChannelTick {
                channel: channel.to_string(),
                value,
            }))
        })
        .collect()
}

fn merged_payload_set(channel: &str, values: std::ops::RangeInclusive<u64>) -> BTreeSet<String> {
    values
        .map(|value| {
            json!(Merged {
                channel: channel.to_string(),
                value,
            })
            .to_string()
        })
        .collect()
}

fn payload_set(rows: &[MixedRow]) -> BTreeSet<String> {
    rows.iter()
        .filter_map(|row| match row {
            MixedRow::Data(payload) => Some(payload.to_string()),
            _ => None,
        })
        .collect()
}

/// The F17 vacuous-EOF crossing on the path where the finite source's EOF is
/// delivered at the fan-in before the infinite source's watermark: the finite
/// channel is shorter, so its authored EOF takes a lower merge ordinal than
/// the watermark and crosses first.
#[tokio::test(flavor = "multi_thread")]
async fn mixed_sources_resume_flips_the_fan_in_across_a_vacuous_eof_crossing() -> Result<()> {
    // Recorded: f emits 1..=3 then EOF, i emits 101..=104 then idles. All 7
    // recorded inputs deliver (seq-ordered fan-in, FLOWIP-120n F18). Live:
    // i emits 105..=106.
    const RECORDED: u64 = 7;
    const LIVE: u64 = 2;
    let recorded_ranges = SourceRanges {
        fin_first: 1,
        fin_count: 3,
        inf_first: 101,
        inf_count: 4,
    };
    let live_ranges = SourceRanges {
        fin_first: 1,
        fin_count: 3,
        inf_first: 105,
        inf_count: 2,
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let recorded_calls = run_until_delivered(&journal_base, &recorded_ranges, RECORDED).await?;
    assert_eq!(recorded_calls, RECORDED as usize);
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

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
        run_until_delivered(&journal_base, &live_ranges, RECORDED + LIVE).await?
    };
    assert_eq!(
        resumed_calls, LIVE as usize,
        "catch-up re-executes zero effects; only the live tail runs"
    );
    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, resumed_run);

    // The finite source behaves as replay: re-admitted rows, re-authored EOF,
    // no watermark.
    let fin_rows =
        mixed_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "fin").await);
    let mut expected_fin = tick_rows("f", 1..=3);
    expected_fin.push(MixedRow::Eof);
    assert_eq!(
        fin_rows, expected_fin,
        "the finite source re-admits its rows and re-authors EOF, no watermark"
    );

    // The infinite source authors its watermark and continues live.
    let inf_rows =
        mixed_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "inf").await);
    let mut expected_inf = tick_rows("i", 101..=104);
    expected_inf.push(MixedRow::CatchUp {
        stage_key: "inf".to_string(),
        generation: 1,
    });
    expected_inf.extend(tick_rows("i", 105..=106));
    assert_eq!(
        inf_rows, expected_inf,
        "the infinite source authors its watermark between prefix and live tail"
    );

    // The fan-in authors its own single watermark, counting the EOF-exhausted
    // finite input as vacuously crossed (F17), and goes live. Its journal
    // also carries the forwarded finite EOF, so the boundary is located and
    // measured over data rows, not raw indices.
    let merge_rows =
        mixed_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "merge").await);
    let boundary = merge_rows
        .iter()
        .position(|row| {
            matches!(
                row,
                MixedRow::CatchUp { stage_key, generation } if stage_key == "merge" && *generation == 1
            )
        })
        .unwrap_or_else(|| panic!("the fan-in must author its watermark: {merge_rows:#?}"));
    assert_eq!(
        merge_rows
            .iter()
            .filter(|row| matches!(row, MixedRow::CatchUp { .. }))
            .count(),
        1,
        "the fan-in authors exactly one watermark"
    );
    let data_before = merge_rows[..boundary]
        .iter()
        .filter(|row| matches!(row, MixedRow::Data(_)))
        .count();
    assert_eq!(
        data_before as u64, RECORDED,
        "every recorded input is processed before the fan-in's boundary: {merge_rows:#?}"
    );
    assert!(
        merge_rows[..boundary]
            .iter()
            .any(|row| matches!(row, MixedRow::Eof)),
        "the finite EOF crossed the fan-in before its boundary: {merge_rows:#?}"
    );
    let mut expected_recorded = merged_payload_set("f", 1..=3);
    expected_recorded.extend(merged_payload_set("i", 101..=104));
    assert_eq!(
        payload_set(&merge_rows[..boundary]),
        expected_recorded,
        "the pre-boundary outputs are exactly the recorded inputs"
    );
    assert_eq!(
        payload_set(&merge_rows[boundary + 1..]),
        merged_payload_set("i", 105..=106),
        "the post-boundary outputs are exactly the live tail"
    );

    // Catch-up recomputed the recorded delivered order exactly.
    replay_testkit::assert_prefix_stable(&recorded_run, &resumed_run, "merge", &["fin", "inf"])
        .await;

    // The manifest records the entered generation.
    let manifest: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        resumed_run.join("run_manifest.json"),
    )?)?;
    assert_eq!(manifest["resume"]["resume_generation"], 1);

    Ok(())
}

/// The F17 crossing with the finite channel longer than the infinite one.
///
/// Under the seq-ordered merge (FLOWIP-120n F18) the recording no longer
/// blocks on the shorter quiet input, so all 8 recorded inputs deliver, and
/// on resume the fin EOF always precedes the inf watermark at the fan-in (a
/// re-authored EOF orders by inherited journal position, at most the recorded
/// maximum, while the watermark draws a seeded live sequence above it). The
/// original arranged order, watermark consumed while the finite reader is
/// still mid-prefix, is no longer constructible at a source-fed fan-in; this
/// variant keeps the longer-finite shape and the F17 flip coverage.
#[tokio::test(flavor = "multi_thread")]
async fn mixed_sources_resume_flips_when_the_eof_arrives_after_the_watermark() -> Result<()> {
    // Recorded: f emits 1..=5 then EOF, i emits 101..=103 then idles; all 8
    // deliver (no Kahn wait at a source-fed fan-in). Live: i emits 104..=105.
    const RECORDED_DELIVERED: u64 = 8;
    const RECORDED_INPUTS: u64 = 8;
    const LIVE: u64 = 2;
    let recorded_ranges = SourceRanges {
        fin_first: 1,
        fin_count: 5,
        inf_first: 101,
        inf_count: 3,
    };
    let live_ranges = SourceRanges {
        fin_first: 1,
        fin_count: 5,
        inf_first: 104,
        inf_count: 2,
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let recorded_calls =
        run_until_delivered(&journal_base, &recorded_ranges, RECORDED_DELIVERED).await?;
    assert_eq!(recorded_calls, RECORDED_DELIVERED as usize);
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

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
        run_until_delivered(&journal_base, &live_ranges, RECORDED_DELIVERED + LIVE).await?
    };
    assert_eq!(
        resumed_calls, LIVE as usize,
        "recorded outcomes are suppressed; only the live tail executes"
    );
    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, resumed_run);

    // The fan-in must author its watermark: the finite input crossed
    // vacuously at its EOF (F17), the infinite one at its watermark.
    let merge_rows =
        mixed_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "merge").await);
    let boundary = merge_rows
        .iter()
        .position(|row| {
            matches!(
                row,
                MixedRow::CatchUp { stage_key, generation } if stage_key == "merge" && *generation == 1
            )
        })
        .unwrap_or_else(|| panic!("the fan-in must author its watermark: {merge_rows:#?}"));
    let data_before = merge_rows[..boundary]
        .iter()
        .filter(|row| matches!(row, MixedRow::Data(_)))
        .count();
    assert_eq!(
        data_before as u64, RECORDED_INPUTS,
        "every recorded input is processed before the fan-in's boundary: {merge_rows:#?}"
    );
    assert_eq!(
        payload_set(&merge_rows[boundary + 1..]),
        merged_payload_set("i", 104..=105),
        "the post-boundary outputs are exactly the live tail"
    );

    replay_testkit::assert_prefix_stable(&recorded_run, &resumed_run, "merge", &["fin", "inf"])
        .await;

    Ok(())
}

/// Finite source that emits its range, then stalls without exhausting.
#[derive(Clone, Debug)]
struct StallingFiniteSource {
    rows: FiniteChannelSource,
    polls: Arc<AtomicUsize>,
}

impl TypedFiniteSourceHandler for StallingFiniteSource {
    type Output = ChannelTick;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(self.rows.next()?.unwrap_or_default()))
    }
}

#[derive(Clone, Debug)]
struct MergedSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl InlineSink for MergedSink {
    type Input = Merged;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _input: Merged) -> Result<(), SinkWriteFailure> {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn build_stalling_flow(
    journal_base: PathBuf,
    polls: Arc<AtomicUsize>,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let finite_handler = StallingFiniteSource {
            rows: FiniteChannelSource::new("f", 1, 3),
            polls,
        };
        let infinite_handler = InfiniteChannelSource::new("i", 101, 2);
        let merge_handler = MergeTransform::new();
        let collect_handler = MergedSink { delivered };

        Ok(flow! {
            name: "resume_mixed_refusal",
            journals: disk_journals(journal_base),

            stages: {
                fin = source!(ChannelTick => finite_handler);
                inf = infinite_source!(ChannelTick => infinite_handler);
                merge = transform!(ChannelTick -> Merged => merge_handler);
                collect = sink!(Merged => collect_handler);
            },

            topology: {
                fin |> merge;
                inf |> merge;
                merge |> collect;
            }
        })
    })
}

/// FLOWIP-134i B8: a finite direct handler continues as implicit `Refuse`.
/// Stopped before the end of its input, it refuses the resume at admission,
/// named, before any run directory exists or any source polls; the infinite
/// source beside it reattaches and is not named.
#[tokio::test(flavor = "multi_thread")]
async fn an_unfinished_refuse_source_refuses_the_resume_at_admission() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_stalling_flow(
        journal_base.clone(),
        Arc::new(AtomicUsize::new(0)),
        delivered.clone(),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    wait_for_count(&delivered, 5).await?;
    handle.stop_graceful(Duration::from_secs(5)).await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    // The operator stop cut the finite input short (B5).
    let fin_eofs: Vec<EofKind> =
        replay_testkit::read_stage_envelopes_appended(&recorded_run, "fin")
            .await
            .iter()
            .filter_map(|envelope| match &envelope.payload {
                ChainPayload::FlowControl(control) => control.eof_kind(),
                _ => None,
            })
            .collect();
    assert_eq!(fin_eofs, vec![EofKind::Truncated]);

    let runs_before = std::fs::read_dir(journal_base.join("flows"))?.count();
    let resumed_polls = Arc::new(AtomicUsize::new(0));
    let failure = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        build_stalling_flow(
            journal_base.clone(),
            resumed_polls.clone(),
            Arc::new(AtomicU64::new(0)),
        )
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .err()
        .ok_or_else(|| anyhow!("an unfinished Refuse source must refuse the resume"))?
    };
    match &failure.error {
        obzenflow_dsl::dsl::FlowBuildError::ResumeRefusedUnfinishedSources { archive, sources } => {
            assert_eq!(archive, &recorded_run);
            assert_eq!(
                sources,
                &vec!["fin".to_string()],
                "the reattaching source is not named"
            );
        }
        other => panic!("expected the unfinished-source refusal, got: {other}"),
    }
    assert!(failure
        .error
        .to_string()
        .contains("source 'fin' stopped before the end of its input"));
    assert_eq!(
        std::fs::read_dir(journal_base.join("flows"))?.count(),
        runs_before,
        "the refusal leaves no run directory"
    );
    assert_eq!(
        resumed_polls.load(Ordering::SeqCst),
        0,
        "the refused resume never polls a source"
    );

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order {
    id: u64,
}

impl TypedPayload for Order {
    const EVENT_TYPE: &'static str = "resume_mixed.order";
}

#[derive(Clone)]
struct OrderCsv;

impl CsvDecoder for OrderCsv {
    type Output = Order;
}

const ORDERS: u64 = 8;

fn write_orders(path: &Path) -> Result<()> {
    let rows: String = (1..=ORDERS).map(|id| format!("{id}\n")).collect();
    std::fs::write(path, format!("id\n{rows}"))?;
    Ok(())
}

/// Orders read one row per poll, paced by a limiter that admits `burst` rows
/// at once and then `rate` rows a second.
fn build_csv_flow(
    journal_base: PathBuf,
    input: PathBuf,
    rate: f64,
    burst: f64,
    delivered: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let orders_source = CsvSource::builder(OrderCsv)
            .path(input)
            .chunk_size(1)
            .build()
            .expect("valid CSV configuration");
        let collect_handler = replay_testkit::CountDeliveries::<Order>::new(delivered);

        Ok(flow! {
            name: "resume_csv",
            journals: disk_journals(journal_base),

            stages: {
                orders = source!(Order => orders_source with {
                    rate_limit(rate).burst_capacity(burst)
                });
                collect = sink!(Order => collect_handler);
            },

            topology: {
                orders |> collect;
            }
        })
    })
}

async fn wait_for_deliveries(counter: &Arc<AtomicUsize>, target: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while counter.load(Ordering::SeqCst) < target {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for {target} deliveries"))
}

/// Record a run stopped after the limiter's burst, before the end of its input.
async fn record_interrupted_csv_run(journal_base: &Path, input: &Path) -> Result<PathBuf> {
    let delivered = Arc::new(AtomicUsize::new(0));
    let handle = build_csv_flow(
        journal_base.to_path_buf(),
        input.to_path_buf(),
        0.5,
        3.0,
        delivered.clone(),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    wait_for_deliveries(&delivered, 3).await?;
    handle.stop_graceful(Duration::from_secs(10)).await?;
    tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    Ok(replay_testkit::latest_run_dir(journal_base))
}

/// Resume `archive` without pacing; the outcome and the resumed run.
async fn resume_csv_run(
    journal_base: &Path,
    input: &Path,
    archive: &Path,
) -> Result<(Result<(), obzenflow_runtime::errors::FlowError>, PathBuf)> {
    let _bootstrap = install_bootstrap_config(
        replay_testkit::bootstrap_with_archive(ReplayBootstrap {
            archive_path: archive.to_path_buf(),
            allow_incomplete_archive: true,
            allow_duplicate_sink_delivery: false,
            verb: ReplayVerb::Resume,
        })
        .await,
    );
    let handle = build_csv_flow(
        journal_base.to_path_buf(),
        input.to_path_buf(),
        1000.0,
        100.0,
        Arc::new(AtomicUsize::new(0)),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("resumed flow failed to build: {e:?}"))?;
    let outcome = tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the resumed run to finish"))?;
    let resumed = replay_testkit::latest_run_dir(journal_base);
    assert_ne!(resumed, archive, "the resume writes a new run");
    Ok((outcome, resumed))
}

/// A positioned source's journal reduced to its source evidence and records,
/// in append order.
#[derive(Clone, Debug, PartialEq)]
enum SourceRow {
    Acquired(SourceOpenIntent),
    Pinned,
    Positioned(u64),
    StartedOnResume(u64),
    CatchUp,
    Order(u64),
    Eof(EofKind),
    Released(SourceCleanupTrigger, SourceCleanupPhase),
}

async fn source_rows(run: &Path) -> Vec<SourceRow> {
    replay_testkit::read_stage_envelopes_appended(run, "orders")
        .await
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            ChainPayload::Fact(order) => {
                assert!(
                    envelope.envelope.provenance.event.source_progress.is_some(),
                    "every positioned record commits its position"
                );
                Some(SourceRow::Order(order["id"].as_u64().expect("order id")))
            }
            ChainPayload::Execution(ExecutionPayload::SourceReaderAcquired(acquired)) => {
                Some(SourceRow::Acquired(acquired.intent))
            }
            ChainPayload::Execution(ExecutionPayload::SourceInputPinned(_)) => {
                Some(SourceRow::Pinned)
            }
            ChainPayload::Execution(ExecutionPayload::SourcePositioned(fact)) => {
                Some(SourceRow::Positioned(fact.generation))
            }
            ChainPayload::Execution(ExecutionPayload::SourceStartedOnResume(fact)) => {
                Some(SourceRow::StartedOnResume(fact.generation))
            }
            ChainPayload::Execution(ExecutionPayload::SourceCleanupSucceeded(fact)) => {
                Some(SourceRow::Released(fact.trigger, fact.phase))
            }
            ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete { .. }) => {
                Some(SourceRow::CatchUp)
            }
            ChainPayload::FlowControl(control) => control.eof_kind().map(SourceRow::Eof),
            _ => None,
        })
        .collect()
}

async fn source_errors(run: &Path) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = replay_testkit::archive_manifest(run);
    let file = manifest["stages"]["orders"]["error_journal_file"]
        .as_str()
        .expect("the source has an error journal");
    replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run.join(file)).await
}

fn orders(ids: std::ops::RangeInclusive<u64>) -> impl Iterator<Item = SourceRow> {
    ids.map(SourceRow::Order)
}

/// The generations whose reconstruction a run recorded as complete (B6).
async fn reconstruction_completions(run: &Path) -> Vec<u64> {
    let manifest = replay_testkit::archive_manifest(run);
    let file = manifest["system_journal_file"]
        .as_str()
        .expect("the run has a system journal");
    replay_testkit::read_journal_envelopes_appended::<SystemEvent>(&run.join(file))
        .await
        .into_iter()
        .filter_map(|envelope| match envelope.into_parts().1 {
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::ReconstructionCompleted {
                generation,
            }) => Some(generation),
            _ => None,
        })
        .collect()
}

/// The interrupted run's committed orders, 1..=k, after checking its shape.
async fn recorded_prefix(run: &Path) -> u64 {
    let rows = source_rows(run).await;
    let committed = rows
        .iter()
        .filter(|row| matches!(row, SourceRow::Order(_)))
        .count() as u64;
    assert!(
        (1..ORDERS).contains(&committed),
        "the stop must land inside the input: {rows:?}"
    );
    let mut expected = vec![
        SourceRow::Acquired(SourceOpenIntent::Start),
        SourceRow::Pinned,
    ];
    expected.extend(orders(1..=committed));
    expected.extend([
        SourceRow::Eof(EofKind::Truncated),
        SourceRow::Released(SourceCleanupTrigger::Stopped, SourceCleanupPhase::Live),
    ]);
    assert_eq!(rows, expected, "the pin precedes every positioned record");
    committed
}

/// FLOWIP-134i B1, B3: the resumed run re-admits the committed prefix, then
/// positions the reader strictly after it and commits the catch-up marker
/// before the first live row, so every row is admitted exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_csv_source_continues_after_its_last_committed_row() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let input = temp.path().join("orders.csv");
    write_orders(&input)?;

    let recorded_run = record_interrupted_csv_run(&journal_base, &input).await?;
    let committed = recorded_prefix(&recorded_run).await;

    let (outcome, resumed_run) = resume_csv_run(&journal_base, &input, &recorded_run).await?;
    outcome?;
    let mut expected = vec![SourceRow::Pinned];
    expected.extend(orders(1..=committed));
    expected.extend([
        SourceRow::Acquired(SourceOpenIntent::Resume),
        SourceRow::Positioned(1),
        SourceRow::CatchUp,
    ]);
    expected.extend(orders(committed + 1..=ORDERS));
    expected.extend([
        SourceRow::Eof(EofKind::Natural),
        SourceRow::Released(SourceCleanupTrigger::Exhausted, SourceCleanupPhase::Live),
    ]);
    assert_eq!(source_rows(&resumed_run).await, expected);
    assert!(source_errors(&resumed_run).await.is_empty());
    // B6: every stage crossed, so the resumed run alone is resume authority.
    assert!(reconstruction_completions(&recorded_run).await.is_empty());
    assert_eq!(reconstruction_completions(&resumed_run).await, [1]);
    Ok(())
}

/// FLOWIP-134i B10: an input appended after its pin refuses the resume,
/// naming the changed part, before any live row.
#[tokio::test(flavor = "multi_thread")]
async fn a_csv_input_changed_since_its_pin_refuses_the_resume() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let input = temp.path().join("orders.csv");
    write_orders(&input)?;

    let recorded_run = record_interrupted_csv_run(&journal_base, &input).await?;
    let committed = recorded_prefix(&recorded_run).await;
    let mut appended = std::fs::OpenOptions::new().append(true).open(&input)?;
    std::io::Write::write_all(&mut appended, b"9\n")?;

    let (outcome, resumed_run) = resume_csv_run(&journal_base, &input, &recorded_run).await?;
    assert!(outcome.is_err(), "a refused source fails the resumed run");
    let mut expected = vec![SourceRow::Pinned];
    expected.extend(orders(1..=committed));
    expected.extend([
        SourceRow::Acquired(SourceOpenIntent::Resume),
        SourceRow::Released(SourceCleanupTrigger::Failed, SourceCleanupPhase::Recovery),
    ]);
    assert_eq!(source_rows(&resumed_run).await, expected);
    let refusals: Vec<_> = source_errors(&resumed_run)
        .await
        .into_iter()
        .filter_map(|envelope| match envelope.into_parts().1 {
            ChainPayload::Execution(ExecutionPayload::SourceContinuationRefused(fact)) => {
                Some(fact.refusal)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        refusals,
        [SourceContinuationRefusal::InputChanged {
            part: "file".into()
        }]
    );
    assert!(reconstruction_completions(&resumed_run).await.is_empty());
    Ok(())
}

/// FLOWIP-134i B15: a source whose open failed never pinned, so its resume
/// pins the current input and reads it all, once.
#[tokio::test(flavor = "multi_thread")]
async fn a_csv_source_that_never_pinned_starts_on_resume() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let input = temp.path().join("orders.csv");

    let handle = build_csv_flow(
        journal_base.clone(),
        input.clone(),
        1000.0,
        100.0,
        Arc::new(AtomicUsize::new(0)),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    let failed = tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the failed open"))?;
    assert!(failed.is_err(), "a missing input fails the source's open");
    let failed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(source_rows(&failed_run).await, Vec::new());

    write_orders(&input)?;
    let (outcome, resumed_run) = resume_csv_run(&journal_base, &input, &failed_run).await?;
    outcome?;
    let mut expected = vec![
        SourceRow::Acquired(SourceOpenIntent::Resume),
        SourceRow::Pinned,
        SourceRow::StartedOnResume(1),
        SourceRow::CatchUp,
    ];
    expected.extend(orders(1..=ORDERS));
    expected.extend([
        SourceRow::Eof(EofKind::Natural),
        SourceRow::Released(SourceCleanupTrigger::Exhausted, SourceCleanupPhase::Live),
    ]);
    assert_eq!(source_rows(&resumed_run).await, expected);
    assert_eq!(reconstruction_completions(&resumed_run).await, [1]);
    Ok(())
}
