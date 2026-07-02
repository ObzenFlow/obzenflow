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

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEventContent, EventEnvelope};
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{
    effectful_transform, flow, infinite_source, sink, source, transform, FlowDefinition,
};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::{
    install_bootstrap_config, BootstrapConfig, ReplayBootstrap, ReplayVerb,
};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey, SinkDeliverySafety,
};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, FiniteSourceHandler, InfiniteSourceHandler, SinkHandler,
    TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
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
    writer_id: WriterId,
    next_value: u64,
    remaining: u64,
}

impl FiniteChannelSource {
    fn new(channel: &'static str, first_value: u64, count: u64) -> Self {
        Self {
            channel,
            writer_id: WriterId::from(StageId::new()),
            next_value: first_value,
            remaining: count,
        }
    }
}

impl FiniteSourceHandler for FiniteChannelSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let value = self.next_value;
        self.next_value += 1;
        Ok(Some(vec![ChannelTick {
            channel: self.channel.to_string(),
            value,
        }
        .to_event(self.writer_id)]))
    }
}

/// Infinite source: emits `count` values starting at `first_value`, then
/// idles forever.
#[derive(Clone, Debug)]
struct InfiniteChannelSource {
    channel: &'static str,
    writer_id: WriterId,
    next_value: u64,
    remaining: u64,
}

impl InfiniteChannelSource {
    fn new(channel: &'static str, first_value: u64, count: u64) -> Self {
        Self {
            channel,
            writer_id: WriterId::from(StageId::new()),
            next_value: first_value,
            remaining: count,
        }
    }
}

impl InfiniteSourceHandler for InfiniteChannelSource {
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.remaining -= 1;
        let value = self.next_value;
        self.next_value += 1;
        Ok(vec![ChannelTick {
            channel: self.channel.to_string(),
            value,
        }
        .to_event(self.writer_id)])
    }
}

#[derive(Clone, Debug)]
struct MergeTransform {
    writer_id: WriterId,
}

impl MergeTransform {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for MergeTransform {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let Some(tick) = ChannelTick::from_event(&event) else {
            return Ok(Vec::new());
        };
        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            Merged::EVENT_TYPE,
            json!(Merged {
                channel: tick.channel,
                value: tick.value,
            }),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
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

    type Outcome = EffectValue;

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

    async fn process(&self, input: Merged, fx: &mut Effects) -> Result<(), HandlerError> {
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
        Ok(())
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
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if MixedOutput::from_event(&event).is_some() {
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
        Ok(DeliveryPayload::success(
            "resume_mixed_sink",
            DeliveryMethod::Noop,
            None,
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
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
    flow! {
        name: "resume_mixed",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            fin = source!(ChannelTick => FiniteChannelSource::new("f", fin_first, fin_count));
            inf = infinite_source!(ChannelTick => InfiniteChannelSource::new("i", inf_first, inf_count));
            merge = transform!(ChannelTick -> Merged => MergeTransform::new());
            effectful = effectful_transform!(
                Merged -> { MixedOutput, EffectValue } => EffectfulTail { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collect = sink!(MixedOutput => CountingSink { delivered });
        },

        topology: {
            fin |> merge;
            inf |> merge;
            merge |> effectful;
            effectful |> collect;
        }
    }
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

fn mixed_rows(envelopes: &[EventEnvelope<ChainEvent>]) -> Vec<MixedRow> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Data { .. } => {
                Some(MixedRow::Data(envelope.event.payload().clone()))
            }
            ChainEventContent::FlowControl(FlowControlPayload::CatchUpComplete {
                generation,
                stage_key,
            }) => Some(MixedRow::CatchUp {
                stage_key: stage_key.to_string(),
                generation: generation.0,
            }),
            ChainEventContent::FlowControl(FlowControlPayload::Eof { .. }) => Some(MixedRow::Eof),
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
        let _bootstrap = install_bootstrap_config(BootstrapConfig {
            replay: Some(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            }),
            ..BootstrapConfig::default()
        });
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
        let _bootstrap = install_bootstrap_config(BootstrapConfig {
            replay: Some(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            }),
            ..BootstrapConfig::default()
        });
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
