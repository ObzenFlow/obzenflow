// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-J: multi-source resume with deterministic fan-in ordering.
//!
//! Two infinite sources feed a fan-in transform whose effectful descendant
//! triggers the FLOWIP-095d enablement walk, so the fan-in runs the canonical
//! deterministic merge with generation as its coarsest axis. The flow is
//! recorded with both sources producing, cancelled, then resumed with both
//! producing live. Each source authors its own catch-up watermark; the fan-in
//! delivers every recorded-prefix input before any live-tail input and
//! authors its own single watermark after both inputs crossed. All assertions
//! read the event-sourced journals.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEventContent, EventEnvelope};
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{effectful_transform, flow, infinite_source, sink, transform, FlowDefinition};
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
    EffectfulTransformHandler, InfiniteSourceHandler, SinkHandler, TransformHandler,
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
    const EVENT_TYPE: &'static str = "resume_fan_in.tick";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Merged {
    channel: String,
    value: u64,
}

impl TypedPayload for Merged {
    const EVENT_TYPE: &'static str = "resume_fan_in.merged";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EffectValue {
    effect_value: u64,
}

impl TypedPayload for EffectValue {
    const EVENT_TYPE: &'static str = "resume_fan_in.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FanInOutput {
    channel: String,
    value: u64,
    effect_value: u64,
}

impl TypedPayload for FanInOutput {
    const EVENT_TYPE: &'static str = "resume_fan_in.output";
}

/// Emits `count` channel ticks starting at `first_value`, then idles forever.
#[derive(Clone, Debug)]
struct ChannelTicker {
    channel: &'static str,
    writer_id: WriterId,
    next_value: u64,
    remaining: u64,
}

impl ChannelTicker {
    fn new(channel: &'static str, first_value: u64, count: u64) -> Self {
        Self {
            channel,
            writer_id: WriterId::from(StageId::new()),
            next_value: first_value,
            remaining: count,
        }
    }
}

impl InfiniteSourceHandler for ChannelTicker {
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

/// Pure 1:1 fan-in: one `Merged` per consumed tick, so the delivered order is
/// fully witnessed by this stage's output journal.
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

/// The effectful descendant that makes the fan-in a deterministic orderer
/// (FLOWIP-095d enablement walk).
#[derive(Clone, Debug)]
struct CountingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for CountingEffect {
    const EFFECT_TYPE: &'static str = "resume_fan_in.counting";
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
        Some(IdempotencyKey(format!("resume-fan-in:{}", self.value)))
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
        fx.emit(FanInOutput {
            channel: input.channel,
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "resume-fan-in-effectful-v1"
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if FanInOutput::from_event(&event).is_some() {
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
        Ok(DeliveryPayload::success(
            "resume_fan_in_sink",
            DeliveryMethod::Noop,
            None,
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

/// Source ranges for one run: channel a produces `a_count` values starting at
/// `a_first`, channel b likewise, disjoint so effect identities never collide.
struct SourceRanges {
    a_first: u64,
    a_count: u64,
    b_first: u64,
    b_count: u64,
}

fn build_flow(
    journal_base: PathBuf,
    ranges: &SourceRanges,
    calls: Arc<AtomicUsize>,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    let a_first = ranges.a_first;
    let a_count = ranges.a_count;
    let b_first = ranges.b_first;
    let b_count = ranges.b_count;
    flow! {
        name: "resume_fan_in",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            src_a = infinite_source!(ChannelTick => ChannelTicker::new("a", a_first, a_count));
            src_b = infinite_source!(ChannelTick => ChannelTicker::new("b", b_first, b_count));
            merge = transform!(ChannelTick -> Merged => MergeTransform::new());
            effectful = effectful_transform!(
                Merged -> { FanInOutput, EffectValue } => EffectfulTail { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collect = sink!(FanInOutput => CountingSink { delivered });
        },

        topology: {
            src_a |> merge;
            src_b |> merge;
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

/// Run one flow instance until the sink consumed `expected` outputs, then
/// stop. Returns the effect-call count observed by this run's handlers.
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

#[derive(Debug, Clone, PartialEq)]
enum ResumeRow {
    Data(serde_json::Value),
    CatchUp { stage_key: String, generation: u64 },
}

fn resume_rows(envelopes: &[EventEnvelope<ChainEvent>]) -> Vec<ResumeRow> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Data { .. } => {
                Some(ResumeRow::Data(envelope.event.payload().clone()))
            }
            ChainEventContent::FlowControl(FlowControlPayload::CatchUpComplete {
                generation,
                stage_key,
            }) => Some(ResumeRow::CatchUp {
                stage_key: stage_key.to_string(),
                generation: generation.0,
            }),
            _ => None,
        })
        .collect()
}

fn tick_rows(channel: &str, values: std::ops::RangeInclusive<u64>) -> Vec<ResumeRow> {
    values
        .map(|value| {
            ResumeRow::Data(json!(ChannelTick {
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

#[tokio::test(flavor = "multi_thread")]
async fn multi_source_resume_orders_every_recorded_input_before_any_live_input() -> Result<()> {
    // Recorded: a emits 1..=4, b emits 101..=104. Live: a 5..=7, b 105..=106.
    //
    // This fan-in's inputs are all source journals, so it runs the seq-ordered
    // merge (FLOWIP-120n F18): a quiet input at the entered generation no
    // longer blocks delivery, so the recording delivers all 8 recorded inputs
    // and the resumed live phase delivers the full unequal live ranges. The
    // resume catch-up keeps the wait (readers below the entered generation)
    // and recomputes the recorded order from the re-admitted sequences.
    const RECORDED_INPUTS: u64 = 8;
    const RECORDED_DELIVERED: u64 = 8;
    const LIVE_DELIVERED: u64 = 5;
    let recorded_ranges = SourceRanges {
        a_first: 1,
        a_count: 4,
        b_first: 101,
        b_count: 4,
    };
    let live_ranges = SourceRanges {
        a_first: 5,
        a_count: 3,
        b_first: 105,
        b_count: 2,
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    // Record with both sources producing, then cancel.
    let recorded_calls =
        run_until_delivered(&journal_base, &recorded_ranges, RECORDED_DELIVERED).await?;
    assert_eq!(
        recorded_calls, RECORDED_DELIVERED as usize,
        "the recording executes every delivered effect live"
    );
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    // The fan-in ran the canonical merge during recording: its delivered
    // order is the content-deterministic alternation, not arrival order.
    let recorded_merge =
        resume_rows(&replay_testkit::read_stage_envelopes_appended(&recorded_run, "merge").await);
    assert_eq!(recorded_merge.len() as u64, RECORDED_DELIVERED);

    // Resume with both sources producing live. The sink re-consumes the
    // recorded prefix during catch-up (F14), so the wait target is
    // prefix + live; a live-only target races the stop into catch-up.
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
        run_until_delivered(
            &journal_base,
            &live_ranges,
            RECORDED_DELIVERED + LIVE_DELIVERED,
        )
        .await?
    };
    assert_eq!(
        resumed_calls, LIVE_DELIVERED as usize,
        "recorded outcomes are suppressed; only the live tail executes"
    );
    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, resumed_run);

    // Each source journal carries its own authored watermark between its
    // recorded prefix and its live tail.
    let src_a =
        resume_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "src_a").await);
    let mut expected_a = tick_rows("a", 1..=4);
    expected_a.push(ResumeRow::CatchUp {
        stage_key: "src_a".to_string(),
        generation: 1,
    });
    expected_a.extend(tick_rows("a", 5..=7));
    assert_eq!(
        src_a, expected_a,
        "src_a journal: prefix, watermark, live tail"
    );

    let src_b =
        resume_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "src_b").await);
    let mut expected_b = tick_rows("b", 101..=104);
    expected_b.push(ResumeRow::CatchUp {
        stage_key: "src_b".to_string(),
        generation: 1,
    });
    expected_b.extend(tick_rows("b", 105..=106));
    assert_eq!(
        src_b, expected_b,
        "src_b journal: prefix, watermark, live tail"
    );

    // The fan-in's journal witnesses the delivered order: every
    // recorded-prefix input before its single authored watermark, every
    // live-tail input after it.
    let merge_rows =
        resume_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "merge").await);
    let watermark_positions: Vec<usize> = merge_rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| match row {
            ResumeRow::CatchUp {
                stage_key,
                generation,
            } => {
                assert_eq!(stage_key, "merge", "the fan-in authors its own marker");
                assert_eq!(*generation, 1);
                Some(index)
            }
            ResumeRow::Data(_) => None,
        })
        .collect();
    assert_eq!(
        watermark_positions.len(),
        1,
        "the fan-in authors exactly one watermark: {merge_rows:#?}"
    );
    let boundary = watermark_positions[0];
    assert_eq!(
        boundary as u64, RECORDED_INPUTS,
        "every recorded input, including the in-flight tail, is processed before \
         the fan-in's boundary"
    );
    assert_eq!(
        merge_rows.len() as u64,
        RECORDED_INPUTS + 1 + LIVE_DELIVERED,
        "the deliverable live tail follows the boundary: {merge_rows:#?}"
    );

    let payload_set = |rows: &[ResumeRow]| -> BTreeSet<String> {
        rows.iter()
            .filter_map(|row| match row {
                ResumeRow::Data(payload) => Some(payload.to_string()),
                ResumeRow::CatchUp { .. } => None,
            })
            .collect()
    };
    let mut expected_recorded = merged_payload_set("a", 1..=4);
    expected_recorded.extend(merged_payload_set("b", 101..=104));
    assert_eq!(
        payload_set(&merge_rows[..boundary]),
        expected_recorded,
        "the pre-boundary outputs are exactly the recorded inputs"
    );
    // Seq mode delivers the full unequal live ranges: quiet b never blocks a.
    let mut expected_live = merged_payload_set("a", 5..=7);
    expected_live.extend(merged_payload_set("b", 105..=106));
    assert_eq!(
        payload_set(&merge_rows[boundary + 1..]),
        expected_live,
        "the post-boundary outputs are exactly the deliverable live tail"
    );

    // The catch-up recomputed the recorded delivered order exactly
    // (FLOWIP-120n prefix stability over the FLOWIP-095d canonical merge).
    replay_testkit::assert_prefix_stable(&recorded_run, &resumed_run, "merge", &["src_a", "src_b"])
        .await;

    Ok(())
}
