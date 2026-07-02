// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-J: catch-up failure aborts the resume loudly.
//!
//! Records a two-source fan-in flow, corrupts one source's archived journal
//! mid-file, and resumes. The corrupted source's catch-up fails, it never
//! authors its watermark, and the resume aborts through normal stage-failure
//! propagation (FLOWIP-120n F5). No live input is admitted through the
//! fan-in: the merge blocks on the failed source's recorded generation, so
//! its journal shows no live-range outputs and no authored watermark, the
//! effect boundary executes nothing, and the sink delivers nothing. The
//! healthy source's own journal is deliberately not asserted live-free: the
//! per-stage flip is local by design and the fan-in is where admission is
//! blocked.

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
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
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
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChannelTick {
    channel: String,
    value: u64,
}

impl TypedPayload for ChannelTick {
    const EVENT_TYPE: &'static str = "resume_abort.tick";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Merged {
    channel: String,
    value: u64,
}

impl TypedPayload for Merged {
    const EVENT_TYPE: &'static str = "resume_abort.merged";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EffectValue {
    effect_value: u64,
}

impl TypedPayload for EffectValue {
    const EVENT_TYPE: &'static str = "resume_abort.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AbortOutput {
    channel: String,
    value: u64,
    effect_value: u64,
}

impl TypedPayload for AbortOutput {
    const EVENT_TYPE: &'static str = "resume_abort.output";
}

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

#[derive(Clone, Debug)]
struct CountingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for CountingEffect {
    const EFFECT_TYPE: &'static str = "resume_abort.counting";
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
        Some(IdempotencyKey(format!("resume-abort:{}", self.value)))
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
        fx.emit(AbortOutput {
            channel: input.channel,
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "resume-abort-effectful-v1"
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if AbortOutput::from_event(&event).is_some() {
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
        Ok(DeliveryPayload::success(
            "resume_abort_sink",
            DeliveryMethod::Noop,
            None,
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

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
        name: "resume_abort",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            src_a = infinite_source!(ChannelTick => ChannelTicker::new("a", a_first, a_count));
            src_b = infinite_source!(ChannelTick => ChannelTicker::new("b", b_first, b_count));
            merge = transform!(ChannelTick -> Merged => MergeTransform::new());
            effectful = effectful_transform!(
                Merged -> { AbortOutput, EffectValue } => EffectfulTail { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collect = sink!(AbortOutput => CountingSink { delivered });
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

/// Wait until the pipeline reports Failed; return the failure reason.
async fn wait_for_failed(handle: &FlowHandle) -> Result<String> {
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let PipelineState::Failed { reason, .. } = &*rx.borrow() {
                return Ok::<_, anyhow::Error>(reason.clone());
            }
            rx.changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for the resume to fail loudly"))?
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

#[tokio::test(flavor = "multi_thread")]
async fn corrupted_source_archive_aborts_the_resume_before_any_live_admission() -> Result<()> {
    // Recorded: a emits 1..=4, b emits 101..=104; the Kahn merge delivers 7
    // of the 8 before blocking on a quiet input, the recording is cancelled.
    const RECORDED_DELIVERED: u64 = 7;
    let recorded_ranges = SourceRanges {
        a_first: 1,
        a_count: 4,
        b_first: 101,
        b_count: 4,
    };
    let live_ranges = SourceRanges {
        a_first: 5,
        a_count: 2,
        b_first: 105,
        b_count: 2,
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    // Record and cancel.
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.clone(),
            &recorded_ranges,
            calls.clone(),
            delivered.clone(),
        )
        .await
        .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        wait_for_count(&delivered, RECORDED_DELIVERED).await?;
        handle.stop().await?;
        tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    }
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    // Corrupt src_b's archived journal mid-file: mangle the frame carrying
    // b:102 while later frames stay intact, so the failure is unambiguous
    // corruption rather than a tolerated torn tail (FLOWIP-120q).
    let manifest: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        recorded_run.join("run_manifest.json"),
    )?)?;
    let src_b_journal = manifest["stages"]["src_b"]["data_journal_file"]
        .as_str()
        .expect("manifest names src_b's data journal");
    let journal_path = recorded_run.join(src_b_journal);
    let original = std::fs::read_to_string(&journal_path)?;
    let corrupted: String = original
        .lines()
        .map(|line| {
            if line.contains("\"value\":102") {
                "garbage-that-is-not-a-frame".to_string()
            } else {
                line.to_string()
            }
        })
        .map(|line| format!("{line}\n"))
        .collect();
    assert_ne!(
        original, corrupted,
        "the b:102 frame must have been mangled"
    );
    std::fs::write(&journal_path, corrupted)?;

    // Resume: the corrupted source's catch-up must fail loudly, and no live
    // input may cross the fan-in.
    let calls = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let failure_reason = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        let handle = build_flow(
            journal_base.clone(),
            &live_ranges,
            calls.clone(),
            delivered.clone(),
        )
        .await
        .map_err(|e| anyhow!("resume flow failed to build: {e:?}"))?;
        let reason = wait_for_failed(&handle).await?;
        // Teardown after failure; stop is an idempotent no-op if the
        // supervisor already terminated.
        let _ = handle.stop().await;
        let _ = tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion()).await;
        reason
    };
    assert!(
        failure_reason.contains("src_b"),
        "the failure must name the corrupted source's stage: {failure_reason}"
    );

    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, resumed_run);

    // No live input was admitted through the fan-in: every merge output is a
    // recorded-range payload, and the merge never authored a watermark (its
    // corrupted input never crossed, F5).
    let merge_rows =
        resume_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "merge").await);
    assert!(
        !merge_rows
            .iter()
            .any(|row| matches!(row, ResumeRow::CatchUp { .. })),
        "the fan-in must not author a watermark when one input's catch-up failed: {merge_rows:#?}"
    );
    let recorded_payloads: BTreeSet<String> = (1..=4)
        .map(|value| {
            json!(Merged {
                channel: "a".to_string(),
                value,
            })
            .to_string()
        })
        .chain((101..=104).map(|value| {
            json!(Merged {
                channel: "b".to_string(),
                value,
            })
            .to_string()
        }))
        .collect();
    for row in &merge_rows {
        let ResumeRow::Data(payload) = row else {
            continue;
        };
        assert!(
            recorded_payloads.contains(&payload.to_string()),
            "the fan-in admitted a non-recorded input during a failed catch-up: {payload}"
        );
    }

    // The corrupted source never crossed: no watermark and no live rows in
    // its journal.
    let src_b_rows =
        resume_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "src_b").await);
    assert!(
        !src_b_rows
            .iter()
            .any(|row| matches!(row, ResumeRow::CatchUp { .. })),
        "a source whose catch-up failed never authors its watermark: {src_b_rows:#?}"
    );
    assert!(
        !src_b_rows.iter().any(|row| {
            matches!(row, ResumeRow::Data(payload)
                if payload["value"].as_u64().is_some_and(|value| value >= 105))
        }),
        "the corrupted source must not reach its live phase: {src_b_rows:#?}"
    );

    // Nothing reached the effect boundary as live work: every delivered
    // position lay inside the recorded prefix, so outcomes read from history.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no effect executed during the failed catch-up"
    );
    // The sink re-consumes the recorded prefix during catch-up (F14), so rows
    // whose admission seq precedes the corrupt frame may reach it before
    // teardown lands; how many is scheduling-dependent. The merge-row
    // assertions above prove nothing outside the recorded prefix moved.
    assert!(
        delivered.load(Ordering::SeqCst) <= RECORDED_DELIVERED,
        "only recorded-prefix re-consumption may reach the sink during a failed catch-up (saw {})",
        delivered.load(Ordering::SeqCst)
    );

    Ok(())
}
