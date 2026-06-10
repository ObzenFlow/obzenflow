// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095d Phase 5: the runtime half of FLOWIP-120a's effectful fan-in
//! envelope (success criterion 8).
//!
//! The effectful stage here IS the fan-in: two sources feed it directly, so
//! its effect cursors key on `input_seq` values assigned by the canonical
//! deterministic merge. The criterion: an effect performed downstream of a
//! deterministically-ordered multi-source fan-in replays with the same
//! recorded outcome and zero re-execution, with identical cursors across
//! live, replay, and replay-of-replay.

mod replay_testkit;

use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::ChainEventContent,
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, FiniteSourceHandler, SinkHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

static ENVELOPE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EnvelopeInput {
    channel: String,
    value: u64,
}

impl TypedPayload for EnvelopeInput {
    const EVENT_TYPE: &'static str = "envelope.input";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EnvelopeEffectValue {
    effect_value: u64,
}

impl TypedPayload for EnvelopeEffectValue {
    const EVENT_TYPE: &'static str = "envelope.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EnvelopeOutput {
    channel: String,
    value: u64,
    effect_value: u64,
}

impl TypedPayload for EnvelopeOutput {
    const EVENT_TYPE: &'static str = "envelope.output";
}

#[derive(Clone, Debug)]
struct ChannelSource {
    channel: &'static str,
    next_index: usize,
    count: usize,
    writer_id: WriterId,
}

impl ChannelSource {
    fn new(channel: &'static str, count: usize) -> Self {
        Self {
            channel,
            next_index: 0,
            count,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for ChannelSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next_index >= self.count {
            return Ok(None);
        }
        self.next_index += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            EnvelopeInput::EVENT_TYPE,
            json!(EnvelopeInput {
                channel: self.channel.to_string(),
                value: self.next_index as u64,
            }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct CountingEffect {
    channel: String,
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for CountingEffect {
    const EFFECT_TYPE: &'static str = "envelope.counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Output = EnvelopeEffectValue;

    fn label(&self) -> &str {
        "counting"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "channel": self.channel, "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(EnvelopeEffectValue {
            effect_value: self.value + 100,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!(
            "envelope:{}:{}",
            self.channel, self.value
        )))
    }
}

/// The effectful stage is itself the multi-inbound fan-in, so its effect
/// cursors key directly on canonical-merge delivered positions.
#[derive(Clone, Debug)]
struct EffectfulMerge {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for EffectfulMerge {
    type Input = EnvelopeInput;

    async fn process(&self, input: EnvelopeInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let effect_value = fx
            .perform(CountingEffect {
                channel: input.channel.clone(),
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(EnvelopeOutput {
            channel: input.channel,
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "envelope-merge-v1"
    }
}

#[derive(Clone, Debug)]
struct DropSink;

#[async_trait]
impl SinkHandler for DropSink {
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> Result<obzenflow_core::event::payloads::delivery_payload::DeliveryPayload, HandlerError>
    {
        Ok(
            obzenflow_core::event::payloads::delivery_payload::DeliveryPayload::success(
                "drop",
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
                None,
            ),
        )
    }
}

fn build_flow(journal_base: PathBuf, calls: Arc<AtomicUsize>) -> FlowDefinition {
    flow! {
        name: "effectful_fan_in_envelope",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            source_a = source!(EnvelopeInput => ChannelSource::new("a", 4));
            source_b = source!(EnvelopeInput => ChannelSource::new("b", 3));
            effectful_merge = effectful_transform!(
                EnvelopeInput -> { EnvelopeOutput, EnvelopeEffectValue } => EffectfulMerge { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collector = sink!(EnvelopeOutput => DropSink);
        },

        topology: {
            source_a |> effectful_merge;
            source_b |> effectful_merge;
            effectful_merge |> collector;
        }
    }
}

/// The cursor-and-outcome witness for one run: every effect-provenance row of
/// the effectful stage projected to `(input_seq, effect_ordinal, event_type,
/// payload)`, in journal order. Identical across runs when the fan-in order
/// reproduces; ids and envelope metadata legitimately differ until 120o.
async fn effect_cursor_projection(
    run_dir: &Path,
    stage_key: &str,
) -> Vec<(u64, u32, String, String)> {
    replay_testkit::read_stage_envelopes(run_dir, stage_key)
        .await
        .into_iter()
        .filter_map(|envelope| {
            let event = envelope.event;
            if !matches!(event.content, ChainEventContent::Data { .. }) {
                return None;
            }
            let provenance = event.effect_provenance.as_ref()?;
            let payload = match &event.content {
                ChainEventContent::Data { payload, .. } => {
                    serde_json::to_string(payload).unwrap_or_default()
                }
                _ => String::new(),
            };
            Some((
                provenance.cursor.input_seq.get(),
                provenance.cursor.effect_ordinal.get(),
                event.event_type(),
                payload,
            ))
        })
        .collect()
}

async fn run_live(flow: FlowDefinition) {
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow)
        .await
        .expect("live flow should complete");
}

async fn run_replay(archive: &Path, flow: FlowDefinition) {
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive.as_os_str().to_os_string(),
        ])
        .run_async(flow)
        .await
        .expect("replay flow should complete");
}

/// FLOWIP-095d success criterion 8: an effect performed in a stage downstream
/// of a deterministically-ordered multi-source fan-in replays with the same
/// recorded outcome and zero re-execution.
#[tokio::test]
async fn effect_below_canonical_fan_in_replays_with_recorded_outcome_and_zero_executions() {
    let _guard = ENVELOPE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    run_live(build_flow(journal_base.clone(), live_calls.clone())).await;
    assert_eq!(live_calls.load(Ordering::SeqCst), 7);
    let live_run = replay_testkit::latest_run_dir(&journal_base);
    let live_cursors = effect_cursor_projection(&live_run, "effectful_merge").await;
    assert!(
        !live_cursors.is_empty(),
        "live run must record effect-provenance facts"
    );

    let replay_calls = Arc::new(AtomicUsize::new(0));
    run_replay(
        &live_run,
        build_flow(journal_base.clone(), replay_calls.clone()),
    )
    .await;
    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "replay must return recorded outcomes without executing the effect"
    );
    let replay_run = replay_testkit::latest_run_dir(&journal_base);
    let replay_cursors = effect_cursor_projection(&replay_run, "effectful_merge").await;
    assert_eq!(
        live_cursors, replay_cursors,
        "effect cursors and outcome facts must reproduce below the canonical fan-in"
    );

    // Replay of the replay: cursor closure holds at any depth.
    let second_replay_calls = Arc::new(AtomicUsize::new(0));
    run_replay(
        &replay_run,
        build_flow(journal_base.clone(), second_replay_calls.clone()),
    )
    .await;
    assert_eq!(second_replay_calls.load(Ordering::SeqCst), 0);
    let second_replay_run = replay_testkit::latest_run_dir(&journal_base);
    let second_cursors = effect_cursor_projection(&second_replay_run, "effectful_merge").await;
    assert_eq!(replay_cursors, second_cursors);
}

/// The FLOWIP-120n hook: multi-source catch-up relies on the canonical merge
/// recomputing the recorded prefix order. Until 120n's live continuation
/// exists, replay is the degenerate catch-up (the whole archive is the
/// prefix); 120n's multi-source resume test extends this with a live tail
/// behind its all-sources admission barrier.
#[tokio::test]
async fn multi_source_prefix_order_reproduced_for_resume_catch_up() {
    let _guard = ENVELOPE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    run_live(build_flow(journal_base.clone(), live_calls.clone())).await;
    let live_run = replay_testkit::latest_run_dir(&journal_base);

    let replay_calls = Arc::new(AtomicUsize::new(0));
    run_replay(
        &live_run,
        build_flow(journal_base.clone(), replay_calls.clone()),
    )
    .await;
    let replay_run = replay_testkit::latest_run_dir(&journal_base);

    replay_testkit::assert_prefix_stable(
        &live_run,
        &replay_run,
        "effectful_merge",
        &["source_a", "source_b"],
    )
    .await;
}
