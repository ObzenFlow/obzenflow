// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095d Phase 4: deterministic fan-in delivery order across live,
//! replay, and replay-of-replay.
//!
//! The flow under test fans two finite sources into a pure merge transform
//! whose effectful descendant triggers the build-time enablement walk, so the
//! merge runs the canonical deterministic merge. Delivery order is asserted as
//! delivered-order projections from the event-sourced journals; raw journal
//! bytes legitimately differ across runs until FLOWIP-120o.

mod replay_testkit;

use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl::{effectful_transform, flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

static FAN_IN_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FanInInput {
    channel: String,
    value: u64,
}

impl TypedPayload for FanInInput {
    const EVENT_TYPE: &'static str = "fan_in.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MergedRecord {
    channel: String,
    value: u64,
}

impl TypedPayload for MergedRecord {
    const EVENT_TYPE: &'static str = "fan_in.merged";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FanInEffectValue {
    effect_value: u64,
}

impl TypedPayload for FanInEffectValue {
    const EVENT_TYPE: &'static str = "fan_in.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FanInOutput {
    channel: String,
    value: u64,
    effect_value: u64,
}

impl TypedPayload for FanInOutput {
    const EVENT_TYPE: &'static str = "fan_in.output";
}

#[derive(Clone, Debug)]
struct ChannelSource {
    channel: &'static str,
    next_index: usize,
    count: usize,
    jitter_ms: u64,
    writer_id: WriterId,
}

impl ChannelSource {
    fn new(channel: &'static str, count: usize) -> Self {
        Self::with_jitter(channel, count, 0)
    }

    /// Deterministic pseudo-jitter per (channel, index): varies arrival
    /// timing without varying stream content, so two live runs with
    /// different jitter settings must still merge identically.
    fn with_jitter(channel: &'static str, count: usize, jitter_ms: u64) -> Self {
        Self {
            channel,
            next_index: 0,
            count,
            jitter_ms,
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
        if self.jitter_ms > 0 {
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
            for byte in self.channel.bytes().chain(self.next_index.to_le_bytes()) {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
            std::thread::sleep(std::time::Duration::from_millis(
                hash % (self.jitter_ms + 1),
            ));
        }
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            FanInInput::EVENT_TYPE,
            json!(FanInInput {
                channel: self.channel.to_string(),
                value: self.next_index as u64,
            }),
        )]))
    }
}

/// Pure 1:1 merge: one `MergedRecord` per consumed input, so the delivered
/// order is fully witnessed by the output journal.
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
        let Some(input) = FanInInput::from_event(&event) else {
            return Ok(Vec::new());
        };
        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            MergedRecord::EVENT_TYPE,
            json!(MergedRecord {
                channel: input.channel,
                value: input.value,
            }),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Pure 1:1 tap used by the skip-level topology: re-emits each input so the
/// merge sees both the original event and a derived sibling.
#[derive(Clone, Debug)]
struct TapTransform {
    writer_id: WriterId,
}

impl TapTransform {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for TapTransform {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let Some(input) = FanInInput::from_event(&event) else {
            return Ok(Vec::new());
        };
        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            FanInInput::EVENT_TYPE,
            json!(FanInInput {
                channel: format!("tap:{}", input.channel),
                value: input.value,
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
    const EFFECT_TYPE: &'static str = "fan_in.counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Output = FanInEffectValue;

    fn label(&self) -> &str {
        "counting"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(FanInEffectValue {
            effect_value: self.value + 100,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("fan-in-counting:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct EffectfulTail {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for EffectfulTail {
    type Input = MergedRecord;

    async fn process(&self, input: MergedRecord, fx: &mut Effects) -> Result<(), HandlerError> {
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
        "fan-in-effectful-v1"
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

/// Two channels of unequal length fan into the merge; the effectful tail
/// triggers the FLOWIP-095d enablement walk, so `merge` runs the canonical
/// deterministic merge.
fn build_two_channel_flow(journal_base: PathBuf, calls: Arc<AtomicUsize>) -> FlowDefinition {
    build_two_channel_flow_with_jitter(journal_base, calls, 0)
}

fn build_two_channel_flow_with_jitter(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    jitter_ms: u64,
) -> FlowDefinition {
    flow! {
        name: "deterministic_fan_in",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            source_a = source!(FanInInput => ChannelSource::with_jitter("a", 4, jitter_ms));
            source_b = source!(FanInInput => ChannelSource::with_jitter("b", 3, jitter_ms));
            merge = transform!(FanInInput -> MergedRecord => MergeTransform::new());
            effectful = effectful_transform!(
                MergedRecord -> { FanInOutput, FanInEffectValue } => EffectfulTail { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collector = sink!(FanInOutput => DropSink);
        },

        topology: {
            source_a |> merge;
            source_b |> merge;
            merge |> effectful;
            effectful |> collector;
        }
    }
}

/// Skip-level topology: `source_a` feeds both the tap and the merge directly,
/// so the merge's two inputs are causally related and the canonical merge must
/// deliver each original before the tap sibling derived from it.
fn build_skip_level_flow(journal_base: PathBuf, calls: Arc<AtomicUsize>) -> FlowDefinition {
    flow! {
        name: "deterministic_fan_in_skip_level",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            source_a = source!(FanInInput => ChannelSource::new("a", 4));
            tap = transform!(FanInInput -> FanInInput => TapTransform::new());
            merge = transform!(FanInInput -> MergedRecord => MergeTransform::new());
            effectful = effectful_transform!(
                MergedRecord -> { FanInOutput, FanInEffectValue } => EffectfulTail { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collector = sink!(FanInOutput => DropSink);
        },

        topology: {
            source_a |> tap;
            source_a |> merge;
            tap |> merge;
            merge |> effectful;
            effectful |> collector;
        }
    }
}

async fn run_live(journal_base: &PathBuf, flow: FlowDefinition) {
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow)
        .await
        .expect("live flow should complete");
    let _ = journal_base;
}

async fn run_replay(archive: &std::path::Path, flow: FlowDefinition) {
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

#[tokio::test]
async fn fan_in_delivery_order_identical_across_live_replay_and_replay_of_replay() {
    let _guard = FAN_IN_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    run_live(
        &journal_base,
        build_two_channel_flow(journal_base.clone(), live_calls.clone()),
    )
    .await;
    assert_eq!(live_calls.load(Ordering::SeqCst), 7);
    let live_run = replay_testkit::latest_run_dir(&journal_base);

    // The canonical tiebreak alternates the two channels fairly and streams
    // the longer channel's tail once the shorter one seals.
    let live_projection =
        replay_testkit::project_delivered_order(&live_run, "merge", &["source_a", "source_b"])
            .await;
    assert_eq!(
        live_projection.consumption_sequence(),
        vec![
            ("source_a".to_string(), 1),
            ("source_b".to_string(), 1),
            ("source_a".to_string(), 2),
            ("source_b".to_string(), 2),
            ("source_a".to_string(), 3),
            ("source_b".to_string(), 3),
            ("source_a".to_string(), 4),
        ],
        "live fan-in delivery must follow the canonical alternation"
    );

    // Replay the live run.
    let replay_calls = Arc::new(AtomicUsize::new(0));
    run_replay(
        &live_run,
        build_two_channel_flow(journal_base.clone(), replay_calls.clone()),
    )
    .await;
    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "replay must not execute effects"
    );
    let replay_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(live_run, replay_run);
    replay_testkit::assert_same_delivered_order(
        &live_run,
        &replay_run,
        "merge",
        &["source_a", "source_b"],
    )
    .await;

    // Replay the replay: the archive is deterministic by construction.
    let second_replay_calls = Arc::new(AtomicUsize::new(0));
    run_replay(
        &replay_run,
        build_two_channel_flow(journal_base.clone(), second_replay_calls.clone()),
    )
    .await;
    assert_eq!(second_replay_calls.load(Ordering::SeqCst), 0);
    let second_replay_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(replay_run, second_replay_run);
    replay_testkit::assert_same_delivered_order(
        &replay_run,
        &second_replay_run,
        "merge",
        &["source_a", "source_b"],
    )
    .await;
}

/// Arrival-timing independence: two LIVE runs with different per-source
/// jitter must merge identically, because the canonical merge orders by
/// stream content rather than arrival. This is the property the recorded
/// arbitration alternative could never provide for live runs.
#[tokio::test]
async fn two_live_runs_with_different_arrival_timing_produce_identical_merged_order() {
    let _guard = FAN_IN_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let first_calls = Arc::new(AtomicUsize::new(0));
    run_live(
        &journal_base,
        build_two_channel_flow_with_jitter(journal_base.clone(), first_calls.clone(), 0),
    )
    .await;
    let first_run = replay_testkit::latest_run_dir(&journal_base);

    let second_calls = Arc::new(AtomicUsize::new(0));
    run_live(
        &journal_base,
        build_two_channel_flow_with_jitter(journal_base.clone(), second_calls.clone(), 25),
    )
    .await;
    let second_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(first_run, second_run);

    // Both runs executed their effects live; the merged order is identical.
    assert_eq!(first_calls.load(Ordering::SeqCst), 7);
    assert_eq!(second_calls.load(Ordering::SeqCst), 7);
    replay_testkit::assert_same_delivered_order(
        &first_run,
        &second_run,
        "merge",
        &["source_a", "source_b"],
    )
    .await;
}

#[tokio::test]
async fn skip_level_fan_in_delivers_causal_ancestors_first() {
    let _guard = FAN_IN_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let calls = Arc::new(AtomicUsize::new(0));
    run_live(
        &journal_base,
        build_skip_level_flow(journal_base.clone(), calls.clone()),
    )
    .await;
    let run_dir = replay_testkit::latest_run_dir(&journal_base);

    let projection =
        replay_testkit::project_delivered_order(&run_dir, "merge", &["source_a", "tap"]).await;

    // Map each tap output back to the source_a event it derives from.
    let tap_envelopes = replay_testkit::read_stage_envelopes(&run_dir, "tap").await;
    let tap_parent_of: std::collections::HashMap<_, _> = tap_envelopes
        .iter()
        .filter(|envelope| envelope.event.is_data())
        .filter_map(|envelope| {
            envelope
                .event
                .causality
                .parent_ids
                .first()
                .map(|parent| (envelope.event.id, *parent))
        })
        .collect();

    let position_of_parent: std::collections::HashMap<_, _> = projection
        .rows
        .iter()
        .enumerate()
        .map(|(index, row)| (row.parent_event_id, index))
        .collect();

    let mut checked = 0;
    for row in &projection.rows {
        if row.upstream_stage_key != "tap" {
            continue;
        }
        let source_ancestor = tap_parent_of
            .get(&row.parent_event_id)
            .expect("tap output should have a source_a parent");
        let ancestor_position = position_of_parent
            .get(source_ancestor)
            .expect("the source_a ancestor must also be consumed by the merge");
        let tap_position = position_of_parent[&row.parent_event_id];
        assert!(
            ancestor_position < &tap_position,
            "causal ancestor (source_a row at {ancestor_position}) must deliver before the \
             tap sibling derived from it (row at {tap_position})"
        );
        checked += 1;
    }
    assert_eq!(checked, 4, "every tap row must be checked");

    // The property must also hold under replay.
    let replay_calls = Arc::new(AtomicUsize::new(0));
    run_replay(
        &run_dir,
        build_skip_level_flow(journal_base.clone(), replay_calls.clone()),
    )
    .await;
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    let replay_run = replay_testkit::latest_run_dir(&journal_base);
    replay_testkit::assert_same_delivered_order(
        &run_dir,
        &replay_run,
        "merge",
        &["source_a", "tap"],
    )
    .await;
}
