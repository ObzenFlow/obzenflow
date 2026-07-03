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
use obzenflow_dsl::{effectful_transform, flow, sink, source, stateful, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey, SinkDeliverySafety,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, FiniteSourceHandler, SinkHandler, StatefulHandler, TransformHandler,
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
    delay_ms: u64,
    writer_id: WriterId,
}

impl ChannelSource {
    /// Deterministic pseudo-jitter per (channel, index): varies arrival
    /// timing without varying stream content, so two live runs with
    /// different jitter settings must still merge identically.
    fn with_jitter(channel: &'static str, count: usize, jitter_ms: u64) -> Self {
        Self {
            channel,
            next_index: 0,
            count,
            jitter_ms,
            delay_ms: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }

    /// Fixed per-event delay (unlike `jitter_ms`, which is a maximum). The
    /// telemetry witness uses this to guarantee the live run crosses the
    /// contract tick so intermediate stages emit ConsumptionProgress
    /// mid-stream.
    fn with_delay(channel: &'static str, count: usize, delay_ms: u64) -> Self {
        Self {
            channel,
            next_index: 0,
            count,
            jitter_ms: 0,
            delay_ms,
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
        if self.delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.delay_ms));
        }
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

    type Outcome = FanInEffectValue;

    fn label(&self) -> &str {
        "counting"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
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
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
                None,
            ),
        )
    }

    // Deterministic local drop: re-delivery under either archive verb is safe.
    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderedList {
    items: Vec<String>,
}

impl TypedPayload for OrderedList {
    const EVENT_TYPE: &'static str = "fan_in.ordered_list";
}

/// Non-effectful ORDER-DEPENDENT fold: it accumulates the arrival-ordered
/// `channel:value` word and emits it at drain. Its durable output differs under
/// a different fan-in interleaving, so reproducing it across live and replay
/// witnesses that the canonical merge pinned the order (FLOWIP-095m). A
/// commutative fold (e.g. a running sum) would emit the same output regardless
/// of order and so could not witness this.
#[derive(Clone, Debug)]
struct OrderedListStateful {
    writer_id: WriterId,
}

impl OrderedListStateful {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for OrderedListStateful {
    type State = Vec<String>;

    fn accumulate(&mut self, state: &mut Vec<String>, event: ChainEvent) {
        if let Some(record) = MergedRecord::from_event(&event) {
            state.push(format!("{}:{}", record.channel, record.value));
        }
    }

    fn initial_state(&self) -> Vec<String> {
        Vec::new()
    }

    fn create_events(&self, state: &Vec<String>) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            OrderedList::EVENT_TYPE,
            json!(OrderedList {
                items: state.clone()
            }),
        )])
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
    build_skip_level_flow_with_delay(journal_base, calls, 0)
}

fn build_skip_level_flow_with_delay(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    delay_ms: u64,
) -> FlowDefinition {
    flow! {
        name: "deterministic_fan_in_skip_level",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            source_a = source!(FanInInput => ChannelSource::with_delay("a", 4, delay_ms));
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

/// FLOWIP-095m: two channels fan into `merge`, whose only descendant is a
/// NON-effectful stateful counter. 095d would leave this fan-in
/// availability-scheduled; 095m marks it because the stateful stage observes
/// order, so `merge` runs the canonical deterministic merge.
fn build_two_channel_stateful_flow(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "stateful_fan_in",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            source_a = source!(FanInInput => ChannelSource::with_jitter("a", 4, 0));
            source_b = source!(FanInInput => ChannelSource::with_jitter("b", 3, 0));
            merge = transform!(FanInInput -> MergedRecord => MergeTransform::new());
            counter = stateful!(MergedRecord -> OrderedList => OrderedListStateful::new());
            collector = sink!(OrderedList => DropSink);
        },

        topology: {
            source_a |> merge;
            source_b |> merge;
            merge |> counter;
            counter |> collector;
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

    // This fan-in is source-fed, so it orders by admission sequence
    // (FLOWIP-120n F18): the live order follows append order rather than the
    // Kahn alternation, delivers every input, and preserves each channel's
    // internal order. Exact reproduction under replay is asserted below.
    let live_projection =
        replay_testkit::project_delivered_order(&live_run, "merge", &["source_a", "source_b"])
            .await;
    let sequence = live_projection.consumption_sequence();
    assert_eq!(sequence.len(), 7, "every input delivers exactly once");
    for upstream in ["source_a", "source_b"] {
        let ordinals: Vec<u64> = sequence
            .iter()
            .filter(|(stage, _)| stage == upstream)
            .map(|(_, ordinal)| *ordinal)
            .collect();
        assert!(
            ordinals.windows(2).all(|pair| pair[0] < pair[1]),
            "{upstream} must deliver in its journal order: {ordinals:?}"
        );
    }

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

/// Read the `counter` fold's emitted `channel:value` word from a run's journal.
async fn counter_ordered_word(run_dir: &std::path::Path) -> Vec<String> {
    replay_testkit::read_stage_envelopes(run_dir, "counter")
        .await
        .iter()
        .filter_map(|envelope| OrderedList::from_event(&envelope.event))
        .flat_map(|list| list.items)
        .collect()
}

/// FLOWIP-095m: a non-effectful stateful stage below the fan-in triggers the
/// deterministic-orderer marking (095d fired only for effects); this fan-in
/// is source-fed, so it runs the seq-ordered merge (FLOWIP-120n F18) and its
/// delivered order reproduces under `--replay-from`. The fold is
/// order-DEPENDENT, so its own durable output is a second witness.
#[tokio::test]
async fn stateful_fan_in_delivery_order_reproduces_under_replay() {
    let _guard = FAN_IN_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    run_live(
        &journal_base,
        build_two_channel_stateful_flow(journal_base.clone()),
    )
    .await;
    let live_run = replay_testkit::latest_run_dir(&journal_base);

    run_replay(
        &live_run,
        build_two_channel_stateful_flow(journal_base.clone()),
    )
    .await;
    let replay_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(live_run, replay_run);
    replay_testkit::assert_same_delivered_order(
        &live_run,
        &replay_run,
        "merge",
        &["source_a", "source_b"],
    )
    .await;

    // FLOWIP-095m: the order-DEPENDENT fold's own durable output reproduces,
    // which is the reconstruction property 120n's S_N relies on. A commutative
    // fold could not witness this, since its output would not vary with order.
    // The word itself is admission order (arrival-dependent under F18), so the
    // assertion is reproduction, not a fixed alternation.
    let live_word = counter_ordered_word(&live_run).await;
    assert_eq!(live_word.len(), 7, "the fold saw every input exactly once");
    assert_eq!(
        counter_ordered_word(&replay_run).await,
        live_word,
        "the fold's durable output must reproduce under replay"
    );
}

/// Arrival-timing independence at a DERIVED-fed fan-in: two LIVE runs with
/// different per-event delays must merge identically, because the Kahn merge
/// orders by stream content rather than arrival. FLOWIP-120n F18 scopes this
/// property: a source-fed fan-in orders by admission sequence, where the live
/// order is arrival-dependent but exactly reproducible (asserted by the
/// replay tests above); derived-fed fan-ins keep the Kahn merge and its
/// timing independence, pinned here on the skip-level shape.
#[tokio::test]
async fn two_live_runs_with_different_arrival_timing_produce_identical_merged_order() {
    let _guard = FAN_IN_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let first_calls = Arc::new(AtomicUsize::new(0));
    run_live(
        &journal_base,
        build_skip_level_flow_with_delay(journal_base.clone(), first_calls.clone(), 0),
    )
    .await;
    let first_run = replay_testkit::latest_run_dir(&journal_base);

    let second_calls = Arc::new(AtomicUsize::new(0));
    run_live(
        &journal_base,
        build_skip_level_flow_with_delay(journal_base.clone(), second_calls.clone(), 25),
    )
    .await;
    let second_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(first_run, second_run);

    // Different timing, identical merged order.
    replay_testkit::assert_same_delivered_order(
        &first_run,
        &second_run,
        "merge",
        &["source_a", "tap"],
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

/// The telemetry-lane witness (FLOWIP-095d): a live run long enough to cross
/// the contract tick makes the intermediate `tap` stage emit
/// ConsumptionProgress rows into its data journal mid-stream, at
/// wall-clock-dependent positions. The read-side telemetry filter keeps those
/// rows out of the merge's per-reader ordinals, so replay still reproduces
/// the fan-in order exactly. Before the filter, the interleaved rows took
/// ordinals at timing-dependent positions and could flip the tiebreak between
/// live and replay.
#[tokio::test]
async fn fan_in_replay_unaffected_by_mid_stream_reader_telemetry() {
    let _guard = FAN_IN_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // 4 events x 250ms ≈ 1s of live runtime, comfortably past the ~500ms
    // contract tick, so tap's subscription emits progress mid-stream.
    let live_calls = Arc::new(AtomicUsize::new(0));
    run_live(
        &journal_base,
        build_skip_level_flow_with_delay(journal_base.clone(), live_calls.clone(), 250),
    )
    .await;
    let live_run = replay_testkit::latest_run_dir(&journal_base);

    // Witness validity: the run must actually have exercised the condition.
    let telemetry_rows = replay_testkit::count_reader_telemetry_rows(&live_run, "tap").await;
    assert!(
        telemetry_rows > 0,
        "the slow live run must emit reader telemetry mid-stream for this \
         witness to mean anything (found none; raise the delay?)"
    );

    let replay_calls = Arc::new(AtomicUsize::new(0));
    run_replay(
        &live_run,
        build_skip_level_flow_with_delay(journal_base.clone(), replay_calls.clone(), 250),
    )
    .await;
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    let replay_run = replay_testkit::latest_run_dir(&journal_base);

    replay_testkit::assert_same_delivered_order(
        &live_run,
        &replay_run,
        "merge",
        &["source_a", "tap"],
    )
    .await;
}
