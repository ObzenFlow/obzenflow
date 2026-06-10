// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095d Phase 4 (final tick): join phase transitions reproduce under
//! replay, proven at the flow level.
//!
//! Two shapes:
//! - A hydrating join (FiniteEof reference mode) is a structural deterministic
//!   orderer: the reference side is consumed to its authored EOF before the
//!   stream side enriches. The forwarded reference EOF's position in the
//!   join's own journal IS the Hydrating -> Enriching boundary, and it must
//!   reproduce under replay with no merge machinery.
//! - A live join (Live reference mode) with an effectful descendant is marked
//!   by the build-time enablement walk and runs the canonical cross-side
//!   merge (`dispatch_live_canonical`), so its delivered order, EOF-relative
//!   positions, and the effect cursors below all reproduce under replay.

mod replay_testkit;

use async_trait::async_trait;
use obzenflow::typed::joins;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl::{effectful_transform, flow, join, sink, source, FlowDefinition};
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

static JOIN_REPLAY_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefItem {
    key: String,
    label: String,
}

impl TypedPayload for RefItem {
    const EVENT_TYPE: &'static str = "join_replay.ref";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamItem {
    key: String,
    value: u64,
}

impl TypedPayload for StreamItem {
    const EVENT_TYPE: &'static str = "join_replay.stream";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JoinedItem {
    key: String,
    label: String,
    value: u64,
}

impl TypedPayload for JoinedItem {
    const EVENT_TYPE: &'static str = "join_replay.joined";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct JoinEffectValue {
    effect_value: u64,
}

impl TypedPayload for JoinEffectValue {
    const EVENT_TYPE: &'static str = "join_replay.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FinalOutput {
    key: String,
    value: u64,
    effect_value: u64,
}

impl TypedPayload for FinalOutput {
    const EVENT_TYPE: &'static str = "join_replay.output";
}

/// Reference source: three catalog rows, keys r1..r3.
#[derive(Clone, Debug)]
struct RefSource {
    next_index: usize,
    writer_id: WriterId,
}

impl RefSource {
    fn new() -> Self {
        Self {
            next_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for RefSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next_index >= 3 {
            return Ok(None);
        }
        self.next_index += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            RefItem::EVENT_TYPE,
            json!(RefItem {
                key: format!("r{}", self.next_index),
                label: format!("label-{}", self.next_index),
            }),
        )]))
    }
}

/// Stream source: five rows whose keys cycle the three catalog keys in an
/// order that keeps every key's catalog row ahead of its first stream use
/// under the canonical alternation (reference first on the ordinal tie), so
/// the live inner join emits all five.
#[derive(Clone, Debug)]
struct StreamSource {
    next_index: usize,
    writer_id: WriterId,
}

impl StreamSource {
    fn new() -> Self {
        Self {
            next_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for StreamSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next_index >= 5 {
            return Ok(None);
        }
        self.next_index += 1;
        let key = format!("r{}", ((self.next_index - 1) % 3) + 1);
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            StreamItem::EVENT_TYPE,
            json!(StreamItem {
                key,
                value: self.next_index as u64,
            }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct CountingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for CountingEffect {
    const EFFECT_TYPE: &'static str = "join_replay.counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Output = JoinEffectValue;

    fn label(&self) -> &str {
        "counting"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(JoinEffectValue {
            effect_value: self.value + 100,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("join-replay:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct EffectfulTail {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for EffectfulTail {
    type Input = JoinedItem;

    async fn process(&self, input: JoinedItem, fx: &mut Effects) -> Result<(), HandlerError> {
        let effect_value = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(FinalOutput {
            key: input.key,
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "join-replay-effectful-v1"
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

fn join_fn(reference: RefItem, stream: StreamItem) -> JoinedItem {
    JoinedItem {
        key: stream.key,
        label: reference.label,
        value: stream.value,
    }
}

/// Hydrating join (FiniteEof): the structural orderer, no merge machinery.
fn build_hydrating_flow(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "join_phase_transition_hydrating",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            ref_src = source!(RefItem => RefSource::new());
            stream_src = source!(StreamItem => StreamSource::new());
            joined = join!(catalog ref_src: RefItem, StreamItem -> JoinedItem => joins::inner(
                |r: &RefItem| r.key.clone(),
                |s: &StreamItem| s.key.clone(),
                join_fn
            ));
            collector = sink!(JoinedItem => DropSink);
        },

        topology: {
            stream_src |> joined;
            joined |> collector;
        }
    }
}

/// Live join with an effectful descendant: the enablement walk marks the join
/// and it runs the canonical cross-side merge.
fn build_live_flow(journal_base: PathBuf, calls: Arc<AtomicUsize>) -> FlowDefinition {
    flow! {
        name: "join_phase_transition_live",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            ref_src = source!(RefItem => RefSource::new());
            stream_src = source!(StreamItem => StreamSource::new());
            joined = join!(catalog ref_src: RefItem, StreamItem -> JoinedItem => joins::inner_live(
                |r: &RefItem| r.key.clone(),
                |s: &StreamItem| s.key.clone(),
                join_fn
            ));
            effectful = effectful_transform!(
                JoinedItem -> { FinalOutput, JoinEffectValue } => EffectfulTail { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collector = sink!(FinalOutput => DropSink);
        },

        topology: {
            stream_src |> joined;
            joined |> effectful;
            effectful |> collector;
        }
    }
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

/// FLOWIP-095d success criterion 2, hydrating shape: the forwarded reference
/// EOF (the Hydrating -> Enriching boundary) sits before every joined output
/// in the join's journal, and its position reproduces under replay and
/// replay-of-replay.
#[tokio::test]
async fn hydrating_join_phase_transition_reproduces_under_replay() {
    let _guard = JOIN_REPLAY_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    run_live(build_hydrating_flow(journal_base.clone())).await;
    let live_run = replay_testkit::latest_run_dir(&journal_base);

    let live_signature =
        replay_testkit::transport_row_signature(&live_run, "joined", &["ref_src", "stream_src"])
            .await;

    // The phase boundary: the reference EOF must precede every joined output.
    let ref_eof_position = live_signature
        .iter()
        .position(|row| row == "eof:ref_src")
        .expect("the join must forward the reference side's authored EOF");
    let first_data_position = live_signature
        .iter()
        .position(|row| row.starts_with("data:"))
        .expect("the join must emit joined outputs");
    assert!(
        ref_eof_position < first_data_position,
        "hydration completes before enrichment: reference EOF at {ref_eof_position} must \
         precede the first joined output at {first_data_position} in {live_signature:?}"
    );

    // Output order is witnessed payload-aware (the signature alone cannot
    // distinguish a permutation of same-typed outputs).
    let live_projection =
        replay_testkit::project_delivered_order(&live_run, "joined", &["stream_src"]).await;
    assert_eq!(
        live_projection.consumption_sequence().len(),
        5,
        "every stream row must be witnessed by a joined output"
    );

    // Replay: the phase-transition point and output order reproduce.
    run_replay(&live_run, build_hydrating_flow(journal_base.clone())).await;
    let replay_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(live_run, replay_run);

    let replay_signature =
        replay_testkit::transport_row_signature(&replay_run, "joined", &["ref_src", "stream_src"])
            .await;
    assert_eq!(
        live_signature, replay_signature,
        "the join's transport-row signature (data and EOF positions) must reproduce"
    );
    replay_testkit::assert_same_delivered_order(&live_run, &replay_run, "joined", &["stream_src"])
        .await;

    // Replay of the replay: the archive is deterministic by construction.
    run_replay(&replay_run, build_hydrating_flow(journal_base.clone())).await;
    let second_replay_run = replay_testkit::latest_run_dir(&journal_base);
    let second_signature = replay_testkit::transport_row_signature(
        &second_replay_run,
        "joined",
        &["ref_src", "stream_src"],
    )
    .await;
    assert_eq!(replay_signature, second_signature);
}

/// FLOWIP-095d success criterion 2, live shape: an ordered live join (marked
/// by the enablement walk through its effectful descendant) reproduces its
/// merged delivery order and EOF-relative positions under replay, with the
/// recorded effect outcomes returned and zero re-execution.
#[tokio::test]
async fn live_join_canonical_merge_reproduces_under_replay() {
    let _guard = JOIN_REPLAY_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    run_live(build_live_flow(journal_base.clone(), live_calls.clone())).await;
    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        5,
        "the canonical alternation keeps each catalog row ahead of its stream uses, \
         so the live inner join emits all five"
    );
    let live_run = replay_testkit::latest_run_dir(&journal_base);

    let live_signature =
        replay_testkit::transport_row_signature(&live_run, "joined", &["ref_src", "stream_src"])
            .await;
    let live_projection =
        replay_testkit::project_delivered_order(&live_run, "joined", &["stream_src"]).await;
    assert_eq!(live_projection.consumption_sequence().len(), 5);

    let replay_calls = Arc::new(AtomicUsize::new(0));
    run_replay(
        &live_run,
        build_live_flow(journal_base.clone(), replay_calls.clone()),
    )
    .await;
    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "replay must return recorded outcomes without executing the effect"
    );
    let replay_run = replay_testkit::latest_run_dir(&journal_base);

    let replay_signature =
        replay_testkit::transport_row_signature(&replay_run, "joined", &["ref_src", "stream_src"])
            .await;
    assert_eq!(
        live_signature, replay_signature,
        "the live join's merged delivery order and EOF positions must reproduce"
    );
    replay_testkit::assert_same_delivered_order(&live_run, &replay_run, "joined", &["stream_src"])
        .await;
}
