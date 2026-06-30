// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095l Tier 1 (barrier case): a commutative `#[trace_invariant]` fold under
//! a multi-source fan-in keeps the fan-in availability-scheduled (the barrier absorbs
//! input order) and still reconstructs identical emitted facts across arrival
//! interleavings and under `--replay-from`.
//!
//! This is the complement of `deterministic_fan_in_replay_test`: there an
//! order-sensitive (or effectful) descendant forces the canonical merge, so delivered
//! order reproduces exactly. Here the descendant is a proven barrier, so the fan-in is
//! NOT merged and delivery order may legitimately differ run-to-run; the commutative
//! fold makes the emitted total identical regardless. The build-time guarantee that
//! this fan-in stays availability-scheduled is pinned separately by the DSL unit tests
//! `commutative_barrier_as_fan_in_consumer_keeps_availability_scheduling` and
//! `barrier_shields_its_fan_in_from_a_downstream_observer`; this test owns the runtime
//! reconstruction half. Because a commutative fold yields identical facts whether or
//! not the merge runs, neither half is sufficient alone.

mod replay_testkit;

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, stateful, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::trace_invariant::word_from;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, StatefulHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::typing::StatefulTyping;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

static BARRIER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Contribution {
    value: u64,
}

impl TypedPayload for Contribution {
    const EVENT_TYPE: &'static str = "barrier.contribution";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RunningTotal {
    sum: u64,
}

impl TypedPayload for RunningTotal {
    const EVENT_TYPE: &'static str = "barrier.running_total";
}

/// A finite source of `Contribution`s. `base` keeps the two channels' value sets
/// disjoint so a non-commutative fold would diverge on reordering while the sum
/// cannot. Deterministic per-(channel, index) jitter varies arrival timing without
/// varying stream content.
#[derive(Clone, Debug)]
struct ContribSource {
    channel: &'static str,
    base: u64,
    next_index: usize,
    count: usize,
    jitter_ms: u64,
    writer_id: WriterId,
}

impl ContribSource {
    fn with_jitter(channel: &'static str, base: u64, count: usize, jitter_ms: u64) -> Self {
        Self {
            channel,
            base,
            next_index: 0,
            count,
            jitter_ms,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for ContribSource {
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
            Contribution::EVENT_TYPE,
            json!(Contribution {
                value: self.base + self.next_index as u64,
            }),
        )]))
    }
}

/// A commutative fold: a running sum emitted once at drain. Declared a barrier via
/// `#[trace_invariant]`, so the fan-in above it is availability-scheduled. The
/// attribute also emits its own `output_is_order_invariant` trial test into this
/// binary (cfg(test) is active here).
#[derive(Clone, Debug)]
struct SumBarrier;

impl StatefulTyping for SumBarrier {
    type Input = Contribution;
    type Output = RunningTotal;
}

#[obzenflow_runtime::trace_invariant(
    new = SumBarrier,
    inputs = vec![word_from([
        Contribution { value: 1 },
        Contribution { value: 2 },
        Contribution { value: 3 }
    ])]
)]
#[async_trait]
impl StatefulHandler for SumBarrier {
    type State = u64;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        if let Some(contribution) = Contribution::from_event(&event) {
            *state += contribution.value;
        }
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        let writer = WriterId::from(StageId::new());
        Ok(vec![RunningTotal { sum: *state }.to_event(writer)])
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

/// Two channels with disjoint value sets fan directly into the commutative barrier.
/// `source_a` contributes 1..=4 (sum 10); `source_b` contributes 101..=103 (sum 306);
/// the barrier emits a single `RunningTotal { sum: 316 }` at drain, regardless of the
/// interleaving the availability-scheduled fan-in produced.
fn build_barrier_flow(journal_base: PathBuf, jitter_ms: u64) -> FlowDefinition {
    flow! {
        name: "commutative_barrier_fan_in",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            source_a = source!(Contribution => ContribSource::with_jitter("a", 0, 4, jitter_ms));
            source_b = source!(Contribution => ContribSource::with_jitter("b", 100, 3, jitter_ms));
            sum_barrier = stateful!(Contribution -> RunningTotal => SumBarrier);
            collector = sink!(RunningTotal => DropSink);
        },

        topology: {
            source_a |> sum_barrier;
            source_b |> sum_barrier;
            sum_barrier |> collector;
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

/// The barrier's emitted durable facts, projected run-id-free as `(event_type,
/// payload)`. State is `fold(facts)`, so the emitted `RunningTotal` is the observable
/// reconstructed state.
async fn emitted_facts(run_dir: &Path, stage_key: &str) -> Vec<(String, serde_json::Value)> {
    replay_testkit::read_stage_envelopes(run_dir, stage_key)
        .await
        .into_iter()
        .filter(|envelope| matches!(envelope.event.content, ChainEventContent::Data { .. }))
        .map(|envelope| (envelope.event.event_type(), envelope.event.payload()))
        .collect()
}

#[tokio::test]
async fn commutative_barrier_reconstructs_identical_facts_across_interleavings_and_replay() {
    let _guard = BARRIER_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // Two live runs with different arrival timing. The fan-in is availability-scheduled
    // (the barrier absorbs order), so the two runs may deliver in different orders; the
    // commutative sum makes the emitted facts identical regardless. We do not assert a
    // specific delivered order here, because that is exactly what a barrier leaves free.
    run_live(build_barrier_flow(journal_base.clone(), 0)).await;
    let live0 = replay_testkit::latest_run_dir(&journal_base);

    run_live(build_barrier_flow(journal_base.clone(), 25)).await;
    let live25 = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(live0, live25);

    let facts0 = emitted_facts(&live0, "sum_barrier").await;
    let facts25 = emitted_facts(&live25, "sum_barrier").await;
    assert_eq!(
        facts0.len(),
        1,
        "the barrier emits exactly one RunningTotal at drain"
    );
    assert_eq!(
        facts0[0].1["sum"].as_u64(),
        Some(316),
        "the sum is order-invariant (1..=4 + 101..=103)"
    );
    assert_eq!(
        facts0, facts25,
        "two arrival interleavings of a commutative barrier reconstruct identical facts"
    );

    // Replay each archive: the barrier facts must reconstruct identically under
    // --replay-from despite the fan-in being availability-scheduled.
    run_replay(&live0, build_barrier_flow(journal_base.clone(), 0)).await;
    let replay0 = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(live0, replay0);
    assert_eq!(
        emitted_facts(&replay0, "sum_barrier").await,
        facts0,
        "--replay-from reconstructs the identical barrier facts"
    );

    run_replay(&live25, build_barrier_flow(journal_base.clone(), 0)).await;
    let replay25 = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(
        emitted_facts(&replay25, "sum_barrier").await,
        facts25,
        "--replay-from of the second interleaving reconstructs its identical barrier facts"
    );
}

/// FLOWIP-095l Tier 3 (contract, gated on FLOWIP-120n): `--resume-from` of an
/// interleaved SIGINT archive must reconstruct the identical `S_N` for the commutative
/// barrier and continue live. Unignore when 120n lands `--resume-from`.
#[tokio::test]
#[ignore = "FLOWIP-120n: resume reconstruction needs --resume-from (Tier 3)"]
async fn commutative_barrier_reconstructs_identical_s_n_under_resume() {
    panic!("contract pending FLOWIP-120n (durable resume)");
}
