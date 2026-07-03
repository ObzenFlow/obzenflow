// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n F18: admission-sequence ordering at a source-fed ordered fan-in.
//!
//! The piggy bank shape: two infinite sources feed a live join above a
//! stateful fold and a sink. The reference source posts one row and stays
//! quiet forever; the stream source keeps emitting. Under the Kahn wait the
//! join would freeze on the quiet reference side; in seq mode the reference
//! reader's silence is proof (live run, at the entered generation) and the
//! stream delivers. A `--replay-from` of the run keeps the wait (readers stay
//! below the entered generation) and recomputes the identical delivered order
//! from the re-admitted sequences alone. All assertions read the
//! event-sourced journals.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, infinite_source, join, sink, stateful, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InfiniteSourceHandler, JoinHandler, SinkHandler, StatefulHandler, StatefulHandlerExt,
};
use obzenflow_runtime::stages::join::JoinReferenceMode;
use obzenflow_runtime::stages::stateful::strategies::emissions::EmitAlways;
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AccountRow {
    account_id: String,
}

impl TypedPayload for AccountRow {
    const EVENT_TYPE: &'static str = "seq_fan_in.account";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TxRow {
    value: u64,
}

impl TypedPayload for TxRow {
    const EVENT_TYPE: &'static str = "seq_fan_in.tx";
}

/// One row per delivered stream input; `refs_seen` witnesses where the
/// reference row interleaved in the fan-in's delivered order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PostedRow {
    value: u64,
    refs_seen: u64,
}

impl TypedPayload for PostedRow {
    const EVENT_TYPE: &'static str = "seq_fan_in.posted";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct LedgerSnapshot {
    count: u64,
    sum: u64,
}

impl TypedPayload for LedgerSnapshot {
    const EVENT_TYPE: &'static str = "seq_fan_in.snapshot";
}

/// Emits `count` rows then idles forever (an infinite source that goes quiet).
#[derive(Clone, Debug)]
struct AccountSource {
    writer_id: WriterId,
    remaining: u64,
}

impl AccountSource {
    fn new(count: u64) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            remaining: count,
        }
    }
}

impl InfiniteSourceHandler for AccountSource {
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.remaining -= 1;
        Ok(vec![AccountRow {
            account_id: "acct-1".to_string(),
        }
        .to_event(self.writer_id)])
    }
}

#[derive(Clone, Debug)]
struct TxSource {
    writer_id: WriterId,
    next_value: u64,
    remaining: u64,
}

impl TxSource {
    fn new(first_value: u64, count: u64) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            next_value: first_value,
            remaining: count,
        }
    }
}

impl InfiniteSourceHandler for TxSource {
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.remaining -= 1;
        let value = self.next_value;
        self.next_value += 1;
        Ok(vec![TxRow { value }.to_event(self.writer_id)])
    }
}

/// Live join that never drops: counts reference rows and emits one
/// order-witnessing `PostedRow` per stream row.
#[derive(Clone, Debug)]
struct SeqWitnessJoin;

#[async_trait]
impl JoinHandler for SeqWitnessJoin {
    type State = u64;

    fn initial_state(&self) -> Self::State {
        0
    }

    fn reference_mode(&self) -> JoinReferenceMode {
        JoinReferenceMode::Live
    }

    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if AccountRow::from_event(&event).is_some() {
            *state += 1;
            return Ok(Vec::new());
        }
        if let Some(tx) = TxRow::from_event(&event) {
            return Ok(vec![PostedRow {
                value: tx.value,
                refs_seen: *state,
            }
            .to_event(writer_id)]);
        }
        Ok(Vec::new())
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

/// Running-total fold; with `EmitAlways` it snapshots per input, so the sink
/// count gates the run.
#[derive(Clone, Debug)]
struct LedgerFold {
    writer_id: WriterId,
}

impl LedgerFold {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct LedgerState {
    count: u64,
    sum: u64,
}

#[async_trait]
impl StatefulHandler for LedgerFold {
    type State = LedgerState;

    fn initial_state(&self) -> Self::State {
        LedgerState::default()
    }

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        if let Some(posted) = PostedRow::from_event(&event) {
            state.count += 1;
            state.sum += posted.value;
        }
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            LedgerSnapshot::versioned_event_type(),
            json!(LedgerSnapshot {
                count: state.count,
                sum: state.sum,
            }),
        )])
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if LedgerSnapshot::from_event(&event).is_some() {
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
        Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

fn build_flow(
    journal_base: PathBuf,
    account_count: u64,
    tx_first: u64,
    tx_count: u64,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    flow! {
        name: "seq_fan_in",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            accounts = infinite_source!(AccountRow => AccountSource::new(account_count));
            tx = infinite_source!(TxRow => TxSource::new(tx_first, tx_count));
            posted = join!(catalog accounts: AccountRow, TxRow -> PostedRow => SeqWitnessJoin);
            ledger = stateful!(PostedRow -> LedgerSnapshot => LedgerFold::new().with_emission(EmitAlways));
            collect = sink!(LedgerSnapshot => CountingSink { delivered });
        },

        topology: {
            tx |> posted;
            posted |> ledger;
            ledger |> collect;
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
            "timeout waiting for the sink to consume {target} events (saw {}); \
             a Kahn wait on the quiet reference would produce exactly this hang \
             (FLOWIP-120n F18)",
            counter.load(Ordering::SeqCst)
        )
    })
}

/// The join journal reduced to its posted outputs, in physical append order:
/// the fan-in's delivered-order witness.
async fn posted_rows(run_dir: &Path) -> Vec<PostedRow> {
    replay_testkit::read_stage_envelopes_appended(run_dir, "posted")
        .await
        .iter()
        .filter_map(|envelope| PostedRow::from_event(&envelope.event))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn seq_fan_in_delivers_stream_while_reference_is_quiet_and_replays_exactly() -> Result<()> {
    const TX_EVENTS: u64 = 4;

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    // Live run: the reference posts one row then stays quiet forever; the
    // stream keeps emitting. The sink consuming every stream-derived output
    // is the liveness property the Kahn wait denies.
    {
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(journal_base.clone(), 1, 1, TX_EVENTS, delivered.clone())
            .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
            .await
            .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        wait_for_count(&delivered, TX_EVENTS).await?;
        handle.stop().await?;
        tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    }
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    // No further reference traffic: the reference journal holds exactly the
    // one posted row.
    let account_rows = replay_testkit::read_stage_envelopes_appended(&recorded_run, "accounts")
        .await
        .iter()
        .filter(|envelope| matches!(envelope.event.content, ChainEventContent::Data { .. }))
        .count();
    assert_eq!(
        account_rows, 1,
        "the quiet reference posted exactly one row"
    );

    let recorded_posted = posted_rows(&recorded_run).await;
    assert_eq!(
        recorded_posted.len() as u64,
        TX_EVENTS,
        "every stream row joined without further reference traffic"
    );
    assert_eq!(
        recorded_posted
            .iter()
            .map(|row| row.value)
            .collect::<Vec<_>>(),
        (1..=TX_EVENTS).collect::<Vec<_>>(),
        "stream rows deliver in stream order"
    );

    // Replay the run: readers stay below the entered generation, so the merge
    // keeps the wait and recomputes the identical order from the re-admitted
    // sequences alone.
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Replay,
            })
            .await,
        );
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(journal_base.clone(), 1, 1, TX_EVENTS, delivered)
            .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
            .await
            .map_err(|e| anyhow!("replay flow failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for the bounded replay to drain"))??;
    }
    let replay_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, replay_run);

    let replayed_posted = posted_rows(&replay_run).await;
    assert_eq!(
        replayed_posted, recorded_posted,
        "the fan-in's delivered order reproduces exactly, including where the \
         reference row interleaved (refs_seen)"
    );

    Ok(())
}
