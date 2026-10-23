// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use obzenflow_adapters::sources::ChannelSource;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainPayload, JournalRecord, WriterId};
use obzenflow_core::{EventId, TypedPayload};
use obzenflow_dsl::{
    async_infinite_source, flow, join, sink, source, stateful, transform, FlowDefinition,
};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    JoinReferenceView, StatefulEmission, TypedFiniteSourceHandler, TypedJoinHandler,
    TypedStatefulHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::join::JoinReferenceMode;
use obzenflow_runtime::stages::stateful::{Accumulator, OnEOF, StatefulWithEmission};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AccountRow {
    account_id: String,
}

impl TypedPayload for AccountRow {
    const EVENT_TYPE: &'static str = "recorded_fan_in.account";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TxRow {
    account_id: String,
    value: u64,
}

impl TypedPayload for TxRow {
    const EVENT_TYPE: &'static str = "recorded_fan_in.tx";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PostedRow {
    account_id: String,
    value: u64,
    account_known: bool,
}

impl TypedPayload for PostedRow {
    const EVENT_TYPE: &'static str = "recorded_fan_in.posted";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Snapshot {
    count: u64,
    sum: u64,
}

impl TypedPayload for Snapshot {
    const EVENT_TYPE: &'static str = "recorded_fan_in.snapshot";
}

fn account(id: &str) -> AccountRow {
    AccountRow {
        account_id: id.to_string(),
    }
}

fn tx(account_id: &str, value: u64) -> TxRow {
    TxRow {
        account_id: account_id.to_string(),
        value,
    }
}

#[derive(Clone, Debug)]
struct AccountBook;

impl TypedTransformHandler for AccountBook {
    type Input = AccountRow;
    type Output = AccountRow;

    fn process(&self, account: AccountRow) -> Result<AccountRow, HandlerError> {
        Ok(account)
    }
}

#[derive(Clone, Debug)]
struct PostEntries {
    mode: JoinReferenceMode,
}

impl TypedJoinHandler for PostEntries {
    type State = ();
    type ReferenceKey = String;
    type Reference = AccountRow;
    type Stream = TxRow;
    type Output = PostedRow;

    fn initial_state(&self) -> Self::State {}

    fn reference_mode(&self) -> JoinReferenceMode {
        self.mode
    }

    fn admit_reference(&self, account: &AccountRow) -> Result<String, HandlerError> {
        Ok(account.account_id.clone())
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, String, AccountRow>,
        tx: TxRow,
    ) -> Result<Vec<PostedRow>, HandlerError> {
        let account_known = references.select(&tx.account_id).is_some();
        Ok(vec![PostedRow {
            account_id: tx.account_id,
            value: tx.value,
            account_known,
        }])
    }
}

#[derive(Clone, Debug)]
struct Checkbook;

#[derive(Clone, Debug, Default)]
struct CheckbookState {
    count: u64,
    sum: u64,
}

impl TypedStatefulHandler for Checkbook {
    type State = CheckbookState;
    type Input = PostedRow;
    type Output = Snapshot;

    fn initial_state(&self) -> Self::State {
        CheckbookState::default()
    }

    fn accumulate(&self, state: &mut Self::State, posted: PostedRow) {
        state.count += 1;
        state.sum += posted.value;
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        state.count > 0
    }

    fn emit(&self, state: &Self::State) -> Result<StatefulEmission<Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            outputs: vec![Snapshot {
                count: state.count,
                sum: state.sum,
            }],
        })
    }

    fn post_emit(&self, _: &mut Self::State) {}
}

#[derive(Clone, Debug)]
struct BatchAccounts {
    rows: Vec<AccountRow>,
}

impl TypedFiniteSourceHandler for BatchAccounts {
    type Output = AccountRow;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(vec![self.rows.remove(0)]))
    }
}

struct LiveInputs<A, B> {
    first: mpsc::Sender<A>,
    second: mpsc::Sender<B>,
}

fn live_inputs<A, B>() -> (LiveInputs<A, B>, mpsc::Receiver<A>, mpsc::Receiver<B>) {
    let (first, first_rx) = mpsc::channel(64);
    let (second, second_rx) = mpsc::channel(64);
    (LiveInputs { first, second }, first_rx, second_rx)
}

fn live_join_flow(
    journal_base: PathBuf,
    derived_catalog: bool,
    accounts_rx: mpsc::Receiver<AccountRow>,
    tx_rx: mpsc::Receiver<TxRow>,
    delivered: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let accounts_source = ChannelSource::new(accounts_rx);
        let tx_source = ChannelSource::new(tx_rx);
        let post_live = PostEntries {
            mode: JoinReferenceMode::Live,
        };
        let book = AccountBook;
        let checkbook = Checkbook;
        let collect = replay_testkit::CountDeliveries::<Snapshot>::new(delivered);
        Ok(if derived_catalog {
            flow! {
                name: "recorded_fan_in",
                journals: disk_journals(journal_base),

                stages: {
                    accounts = async_infinite_source!(AccountRow => accounts_source);
                    tx = async_infinite_source!(TxRow => tx_source);
                    account_book = transform!(AccountRow -> AccountRow => book);
                    posted = join!(catalog account_book: AccountRow, TxRow -> PostedRow => post_live);
                    checkbook = stateful!(PostedRow -> Snapshot => checkbook);
                    collect = sink!(Snapshot => collect);
                },

                topology: {
                    accounts |> account_book;
                    (account_book, tx) |> posted;
                    posted |> checkbook;
                    checkbook |> collect;
                }
            }
        } else {
            flow! {
                name: "recorded_fan_in",
                journals: disk_journals(journal_base),

                stages: {
                    accounts = async_infinite_source!(AccountRow => accounts_source);
                    tx = async_infinite_source!(TxRow => tx_source);
                    posted = join!(catalog accounts: AccountRow, TxRow -> PostedRow => post_live);
                    checkbook = stateful!(PostedRow -> Snapshot => checkbook);
                    collect = sink!(Snapshot => collect);
                },

                topology: {
                    (accounts, tx) |> posted;
                    posted |> checkbook;
                    checkbook |> collect;
                }
            }
        })
    })
}

fn mixed_fold_flow(
    journal_base: PathBuf,
    accounts_rx: mpsc::Receiver<AccountRow>,
    tx_rx: mpsc::Receiver<TxRow>,
    adjustments_rx: mpsc::Receiver<PostedRow>,
    delivered: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let accounts_source = ChannelSource::new(accounts_rx);
        let tx_source = ChannelSource::new(tx_rx);
        let adjustments_source = ChannelSource::new(adjustments_rx);
        let post_live = PostEntries {
            mode: JoinReferenceMode::Live,
        };
        let book = AccountBook;
        let checkbook = Checkbook;
        let collect = replay_testkit::CountDeliveries::<Snapshot>::new(delivered);
        Ok(flow! {
            name: "recorded_fan_in_mixed",
            journals: disk_journals(journal_base),

            stages: {
                accounts = async_infinite_source!(AccountRow => accounts_source);
                tx = async_infinite_source!(TxRow => tx_source);
                adjustments = async_infinite_source!(PostedRow => adjustments_source);
                account_book = transform!(AccountRow -> AccountRow => book);
                posted = join!(catalog account_book: AccountRow, TxRow -> PostedRow => post_live);
                checkbook = stateful!(PostedRow -> Snapshot => checkbook);
                collect = sink!(Snapshot => collect);
            },

            topology: {
                accounts |> account_book;
                (account_book, tx) |> posted;
                posted |> checkbook;
                adjustments |> checkbook;
                checkbook |> collect;
            }
        })
    })
}

fn hydrating_flow(
    journal_base: PathBuf,
    quiet_rx: mpsc::Receiver<TxRow>,
    active_rx: mpsc::Receiver<TxRow>,
    delivered: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let accounts_source = BatchAccounts {
            rows: vec![account("acct-1"), account("acct-2")],
        };
        let quiet_source = ChannelSource::new(quiet_rx);
        let active_source = ChannelSource::new(active_rx);
        let post_batch = PostEntries {
            mode: JoinReferenceMode::FiniteEof,
        };
        let checkbook = Checkbook;
        let collect = replay_testkit::CountDeliveries::<Snapshot>::new(delivered);
        Ok(flow! {
            name: "recorded_fan_in_hydrating",
            journals: disk_journals(journal_base),

            stages: {
                accounts = source!(AccountRow => accounts_source);
                tx_quiet = async_infinite_source!(TxRow => quiet_source);
                tx_active = async_infinite_source!(TxRow => active_source);
                posted = join!(catalog accounts: AccountRow, TxRow -> PostedRow => post_batch);
                checkbook = stateful!(PostedRow -> Snapshot => checkbook);
                collect = sink!(Snapshot => collect);
            },

            topology: {
                (accounts, tx_quiet) |> posted;
                (accounts, tx_active) |> posted;
                posted |> checkbook;
                checkbook |> collect;
            }
        })
    })
}

#[derive(Clone, Debug)]
struct PausingBatch {
    rows: Vec<TxRow>,
    release: Arc<std::sync::atomic::AtomicBool>,
}

impl TypedFiniteSourceHandler for PausingBatch {
    type Output = TxRow;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if !self.rows.is_empty() {
            return Ok(Some(vec![self.rows.remove(0)]));
        }
        if self.release.load(Ordering::SeqCst) {
            Ok(None)
        } else {
            Ok(Some(Vec::new()))
        }
    }
}

#[derive(Clone, Debug)]
struct BatchTotal;

impl Accumulator for BatchTotal {
    type State = Snapshot;
    type Input = TxRow;
    type Output = Snapshot;

    fn initial_state(&self) -> Snapshot {
        Snapshot { count: 0, sum: 0 }
    }

    fn accumulate(&self, total: &mut Snapshot, tx: TxRow) {
        total.count += 1;
        total.sum += tx.value;
    }

    fn outputs(&self, total: &Snapshot) -> Vec<Snapshot> {
        vec![total.clone()]
    }
}

#[derive(Clone, Debug)]
struct Forward;

impl TypedTransformHandler for Forward {
    type Input = Snapshot;
    type Output = Snapshot;

    fn process(&self, snapshot: Snapshot) -> Result<Snapshot, HandlerError> {
        Ok(snapshot)
    }
}

#[derive(Clone, Debug)]
struct Tally;

impl TypedStatefulHandler for Tally {
    type State = CheckbookState;
    type Input = Snapshot;
    type Output = Snapshot;

    fn initial_state(&self) -> Self::State {
        CheckbookState::default()
    }

    fn accumulate(&self, state: &mut Self::State, snapshot: Snapshot) {
        state.count += 1;
        state.sum += snapshot.sum;
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        state.count > 0
    }

    fn emit(&self, state: &Self::State) -> Result<StatefulEmission<Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            outputs: vec![Snapshot {
                count: state.count,
                sum: state.sum,
            }],
        })
    }

    fn post_emit(&self, _: &mut Self::State) {}
}

fn end_of_input_flow(
    journal_base: PathBuf,
    release: Arc<std::sync::atomic::AtomicBool>,
    live_rx: mpsc::Receiver<Snapshot>,
    delivered: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let batch_source = PausingBatch {
            rows: vec![tx("acct-1", 5), tx("acct-1", 7)],
            release,
        };
        let live_source = ChannelSource::new(live_rx);
        let total = StatefulWithEmission::new(BatchTotal, OnEOF);
        let forward = Forward;
        let tally = Tally;
        let collect = replay_testkit::CountDeliveries::<Snapshot>::new(delivered);
        Ok(flow! {
            name: "recorded_fan_in_end_of_input",
            journals: disk_journals(journal_base),

            stages: {
                batch = source!(TxRow => batch_source);
                live = async_infinite_source!(Snapshot => live_source);
                total = stateful!(TxRow -> Snapshot => total);
                merged = transform!(Snapshot -> Snapshot => forward);
                tally = stateful!(Snapshot -> Snapshot => tally);
                collect = sink!(Snapshot => collect);
            },

            topology: {
                batch |> total;
                total |> merged;
                live |> merged;
                merged |> tally;
                tally |> collect;
            }
        })
    })
}

async fn start(flow: FlowDefinition) -> Result<FlowHandle> {
    let handle = flow
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(*rx.borrow(), PipelineState::Running) {
                return Ok::<(), anyhow::Error>(());
            }
            rx.changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for pipeline to reach Running"))??;
    Ok(handle)
}

async fn stop(handle: FlowHandle) -> Result<()> {
    handle.stop().await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    Ok(())
}

async fn wait_for_accounts(journal_base: &Path, stage: &str, target: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let run = replay_testkit::latest_run_dir(journal_base);
            if outputs::<AccountRow>(&run, stage).await.len() >= target {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for {target} accounts to reach '{stage}'"))
}

async fn wait_for_count(counter: &Arc<AtomicUsize>, target: usize) -> Result<Duration> {
    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(20), async {
        while counter.load(Ordering::SeqCst) < target {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "timeout waiting for the sink to consume {target} events (saw {}); a fan-in \
             waiting on its quiet input produces exactly this hang (FLOWIP-095q)",
            counter.load(Ordering::SeqCst)
        )
    })?;
    Ok(started.elapsed())
}

async fn replay(journal_base: &Path, archive: &Path, flow: FlowDefinition) -> Result<PathBuf> {
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: archive.to_path_buf(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Replay,
            })
            .await,
        );
        let handle = start(flow).await?;
        tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for the bounded replay to drain"))??;
    }
    let run = replay_testkit::latest_run_dir(journal_base);
    assert_ne!(run, archive);
    Ok(run)
}

fn resume_bootstrap(archive: &Path) -> ReplayBootstrap {
    ReplayBootstrap {
        archive_path: archive.to_path_buf(),
        allow_incomplete_archive: true,
        allow_duplicate_sink_delivery: false,
        verb: ReplayVerb::Resume,
    }
}

async fn outputs<T: TypedPayload>(run: &Path, stage: &str) -> Vec<T> {
    replay_testkit::read_stage_envelopes_appended(run, stage)
        .await
        .iter()
        .filter_map(|envelope| T::from_event(&envelope.authored()))
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
struct DecisionRow {
    input: String,
    delivered_count: u64,
    input_delivered_count: u64,
}

fn decision_row(record: &JournalRecord<ChainPayload>) -> Option<DecisionRow> {
    match &record.payload {
        ChainPayload::FlowControl(FlowControlPayload::InputSelection {
            input,
            delivered_count,
            input_delivered_count,
        }) => Some(DecisionRow {
            input: input.stage_key.to_string(),
            delivered_count: delivered_count.0,
            input_delivered_count: input_delivered_count.0,
        }),
        _ => None,
    }
}

async fn decision_rows(run: &Path, stage: &str) -> Vec<DecisionRow> {
    replay_testkit::read_stage_envelopes_appended(run, stage)
        .await
        .iter()
        .filter_map(decision_row)
        .collect()
}

fn delivery_order(run: &Path, stage: &str) -> String {
    replay_testkit::archive_manifest(run)["stages"][stage]["delivery_order"]
        .as_str()
        .expect("every stage records its delivery order")
        .to_string()
}

async fn assert_outputs_follow_their_decision_rows(run: &Path, stage: &str) {
    let manifest = replay_testkit::archive_manifest(run);
    let mut input_of: HashMap<EventId, String> = HashMap::new();
    for upstream in manifest["stages"][stage]["inbound"]
        .as_array()
        .expect("inbound stages")
    {
        let upstream = upstream.as_str().expect("an inbound stage key");
        for record in replay_testkit::read_stage_envelopes_appended(run, upstream).await {
            input_of.insert(*record.id(), upstream.to_string());
        }
    }
    let stage_id = manifest["stages"][stage]["stage_id"]
        .as_str()
        .expect("the stage's id")
        .to_string();
    let journal = replay_testkit::read_stage_envelopes_appended(run, stage).await;
    let sequence: Vec<String> = journal
        .iter()
        .map(|record| match decision_row(record) {
            Some(row) => format!("row({} @{})", row.input, row.delivered_count),
            None => format!(
                "{} parent={:?}",
                record.event_type_name(),
                record
                    .envelope
                    .provenance
                    .event
                    .causality
                    .parent_ids
                    .last()
                    .and_then(|parent| input_of.get(parent))
            ),
        })
        .collect();
    let mut governing: Option<String> = None;
    let mut outputs = 0;
    for record in journal {
        if let Some(row) = decision_row(&record) {
            governing = Some(row.input);
            continue;
        }
        let authored_here =
            matches!(record.writer_id(), WriterId::Stage(id) if id.to_string() == stage_id);
        if !authored_here || !record.consumes_data_credit() {
            continue;
        }
        let parent = record
            .envelope
            .provenance
            .event
            .causality
            .parent_ids
            .last()
            .copied()
            .expect("an output names its input");
        let Some(input) = input_of.get(&parent) else {
            continue;
        };
        outputs += 1;
        assert_eq!(
            governing.as_ref(),
            Some(input),
            "{stage}: output {} lies under a decision row naming '{governing:?}', not its \
             input '{input}'; journal: {sequence:#?}",
            record.id()
        );
    }
    assert!(outputs > 0, "{stage} authored outputs to check");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_fed_live_join_records_its_order_and_its_replay_follows_it() -> Result<()> {
    const TX_EVENTS: usize = 4;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let delivered = Arc::new(AtomicUsize::new(0));
    let (inputs, accounts_rx, tx_rx) = live_inputs();
    let handle = start(live_join_flow(
        journal_base.clone(),
        false,
        accounts_rx,
        tx_rx,
        delivered.clone(),
    ))
    .await?;
    inputs.first.send(account("acct-1")).await?;
    for value in 1..=TX_EVENTS as u64 {
        inputs.second.send(tx("acct-1", value)).await?;
    }
    wait_for_count(&delivered, TX_EVENTS).await?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    assert_eq!(delivery_order(&run, "posted"), "recorded");
    let posted = outputs::<PostedRow>(&run, "posted").await;
    assert_eq!(
        posted.iter().map(|row| row.value).collect::<Vec<_>>(),
        (1..=TX_EVENTS as u64).collect::<Vec<_>>(),
        "every transaction posts while the account source stays quiet"
    );
    let rows = decision_rows(&run, "posted").await;
    assert!(!rows.is_empty(), "the live join records its selections");
    assert_outputs_follow_their_decision_rows(&run, "posted").await;

    let (_inputs, accounts_rx, tx_rx) = live_inputs();
    let replayed = replay(
        &journal_base,
        &run,
        live_join_flow(
            journal_base.clone(),
            false,
            accounts_rx,
            tx_rx,
            Arc::new(AtomicUsize::new(0)),
        ),
    )
    .await?;
    assert_eq!(outputs::<PostedRow>(&replayed, "posted").await, posted);
    assert_eq!(
        decision_rows(&replayed, "posted").await,
        rows,
        "the replay re-authors every row it follows"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_join_behind_a_derived_catalog_delivers_while_the_catalog_is_quiet() -> Result<()> {
    const TX_EVENTS: usize = 4;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let delivered = Arc::new(AtomicUsize::new(0));
    let (inputs, accounts_rx, tx_rx) = live_inputs();
    let handle = start(live_join_flow(
        journal_base.clone(),
        true,
        accounts_rx,
        tx_rx,
        delivered.clone(),
    ))
    .await?;
    inputs.first.send(account("acct-1")).await?;
    for value in 1..=TX_EVENTS as u64 {
        inputs.second.send(tx("acct-1", value)).await?;
    }
    let elapsed = wait_for_count(&delivered, TX_EVENTS).await?;
    eprintln!("derived catalog: {TX_EVENTS} transactions posted in {elapsed:?}");
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    assert_eq!(delivery_order(&run, "posted"), "recorded");
    for stage in ["account_book", "checkbook"] {
        assert_eq!(delivery_order(&run, stage), "single_input", "{stage}");
    }
    let posted = outputs::<PostedRow>(&run, "posted").await;
    assert_eq!(posted.len(), TX_EVENTS);
    assert!(
        decision_rows(&run, "posted")
            .await
            .iter()
            .any(|row| row.input == "tx"),
        "the join's journal names each selection's input"
    );
    assert_outputs_follow_their_decision_rows(&run, "posted").await;

    let (_inputs, accounts_rx, tx_rx) = live_inputs();
    let replayed = replay(
        &journal_base,
        &run,
        live_join_flow(
            journal_base.clone(),
            true,
            accounts_rx,
            tx_rx,
            Arc::new(AtomicUsize::new(0)),
        ),
    )
    .await?;
    assert_eq!(
        outputs::<PostedRow>(&replayed, "posted").await,
        posted,
        "the replay reproduces where the account interleaved"
    );
    assert_eq!(
        outputs::<Snapshot>(&replayed, "checkbook").await,
        outputs::<Snapshot>(&run, "checkbook").await
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_account_through_a_derived_catalog_replays_its_interleaving() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let delivered = Arc::new(AtomicUsize::new(0));
    let (inputs, accounts_rx, tx_rx) = live_inputs();
    let handle = start(live_join_flow(
        journal_base.clone(),
        true,
        accounts_rx,
        tx_rx,
        delivered.clone(),
    ))
    .await?;
    inputs.first.send(account("acct-1")).await?;
    wait_for_accounts(&journal_base, "account_book", 1).await?;
    for value in 1..=2 {
        inputs.second.send(tx("acct-1", value)).await?;
    }
    wait_for_count(&delivered, 2).await?;
    inputs.first.send(account("acct-2")).await?;
    wait_for_accounts(&journal_base, "account_book", 2).await?;
    for value in 3..=4 {
        inputs.second.send(tx("acct-2", value)).await?;
    }
    wait_for_count(&delivered, 4).await?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    let posted = outputs::<PostedRow>(&run, "posted").await;
    assert_eq!(posted.len(), 4);
    assert!(
        posted.iter().all(|row| row.account_known),
        "an account that reached the catalog first is applied first (B4)"
    );
    assert!(
        decision_rows(&run, "posted")
            .await
            .iter()
            .filter(|row| row.input == "account_book")
            .count()
            >= 2,
        "each account opens its own selection between transactions"
    );
    assert_outputs_follow_their_decision_rows(&run, "posted").await;

    let (_inputs, accounts_rx, tx_rx) = live_inputs();
    let replayed = replay(
        &journal_base,
        &run,
        live_join_flow(
            journal_base.clone(),
            true,
            accounts_rx,
            tx_rx,
            Arc::new(AtomicUsize::new(0)),
        ),
    )
    .await?;
    assert_eq!(outputs::<PostedRow>(&replayed, "posted").await, posted);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_source_and_derived_fold_records_its_order() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let delivered = Arc::new(AtomicUsize::new(0));
    let (inputs, accounts_rx, tx_rx) = live_inputs();
    let (adjustments, adjustments_rx) = mpsc::channel(8);
    let handle = start(mixed_fold_flow(
        journal_base.clone(),
        accounts_rx,
        tx_rx,
        adjustments_rx,
        delivered.clone(),
    ))
    .await?;
    inputs.first.send(account("acct-1")).await?;
    for value in 1..=3 {
        inputs.second.send(tx("acct-1", value)).await?;
    }
    wait_for_count(&delivered, 3).await?;
    adjustments
        .send(PostedRow {
            account_id: "acct-1".to_string(),
            value: 100,
            account_known: true,
        })
        .await?;
    wait_for_count(&delivered, 4).await?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    assert_eq!(delivery_order(&run, "checkbook"), "recorded");
    let snapshots = outputs::<Snapshot>(&run, "checkbook").await;
    assert_eq!(snapshots.last().map(|snapshot| snapshot.sum), Some(106));
    let rows = decision_rows(&run, "checkbook").await;
    assert!(rows.iter().any(|row| row.input == "posted"));
    assert!(rows.iter().any(|row| row.input == "adjustments"));
    assert_outputs_follow_their_decision_rows(&run, "checkbook").await;

    let (_inputs, accounts_rx, tx_rx) = live_inputs();
    let (_adjustments, adjustments_rx) = mpsc::channel(8);
    let replayed = replay(
        &journal_base,
        &run,
        mixed_fold_flow(
            journal_base.clone(),
            accounts_rx,
            tx_rx,
            adjustments_rx,
            Arc::new(AtomicUsize::new(0)),
        ),
    )
    .await?;
    assert_eq!(outputs::<Snapshot>(&replayed, "checkbook").await, snapshots);
    assert_eq!(decision_rows(&replayed, "checkbook").await, rows);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hydrating_join_records_a_stream_side_with_a_quiet_live_input() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let delivered = Arc::new(AtomicUsize::new(0));
    let (inputs, quiet_rx, active_rx) = live_inputs::<TxRow, TxRow>();
    let handle = start(hydrating_flow(
        journal_base.clone(),
        quiet_rx,
        active_rx,
        delivered.clone(),
    ))
    .await?;
    for value in 1..=4 {
        let account_id = if value % 2 == 0 { "acct-2" } else { "acct-1" };
        inputs.second.send(tx(account_id, value)).await?;
    }
    wait_for_count(&delivered, 4).await?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    assert_eq!(delivery_order(&run, "posted"), "recorded");
    let posted = outputs::<PostedRow>(&run, "posted").await;
    assert_eq!(posted.len(), 4, "the active input keeps posting");
    assert!(
        posted.iter().all(|row| row.account_known),
        "the phase boundary still delivers every account first"
    );

    let journal = replay_testkit::read_stage_envelopes_appended(&run, "posted").await;
    let accounts_id = replay_testkit::archive_manifest(&run)["stages"]["accounts"]["stage_id"]
        .as_str()
        .expect("accounts stage id")
        .to_string();
    let authored_by_accounts =
        |writer: &WriterId| matches!(writer, WriterId::Stage(id) if id.to_string() == accounts_id);
    let reference_eof = journal
        .iter()
        .position(|record| match &record.payload {
            ChainPayload::FlowControl(FlowControlPayload::Eof { writer_id, .. }) => {
                writer_id.as_ref().map_or(
                    authored_by_accounts(record.writer_id()),
                    authored_by_accounts,
                )
            }
            _ => false,
        })
        .expect("the join forwards the reference EOF");
    let first_row = journal
        .iter()
        .position(|record| decision_row(record).is_some())
        .expect("the stream side records its selections");
    assert!(first_row > reference_eof);
    let rows = decision_rows(&run, "posted").await;
    assert!(rows.iter().all(|row| row.input != "accounts"));
    assert!(
        rows[0].delivered_count > 0,
        "the stage total counts the reference deliveries ahead of the stream side"
    );
    assert_outputs_follow_their_decision_rows(&run, "posted").await;

    let (_inputs, quiet_rx, active_rx) = live_inputs::<TxRow, TxRow>();
    let replayed = replay(
        &journal_base,
        &run,
        hydrating_flow(
            journal_base.clone(),
            quiet_rx,
            active_rx,
            Arc::new(AtomicUsize::new(0)),
        ),
    )
    .await?;
    assert_eq!(outputs::<PostedRow>(&replayed, "posted").await, posted);
    assert_eq!(decision_rows(&replayed, "posted").await, rows);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resume_continues_the_recorded_prefix_and_its_archive_replays() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let delivered = Arc::new(AtomicUsize::new(0));
    let (inputs, accounts_rx, tx_rx) = live_inputs();
    let handle = start(live_join_flow(
        journal_base.clone(),
        true,
        accounts_rx,
        tx_rx,
        delivered.clone(),
    ))
    .await?;
    inputs.first.send(account("acct-1")).await?;
    for value in 1..=2 {
        inputs.second.send(tx("acct-1", value)).await?;
    }
    wait_for_count(&delivered, 2).await?;
    stop(handle).await?;
    drop(inputs);
    let stopped = replay_testkit::latest_run_dir(&journal_base);
    let stopped_posted = outputs::<PostedRow>(&stopped, "posted").await;
    let stopped_rows = decision_rows(&stopped, "posted").await;

    let delivered = Arc::new(AtomicUsize::new(0));
    let (inputs, accounts_rx, tx_rx) = live_inputs();
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(resume_bootstrap(&stopped)).await,
        );
        let handle = start(live_join_flow(
            journal_base.clone(),
            true,
            accounts_rx,
            tx_rx,
            delivered.clone(),
        ))
        .await?;
        inputs.second.send(tx("acct-1", 3)).await?;
        wait_for_count(&delivered, 3).await?;
        stop(handle).await?;
    }
    let resumed = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(stopped, resumed);

    let resumed_posted = outputs::<PostedRow>(&resumed, "posted").await;
    assert_eq!(
        &resumed_posted[..stopped_posted.len()],
        &stopped_posted[..],
        "catch-up reproduces the recorded prefix"
    );
    assert_eq!(resumed_posted.len(), 3, "and continues live");
    let resumed_rows = decision_rows(&resumed, "posted").await;
    assert!(resumed_rows.len() > stopped_rows.len());
    assert_eq!(
        &resumed_rows[..stopped_rows.len()],
        &stopped_rows[..],
        "the resume re-authors the rows it follows, then records its own"
    );
    assert_outputs_follow_their_decision_rows(&resumed, "posted").await;

    let (_inputs, accounts_rx, tx_rx) = live_inputs();
    let replayed = replay(
        &journal_base,
        &resumed,
        live_join_flow(
            journal_base.clone(),
            true,
            accounts_rx,
            tx_rx,
            Arc::new(AtomicUsize::new(0)),
        ),
    )
    .await?;
    assert_eq!(
        outputs::<PostedRow>(&replayed, "posted").await,
        resumed_posted
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_folds_end_of_input_output_lands_where_it_was_recorded() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let live_snapshot = |sum| Snapshot { count: 1, sum };
    let batch_total = Snapshot { count: 2, sum: 12 };

    let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let delivered = Arc::new(AtomicUsize::new(0));
    let (live, live_rx) = mpsc::channel(8);
    let handle = start(end_of_input_flow(
        journal_base.clone(),
        release.clone(),
        live_rx,
        delivered.clone(),
    ))
    .await?;
    live.send(live_snapshot(100)).await?;
    wait_for_count(&delivered, 1).await?;
    release.store(true, Ordering::SeqCst);
    wait_for_count(&delivered, 2).await?;
    live.send(live_snapshot(1000)).await?;
    wait_for_count(&delivered, 3).await?;
    stop(handle).await?;
    drop(live);
    let run = replay_testkit::latest_run_dir(&journal_base);

    assert_eq!(delivery_order(&run, "merged"), "recorded");
    let merged = outputs::<Snapshot>(&run, "merged").await;
    assert_eq!(
        merged,
        [live_snapshot(100), batch_total.clone(), live_snapshot(1000)]
    );
    let rows = decision_rows(&run, "merged").await;
    assert_outputs_follow_their_decision_rows(&run, "merged").await;

    let (_live, live_rx) = mpsc::channel(8);
    let replayed = replay(
        &journal_base,
        &run,
        end_of_input_flow(
            journal_base.clone(),
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            live_rx,
            Arc::new(AtomicUsize::new(0)),
        ),
    )
    .await?;
    assert_eq!(outputs::<Snapshot>(&replayed, "merged").await, merged);
    assert_eq!(decision_rows(&replayed, "merged").await, rows);

    let delivered = Arc::new(AtomicUsize::new(0));
    let (live, live_rx) = mpsc::channel(8);
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(resume_bootstrap(&run)).await,
        );
        let handle = start(end_of_input_flow(
            journal_base.clone(),
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            live_rx,
            delivered.clone(),
        ))
        .await?;
        live.send(live_snapshot(10_000)).await?;
        wait_for_count(&delivered, 4).await?;
        stop(handle).await?;
    }
    let resumed = replay_testkit::latest_run_dir(&journal_base);
    let resumed_merged = outputs::<Snapshot>(&resumed, "merged").await;
    assert_eq!(&resumed_merged[..merged.len()], &merged[..]);
    assert_eq!(resumed_merged.last(), Some(&live_snapshot(10_000)));
    assert_eq!(
        &decision_rows(&resumed, "merged").await[..rows.len()],
        &rows[..]
    );
    Ok(())
}
