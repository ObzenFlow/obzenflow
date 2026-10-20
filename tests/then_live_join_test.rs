// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow::stages::sources::{ChannelSource, ValuesSource};
use obzenflow::stages::{joins, merges};
use obzenflow_core::config::{ConfigScope, ConfigSource};
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, JoinPhaseFact, StageLifecycleFact,
};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{ChainPayload, JournalRecord};
use obzenflow_core::{StageKey, TypedPayload};
use obzenflow_dsl::{async_infinite_source, flow, join, merge, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
};
use obzenflow_runtime::stages::common::handlers::{InlineSink, SinkDescription, SinkWriteFailure};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

static THEN_LIVE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Account {
    key: String,
}
impl TypedPayload for Account {
    const EVENT_TYPE: &'static str = "then_live_test.account";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Entry {
    key: String,
    note: String,
}
impl TypedPayload for Entry {
    const EVENT_TYPE: &'static str = "then_live_test.entry";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Posted {
    key: String,
    note: String,
}
impl TypedPayload for Posted {
    const EVENT_TYPE: &'static str = "then_live_test.posted";
}

fn account(key: &str) -> Account {
    Account { key: key.into() }
}

fn entry(key: &str, note: &str) -> Entry {
    Entry {
        key: key.into(),
        note: note.into(),
    }
}

#[derive(Clone, Debug)]
struct Collect {
    notes: Arc<Mutex<Vec<String>>>,
    arrived: Arc<Notify>,
}

#[async_trait]
impl InlineSink for Collect {
    type Input = Posted;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
        )
        .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, posted: Posted) -> Result<(), SinkWriteFailure> {
        self.notes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(posted.note);
        self.arrived.notify_one();
        Ok(())
    }
}

struct Inputs {
    batch_entries: Vec<Entry>,
    live_accounts: mpsc::Receiver<Account>,
    live_entries: mpsc::Receiver<Entry>,
    batch_rate: Option<f64>,
}

fn enforced_backpressure(window: u64, stall_timeout_ms: u64) -> FlowBuildContext {
    let mut candidates = CandidateSet::default();
    for (key, value) in [
        (
            "runtime.backpressure.mode",
            ConfigValue::Text("enforce".to_owned()),
        ),
        ("runtime.backpressure.window", ConfigValue::U64(window)),
        (
            "runtime.backpressure.stall_timeout_ms",
            ConfigValue::U64(stall_timeout_ms),
        ),
    ] {
        candidates
            .admit(ScopedCandidate::unqualified(
                key,
                ConfigScope::Flow,
                ConfigSource::File,
                value,
            ))
            .expect("the backpressure setting admits");
    }
    FlowBuildContext::new(Arc::new(ResolvedRuntimeConfig::new(candidates)))
}

fn build(journal_base: PathBuf, inputs: Inputs, sink: Collect) -> FlowDefinition {
    let inputs = Mutex::new(Some(inputs));
    FlowDefinition::materialize(move |_runtime_config| {
        let Inputs {
            batch_entries,
            live_accounts,
            live_entries,
            batch_rate,
        } = inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("the flow is built once");
        let batch_accounts_source = ValuesSource::new(vec![account("acct-1"), account("acct-2")]);
        let batch_tx_source = ValuesSource::new(batch_entries);
        let live_accounts_source = ChannelSource::new(live_accounts);
        let live_tx_source = ChannelSource::new(live_entries);
        let post_entry = joins::inner_then_live(
            |account: &Account| account.key.clone(),
            |entry: &Entry| entry.key.clone(),
            |_account: Account, entry: Entry| Posted {
                key: entry.key,
                note: entry.note,
            },
        );
        let collector = sink.clone();
        let rate = batch_rate.unwrap_or(10_000.0);
        let journals = journal_base.clone();

        Ok(flow! {
            name: "then_live_join_test",
            journals: disk_journals(journals),

            stages: {
                batch_accounts = source!(Account => batch_accounts_source);
                batch_tx = source!(Entry with {
                    obzenflow_adapters::middleware::rate_limit(rate)
                } => batch_tx_source);
                live_accounts = async_infinite_source!(Account => live_accounts_source);
                live_tx = async_infinite_source!(Entry => live_tx_source);

                accounts = merge!(Account => merges::then_live);
                transactions = merge!(Entry => merges::then_live);
                posted = join!(catalog accounts: Account, Entry -> Posted => post_entry);
                collector = sink!(Posted => collector);
            },

            topology: {
                batch_accounts |> accounts;
                live_accounts |> accounts;
                batch_tx |> transactions;
                live_tx |> transactions;
                (accounts, transactions) |> posted;
                posted |> collector;
            }
        })
    })
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
    .map_err(|_| anyhow!("timeout waiting for the pipeline to run"))?
}

async fn wait_for_postings(sink: &Collect, count: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let arrived = sink.arrived.notified();
            if sink.notes.lock().map(|notes| notes.len()).unwrap_or(0) >= count {
                return;
            }
            arrived.await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for {count} postings"))
}

async fn stop(handle: FlowHandle) -> Result<()> {
    handle.stop_graceful(Duration::from_secs(10)).await?;
    tokio::time::timeout(Duration::from_secs(15), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the pipeline to stop"))??;
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Row {
    Data(String),
    Phase(JoinPhaseFact),
    Eof { kind: EofKind },
}

fn rows(records: &[JournalRecord<ChainPayload>]) -> Vec<Row> {
    records
        .iter()
        .filter_map(|record| match &record.payload {
            ChainPayload::Fact(value) => Some(Row::Data(
                value
                    .get("note")
                    .or_else(|| value.get("key"))
                    .and_then(|note| note.as_str())
                    .unwrap_or_default()
                    .to_string(),
            )),
            ChainPayload::Execution(ExecutionPayload::JoinPhase(fact)) => {
                Some(Row::Phase(fact.clone()))
            }
            ChainPayload::FlowControl(FlowControlPayload::Eof { kind, .. }) => {
                Some(Row::Eof { kind: *kind })
            }
            _ => None,
        })
        .collect()
}

async fn stage_rows(run: &Path, stage: &str) -> Vec<Row> {
    rows(&replay_testkit::read_stage_envelopes_appended(run, stage).await)
}

fn data(rows: &[Row]) -> Vec<&str> {
    rows.iter()
        .filter_map(|row| match row {
            Row::Data(note) => Some(note.as_str()),
            _ => None,
        })
        .collect()
}

fn position(rows: &[Row], wanted: impl Fn(&Row) -> bool) -> usize {
    rows.iter()
        .position(wanted)
        .unwrap_or_else(|| panic!("row missing from {rows:?}"))
}

fn entries(prefix: &str, count: usize) -> Vec<Entry> {
    (1..=count)
        .map(|n| {
            entry(
                if n % 2 == 0 { "acct-2" } else { "acct-1" },
                &format!("{prefix}-{n}"),
            )
        })
        .collect()
}

fn batch_entries() -> Vec<Entry> {
    entries("batch", 6)
}

#[tokio::test]
async fn live_rows_queued_before_the_run_post_after_every_batch_row() -> Result<()> {
    let _guard = THEN_LIVE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (accounts_tx, live_accounts) = mpsc::channel(8);
    let (entries_tx, live_entries) = mpsc::channel(8);
    accounts_tx.send(account("acct-3")).await?;
    entries_tx.send(entry("acct-1", "live-1")).await?;
    entries_tx.send(entry("acct-2", "live-2")).await?;

    let sink = Collect {
        notes: Arc::default(),
        arrived: Arc::default(),
    };
    let handle = build(
        journal_base.clone(),
        Inputs {
            batch_entries: batch_entries(),
            live_accounts,
            live_entries,
            batch_rate: None,
        },
        sink.clone(),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    wait_for_postings(&sink, 8).await?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    assert_eq!(
        data(&stage_rows(&run, "transactions").await),
        ["batch-1", "batch-2", "batch-3", "batch-4", "batch-5", "batch-6", "live-1", "live-2"]
    );
    assert_eq!(
        data(&stage_rows(&run, "accounts").await),
        ["acct-1", "acct-2", "acct-3"]
    );

    let posted = stage_rows(&run, "posted").await;
    let enriching = position(&posted, |row| {
        matches!(row, Row::Phase(JoinPhaseFact::EnrichingEntered { .. }))
    });
    let live = position(&posted, |row| {
        matches!(row, Row::Phase(JoinPhaseFact::LiveEntered { .. }))
    });
    let first_batch = position(&posted, |row| *row == Row::Data("batch-1".into()));
    let last_batch = position(&posted, |row| *row == Row::Data("batch-6".into()));
    let first_live = position(&posted, |row| *row == Row::Data("live-1".into()));
    assert!(
        enriching < first_batch && last_batch < live && live < first_live,
        "{posted:?}"
    );
    assert_eq!(
        posted[enriching],
        Row::Phase(JoinPhaseFact::EnrichingEntered {
            generation: 0,
            ended_by: vec![StageKey::from("batch_accounts")],
        })
    );
    assert_eq!(
        posted[live],
        Row::Phase(JoinPhaseFact::LiveEntered {
            generation: 0,
            ended_by: vec![StageKey::from("batch_tx")],
        })
    );

    let manifest = replay_testkit::archive_manifest(&run).to_string();
    assert_eq!(manifest.matches("\"Merge\"").count(), 2, "{manifest}");
    for stage in ["accounts", "transactions", "posted"] {
        replay_testkit::assert_eof_carries_the_ending_clock(&run, stage).await;
    }

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            run.as_os_str().to_os_string(),
        ])
        .run_async(build(
            journal_base.clone(),
            Inputs {
                batch_entries: batch_entries(),
                live_accounts: mpsc::channel(1).1,
                live_entries: mpsc::channel(1).1,
                batch_rate: None,
            },
            Collect {
                notes: Arc::default(),
                arrived: Arc::default(),
            },
        ))
        .await
        .map_err(|error| anyhow!("{error:?}"))?;
    let replay = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(run, replay);
    for stage in ["transactions", "accounts"] {
        assert_eq!(
            data(&stage_rows(&run, stage).await),
            data(&stage_rows(&replay, stage).await),
            "{stage}"
        );
    }
    let replayed = stage_rows(&replay, "posted").await;
    let phases = |rows: &[Row]| {
        rows.iter()
            .filter(|row| matches!(row, Row::Phase(_) | Row::Data(_)))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(phases(&posted), phases(&replayed));
    Ok(())
}

#[tokio::test]
async fn a_stop_during_the_batch_drains_without_reading_live_rows() -> Result<()> {
    let _guard = THEN_LIVE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (_accounts_tx, live_accounts) = mpsc::channel(8);
    let (entries_tx, live_entries) = mpsc::channel(8);
    entries_tx.send(entry("acct-1", "live-1")).await?;

    let sink = Collect {
        notes: Arc::default(),
        arrived: Arc::default(),
    };
    let handle = build(
        journal_base.clone(),
        Inputs {
            batch_entries: batch_entries(),
            live_accounts,
            live_entries,
            batch_rate: Some(2.0),
        },
        sink.clone(),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    wait_for_postings(&sink, 1).await?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    let transactions = stage_rows(&run, "transactions").await;
    assert!(!data(&transactions).contains(&"live-1"), "{transactions:?}");
    assert!(data(&transactions).len() < 6, "{transactions:?}");
    assert_eq!(
        transactions.last(),
        Some(&Row::Eof {
            kind: EofKind::Truncated
        })
    );

    let posted = stage_rows(&run, "posted").await;
    assert!(
        !posted
            .iter()
            .any(|row| matches!(row, Row::Phase(JoinPhaseFact::LiveEntered { .. }))),
        "{posted:?}"
    );
    assert!(!data(&posted).contains(&"live-1"), "{posted:?}");
    assert_eq!(
        posted.last(),
        Some(&Row::Eof {
            kind: EofKind::Truncated
        })
    );
    Ok(())
}

#[tokio::test]
async fn a_hold_longer_than_the_stall_ceiling_completes_under_enforce() -> Result<()> {
    let _guard = THEN_LIVE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (_accounts_tx, live_accounts) = mpsc::channel(8);
    let (entries_tx, live_entries) = mpsc::channel(8);
    for n in 1..=5 {
        entries_tx
            .send(entry("acct-1", &format!("live-{n}")))
            .await?;
    }

    let sink = Collect {
        notes: Arc::default(),
        arrived: Arc::default(),
    };
    let handle = build(
        journal_base.clone(),
        Inputs {
            batch_entries: batch_entries(),
            live_accounts,
            live_entries,
            batch_rate: Some(2.0),
        },
        sink.clone(),
    )
    .build(enforced_backpressure(2, 500))
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    wait_for_postings(&sink, 11).await?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    for stage in [
        "batch_accounts",
        "batch_tx",
        "live_accounts",
        "live_tx",
        "accounts",
        "transactions",
        "posted",
    ] {
        let records = replay_testkit::read_stage_envelopes_appended(&run, stage).await;
        assert!(
            !records
                .iter()
                .any(|record| record.event_type().ends_with("stall_detected")),
            "{stage} stalled"
        );
        assert!(
            !rows(&records).contains(&Row::Eof {
                kind: EofKind::Poison
            }),
            "{stage} ended in poison"
        );
    }
    assert_eq!(
        data(&stage_rows(&run, "posted").await),
        [
            "batch-1", "batch-2", "batch-3", "batch-4", "batch-5", "batch-6", "live-1", "live-2",
            "live-3", "live-4", "live-5"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_join_with_no_batch_stream_input_journals_only_live_entered() -> Result<()> {
    let _guard = THEN_LIVE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (_accounts_tx, live_accounts) = mpsc::channel::<Account>(8);
    let (entries_tx, live_entries) = mpsc::channel(8);
    entries_tx.send(entry("acct-1", "live-1")).await?;
    let sink = Collect {
        notes: Arc::default(),
        arrived: Arc::default(),
    };
    let collector = sink.clone();
    let journals = journal_base.clone();
    let inputs = Mutex::new(Some((live_accounts, live_entries)));
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let (live_accounts, live_entries) = inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("the flow is built once");
        let batch_accounts_source = ValuesSource::new(vec![account("acct-1")]);
        let live_accounts_source = ChannelSource::new(live_accounts);
        let live_tx_source = ChannelSource::new(live_entries);
        let post_entry = joins::inner_then_live(
            |account: &Account| account.key.clone(),
            |entry: &Entry| entry.key.clone(),
            |_account: Account, entry: Entry| Posted {
                key: entry.key,
                note: entry.note,
            },
        );
        let collector = collector.clone();
        Ok(flow! {
            name: "then_live_join_live_stream_test",
            journals: disk_journals(journals.clone()),

            stages: {
                batch_accounts = source!(Account => batch_accounts_source);
                live_accounts = async_infinite_source!(Account => live_accounts_source);
                live_tx = async_infinite_source!(Entry => live_tx_source);
                accounts = merge!(Account => merges::then_live);
                posted = join!(catalog accounts: Account, Entry -> Posted => post_entry);
                collector = sink!(Posted => collector);
            },

            topology: {
                batch_accounts |> accounts;
                live_accounts |> accounts;
                (accounts, live_tx) |> posted;
                posted |> collector;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    wait_for_postings(&sink, 1).await?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    let posted = stage_rows(&run, "posted").await;
    let phases: Vec<&Row> = posted
        .iter()
        .filter(|row| matches!(row, Row::Phase(_)))
        .collect();
    assert_eq!(
        phases,
        [&Row::Phase(JoinPhaseFact::LiveEntered {
            generation: 0,
            ended_by: vec![StageKey::from("batch_accounts")],
        })]
    );
    assert_eq!(data(&posted), ["live-1"]);
    Ok(())
}

fn completed(records: &[JournalRecord<ChainPayload>]) -> bool {
    records.iter().any(|record| {
        matches!(
            record.payload,
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                StageLifecycleFact::Completed { .. }
            ))
        )
    })
}

#[tokio::test]
async fn a_join_whose_stream_is_all_batch_drains_when_its_batch_ends() -> Result<()> {
    let _guard = THEN_LIVE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (_accounts_tx, live_accounts) = mpsc::channel::<Account>(8);
    let sink = Collect {
        notes: Arc::default(),
        arrived: Arc::default(),
    };
    let collector = sink.clone();
    let journals = journal_base.clone();
    let inputs = Mutex::new(Some(live_accounts));
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let live_accounts = inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("the flow is built once");
        let batch_accounts_source = ValuesSource::new(vec![account("acct-1"), account("acct-2")]);
        let live_accounts_source = ChannelSource::new(live_accounts);
        let batch_tx_source = ValuesSource::new(batch_entries());
        let post_entry = joins::inner_then_live(
            |account: &Account| account.key.clone(),
            |entry: &Entry| entry.key.clone(),
            |_account: Account, entry: Entry| Posted {
                key: entry.key,
                note: entry.note,
            },
        );
        let collector = collector.clone();
        Ok(flow! {
            name: "then_live_join_batch_stream_test",
            journals: disk_journals(journals.clone()),

            stages: {
                batch_accounts = source!(Account => batch_accounts_source);
                live_accounts = async_infinite_source!(Account => live_accounts_source);
                batch_tx = source!(Entry => batch_tx_source);
                accounts = merge!(Account => merges::then_live);
                posted = join!(catalog accounts: Account, Entry -> Posted => post_entry);
                collector = sink!(Posted => collector);
            },

            topology: {
                batch_accounts |> accounts;
                live_accounts |> accounts;
                (accounts, batch_tx) |> posted;
                posted |> collector;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    wait_for_postings(&sink, batch_entries().len()).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !completed(&replay_testkit::read_stage_envelopes_appended(&run, "posted").await) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("the join never completed before the stop"))?;

    let before_stop = stage_rows(&run, "posted").await;
    let phases: Vec<&Row> = before_stop
        .iter()
        .filter(|row| matches!(row, Row::Phase(_)))
        .collect();
    assert_eq!(
        phases,
        [&Row::Phase(JoinPhaseFact::EnrichingEntered {
            generation: 0,
            ended_by: vec![StageKey::from("batch_accounts")],
        })]
    );
    assert_eq!(
        data(&before_stop),
        ["batch-1", "batch-2", "batch-3", "batch-4", "batch-5", "batch-6"]
    );
    assert_eq!(
        before_stop.last(),
        Some(&Row::Eof {
            kind: EofKind::Natural
        })
    );

    let state = handle.state_receiver();
    stop(handle).await?;
    assert_eq!(
        *state.borrow(),
        PipelineState::Cancelled {
            reason: "user_stop".to_string()
        }
    );
    assert_eq!(stage_rows(&run, "posted").await, before_stop);
    assert_eq!(
        replay_testkit::passing_verdicts(&run).await,
        replay_testkit::one_verdict_per_edge(&[
            "batch_accounts -> accounts",
            "live_accounts -> accounts",
            "batch_tx -> posted",
            "posted -> collector",
        ]),
        "the join never reads its catalog to its end, so that edge has no verdict"
    );
    Ok(())
}

async fn wait_for_completed(run: &Path, stage: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !completed(&replay_testkit::read_stage_envelopes_appended(run, stage).await) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("{stage} never completed"))
}

async fn wait_for_data_rows(run: &Path, stage: &str, count: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while data(&stage_rows(run, stage).await).len() < count {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for {count} data rows in {stage}"))
}

async fn assert_no_stage_stalled(run: &Path, stages: &[&str]) {
    for stage in stages {
        let records = replay_testkit::read_stage_envelopes_appended(run, stage).await;
        assert!(
            !records
                .iter()
                .any(|record| record.event_type().ends_with("stall_detected")),
            "{stage} stalled"
        );
        assert!(
            !rows(&records).contains(&Row::Eof {
                kind: EofKind::Poison
            }),
            "{stage} ended in poison"
        );
    }
}

#[tokio::test]
async fn a_join_with_no_live_phase_releases_its_live_catalog_under_enforce() -> Result<()> {
    let _guard = THEN_LIVE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (accounts_tx, live_accounts) = mpsc::channel::<Account>(8);
    let sink = Collect {
        notes: Arc::default(),
        arrived: Arc::default(),
    };
    let collector = sink.clone();
    let journals = journal_base.clone();
    let inputs = Mutex::new(Some(live_accounts));
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let live_accounts = inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("the flow is built once");
        let batch_accounts_source = ValuesSource::new(vec![account("acct-1"), account("acct-2")]);
        let live_accounts_source = ChannelSource::new(live_accounts);
        let batch_tx_source = ValuesSource::new(batch_entries());
        let post_entry = joins::inner_then_live(
            |account: &Account| account.key.clone(),
            |entry: &Entry| entry.key.clone(),
            |_account: Account, entry: Entry| Posted {
                key: entry.key,
                note: entry.note,
            },
        );
        let collector = collector.clone();
        Ok(flow! {
            name: "then_live_join_released_catalog_test",
            journals: disk_journals(journals.clone()),

            stages: {
                batch_accounts = source!(Account => batch_accounts_source);
                live_accounts = async_infinite_source!(Account => live_accounts_source);
                batch_tx = source!(Entry => batch_tx_source);
                accounts = merge!(Account => merges::then_live);
                posted = join!(catalog accounts: Account, Entry -> Posted => post_entry);
                collector = sink!(Posted => collector);
            },

            topology: {
                batch_accounts |> accounts;
                live_accounts |> accounts;
                (accounts, batch_tx) |> posted;
                posted |> collector;
            }
        })
    })
    .build(enforced_backpressure(2, 500))
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    wait_for_postings(&sink, batch_entries().len()).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);
    wait_for_completed(&run, "posted").await?;

    for n in 1..=5 {
        accounts_tx.send(account(&format!("live-{n}"))).await?;
    }
    wait_for_data_rows(&run, "accounts", 7).await?;

    let state = handle.state_receiver();
    stop(handle).await?;
    assert_eq!(
        *state.borrow(),
        PipelineState::Cancelled {
            reason: "user_stop".to_string()
        }
    );
    assert_no_stage_stalled(
        &run,
        &[
            "batch_accounts",
            "live_accounts",
            "batch_tx",
            "accounts",
            "posted",
        ],
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn a_live_join_releases_its_catalog_when_its_stream_ends_under_enforce() -> Result<()> {
    let _guard = THEN_LIVE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (accounts_tx, live_accounts) = mpsc::channel::<Account>(8);
    let sink = Collect {
        notes: Arc::default(),
        arrived: Arc::default(),
    };
    let collector = sink.clone();
    let journals = journal_base.clone();
    let inputs = Mutex::new(Some(live_accounts));
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let live_accounts = inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("the flow is built once");
        let live_accounts_source = ChannelSource::new(live_accounts);
        let batch_tx_source = ValuesSource::new(batch_entries());
        let post_entry = joins::inner_live(
            |account: &Account| account.key.clone(),
            |entry: &Entry| entry.key.clone(),
            |_account: Account, entry: Entry| Posted {
                key: entry.key,
                note: entry.note,
            },
        );
        let collector = collector.clone();
        Ok(flow! {
            name: "live_join_released_catalog_test",
            journals: disk_journals(journals.clone()),

            stages: {
                live_accounts = async_infinite_source!(Account => live_accounts_source);
                batch_tx = source!(Entry => batch_tx_source);
                posted = join!(catalog live_accounts: Account, Entry -> Posted => post_entry);
                collector = sink!(Posted => collector);
            },

            topology: {
                (live_accounts, batch_tx) |> posted;
                posted |> collector;
            }
        })
    })
    .build(enforced_backpressure(2, 500))
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);
    wait_for_completed(&run, "posted").await?;

    for n in 1..=5 {
        accounts_tx.send(account(&format!("live-{n}"))).await?;
    }
    wait_for_data_rows(&run, "live_accounts", 5).await?;

    let state = handle.state_receiver();
    stop(handle).await?;
    assert_eq!(
        *state.borrow(),
        PipelineState::Cancelled {
            reason: "user_stop".to_string()
        }
    );
    assert_no_stage_stalled(&run, &["live_accounts", "batch_tx", "posted"]).await;
    Ok(())
}

#[tokio::test]
async fn a_finite_eof_join_stopped_while_hydrating_posts_nothing() -> Result<()> {
    let _guard = THEN_LIVE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let journals = journal_base.clone();
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let catalog_source = ValuesSource::new(
            (1..=12)
                .map(|n| account(&format!("acct-{n}")))
                .collect::<Vec<_>>(),
        );
        let stream_source = ValuesSource::new(vec![entry("acct-1", "batch-1")]);
        let post_entry = joins::inner(
            |account: &Account| account.key.clone(),
            |entry: &Entry| entry.key.clone(),
            |_account: Account, entry: Entry| Posted {
                key: entry.key,
                note: entry.note,
            },
        );
        let collector = Collect {
            notes: Arc::default(),
            arrived: Arc::default(),
        };
        Ok(flow! {
            name: "finite_eof_stopped_hydration_test",
            journals: disk_journals(journals.clone()),

            stages: {
                catalog = source!(Account with {
                    obzenflow_adapters::middleware::rate_limit(2.0)
                } => catalog_source);
                ledger = source!(Entry => stream_source);
                posted = join!(catalog catalog: Account, Entry -> Posted => post_entry);
                collector = sink!(Posted => collector);
            },

            topology: {
                (catalog, ledger) |> posted;
                posted |> collector;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    let probe = journal_base.clone();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(run) = std::fs::read_dir(probe.join("flows"))
                .ok()
                .and_then(|mut runs| runs.next())
                .and_then(|run| run.ok())
            {
                let catalog = stage_rows(&run.path(), "catalog").await;
                if data(&catalog).len() >= 3 {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("the catalog never started"))?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    let posted = stage_rows(&run, "posted").await;
    assert!(data(&posted).is_empty(), "{posted:?}");
    assert!(
        !posted.iter().any(|row| matches!(row, Row::Phase(_))),
        "{posted:?}"
    );
    assert_eq!(
        posted.last(),
        Some(&Row::Eof {
            kind: EofKind::Truncated
        })
    );
    Ok(())
}

fn build_two_per_side(
    journal_base: PathBuf,
    live: (mpsc::Receiver<Entry>, mpsc::Receiver<Entry>),
    sink: Collect,
) -> FlowDefinition {
    let live = Mutex::new(Some(live));
    FlowDefinition::materialize(move |_runtime_config| {
        let (live_a, live_b) = live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("the flow is built once");
        let batch_accounts_source = ValuesSource::new(vec![account("acct-1"), account("acct-2")]);
        let batch_a_source = ValuesSource::new(entries("batch-a", 4));
        let batch_b_source = ValuesSource::new(entries("batch-b", 2));
        let live_a_source = ChannelSource::new(live_a);
        let live_b_source = ChannelSource::new(live_b);
        let post_entry = joins::inner_then_live(
            |account: &Account| account.key.clone(),
            |entry: &Entry| entry.key.clone(),
            |_account: Account, entry: Entry| Posted {
                key: entry.key,
                note: entry.note,
            },
        );
        let collector = sink.clone();
        Ok(flow! {
            name: "then_live_two_per_side_test",
            journals: disk_journals(journal_base.clone()),

            stages: {
                batch_accounts = source!(Account => batch_accounts_source);
                batch_a = source!(Entry with {
                    obzenflow_adapters::middleware::rate_limit(2.0)
                } => batch_a_source);
                batch_b = source!(Entry => batch_b_source);
                live_a = async_infinite_source!(Entry => live_a_source);
                live_b = async_infinite_source!(Entry => live_b_source);

                transactions = merge!(Entry => merges::then_live);
                posted = join!(catalog batch_accounts: Account, Entry -> Posted => post_entry);
                collector = sink!(Posted => collector);
            },

            topology: {
                batch_a |> transactions;
                batch_b |> transactions;
                live_a |> transactions;
                live_b |> transactions;
                (batch_accounts, transactions) |> posted;
                posted |> collector;
            }
        })
    })
}

#[tokio::test]
async fn a_merge_reads_live_only_after_every_batch_producer_ends() -> Result<()> {
    let _guard = THEN_LIVE_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (live_a_tx, live_a) = mpsc::channel(8);
    let (live_b_tx, live_b) = mpsc::channel(8);
    for n in 1..=2 {
        live_a_tx
            .send(entry("acct-1", &format!("live-a-{n}")))
            .await?;
        live_b_tx
            .send(entry("acct-2", &format!("live-b-{n}")))
            .await?;
    }

    let sink = Collect {
        notes: Arc::default(),
        arrived: Arc::default(),
    };
    let handle = build_two_per_side(journal_base.clone(), (live_a, live_b), sink.clone())
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    wait_for_postings(&sink, 10).await?;
    stop(handle).await?;
    let run = replay_testkit::latest_run_dir(&journal_base);

    let transactions = stage_rows(&run, "transactions").await;
    let merged = data(&transactions);
    assert_eq!(merged.len(), 10, "{transactions:?}");
    assert!(
        merged[..6].iter().all(|note| note.starts_with("batch-"))
            && merged[6..].iter().all(|note| note.starts_with("live-")),
        "{merged:?}"
    );
    let from = |producer: &str| {
        merged
            .iter()
            .copied()
            .filter(|note| note.starts_with(producer))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        from("batch-a-"),
        ["batch-a-1", "batch-a-2", "batch-a-3", "batch-a-4"]
    );
    assert_eq!(from("batch-b-"), ["batch-b-1", "batch-b-2"]);
    assert_eq!(from("live-a-"), ["live-a-1", "live-a-2"]);
    assert_eq!(from("live-b-"), ["live-b-1", "live-b-2"]);

    let first_live = position(
        &transactions,
        |row| matches!(row, Row::Data(note) if note.starts_with("live-")),
    );
    let natural = Row::Eof {
        kind: EofKind::Natural,
    };
    assert_eq!(
        transactions[..first_live]
            .iter()
            .filter(|row| **row == natural)
            .count(),
        2,
        "{transactions:?}"
    );

    let records = replay_testkit::read_stage_envelopes_appended(&run, "transactions").await;
    let last_batch = records
        .iter()
        .rposition(|record| {
            matches!(&record.payload, ChainPayload::Fact(value)
                if value
                    .get("note")
                    .and_then(|note| note.as_str())
                    .is_some_and(|note| note.starts_with("batch-")))
        })
        .expect("the merge journalled batch rows");
    let selections: Vec<usize> = records
        .iter()
        .enumerate()
        .filter(|(_, record)| record.event_type().ends_with("input_selected"))
        .map(|(index, _)| index)
        .collect();
    assert!(
        !selections.is_empty() && selections.iter().all(|index| *index > last_batch),
        "selections at {selections:?}, last batch row at {last_batch}"
    );

    let posted = stage_rows(&run, "posted").await;
    assert_eq!(data(&posted), merged);
    let mut ended_by = posted
        .iter()
        .find_map(|row| match row {
            Row::Phase(JoinPhaseFact::LiveEntered { ended_by, .. }) => Some(ended_by.clone()),
            _ => None,
        })
        .expect("the join went live");
    ended_by.sort();
    assert_eq!(
        ended_by,
        [StageKey::from("batch_a"), StageKey::from("batch_b")]
    );

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            run.as_os_str().to_os_string(),
        ])
        .run_async(build_two_per_side(
            journal_base.clone(),
            (mpsc::channel(1).1, mpsc::channel(1).1),
            Collect {
                notes: Arc::default(),
                arrived: Arc::default(),
            },
        ))
        .await
        .map_err(|error| anyhow!("{error:?}"))?;
    let replay = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(run, replay);
    assert_eq!(merged, data(&stage_rows(&replay, "transactions").await));
    assert_eq!(data(&posted), data(&stage_rows(&replay, "posted").await));
    Ok(())
}
