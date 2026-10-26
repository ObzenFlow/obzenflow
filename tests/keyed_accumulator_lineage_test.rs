// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod replay_testkit;

use obzenflow::stages::stateful::{self, emit};
use obzenflow_core::{EventId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, stateful, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{render_verdict, verify_run_dirs, VerifyOptions};
use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

const ENTRIES: u64 = 200;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Entry {
    account: String,
    amount: u64,
}

impl TypedPayload for Entry {
    const EVENT_TYPE: &'static str = "keyed_lineage.entry";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Balance {
    account: String,
    total: u64,
    entries: u64,
}

impl TypedPayload for Balance {
    const EVENT_TYPE: &'static str = "keyed_lineage.balance";
}

#[derive(Clone, Debug, Default)]
struct Ledger {
    total: u64,
    entries: u64,
}

type Balances = stateful::GroupBy<
    Entry,
    String,
    Ledger,
    Balance,
    fn(&Entry) -> String,
    fn(&mut Ledger, &Entry),
    fn(&String, &Ledger) -> Balance,
>;

fn balances() -> Balances {
    stateful::GroupBy::new(
        |entry| entry.account.clone(),
        |ledger, entry| {
            ledger.total += entry.amount;
            ledger.entries += 1;
        },
        |account, ledger| Balance {
            account: account.clone(),
            total: ledger.total,
            entries: ledger.entries,
        },
    )
}

fn account(index: u64) -> &'static str {
    if index % 4 == 1 {
        "b"
    } else {
        "a"
    }
}

#[derive(Clone, Debug)]
struct Entries {
    next: u64,
}

impl TypedFiniteSourceHandler for Entries {
    type Output = Entry;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next == ENTRIES {
            return Ok(None);
        }
        let index = self.next;
        self.next += 1;
        Ok(Some(vec![Entry {
            account: account(index).into(),
            amount: index + 1,
        }]))
    }
}

fn build_flow(journal_base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let entries = Entries { next: 0 };
        let updates = balances().emit(emit::always);
        let snapshots = balances().emit(emit::always).all_groups();
        let update_sink = replay_testkit::Discard::<Balance>::default();
        let snapshot_sink = replay_testkit::Discard::<Balance>::default();

        Ok(flow! {
            name: "keyed_accumulator_lineage",
            journals: disk_journals(journal_base),

            stages: {
                entries = source!(Entry => entries);
                updates = stateful!(Entry -> Balance => updates);
                snapshots = stateful!(Entry -> Balance => snapshots);
                update_sink = sink!(Balance => update_sink);
                snapshot_sink = sink!(Balance => snapshot_sink);
            },

            topology: {
                entries |> updates;
                entries |> snapshots;
                updates |> update_sink;
                snapshots |> snapshot_sink;
            }
        })
    })
}

async fn run(journal_base: &Path, config: Option<&Path>, replay_from: Option<&Path>) -> PathBuf {
    let mut args = vec![OsString::from("keyed-accumulator-lineage")];
    if let Some(archive) = replay_from {
        args.extend([
            OsString::from("--replay-from"),
            archive.as_os_str().to_os_string(),
            OsString::from("--verify"),
        ]);
    }
    let mut application = FlowApplication::builder().with_cli_args(args);
    if let Some(config) = config {
        application = application.with_config_file(config);
    }
    application
        .run_async(build_flow(journal_base.to_path_buf()))
        .await
        .expect("keyed lineage flow completes");
    replay_testkit::latest_run_dir(journal_base)
}

struct StageLineage {
    inputs: HashMap<EventId, String>,
    outputs: Vec<(EventId, Balance, Vec<EventId>)>,
}

async fn read_lineage(run_dir: &Path, stage: &str) -> StageLineage {
    let inputs = replay_testkit::read_stage_envelopes_appended(run_dir, "entries")
        .await
        .iter()
        .filter_map(|record| {
            Entry::from_event(&record.authored())
                .map(|entry| (record.envelope.provenance.event.id, entry.account))
        })
        .collect();
    let outputs = replay_testkit::read_stage_envelopes_appended(run_dir, stage)
        .await
        .iter()
        .filter_map(|record| {
            let output = record.authored();
            Balance::from_event(&output)
                .map(|balance| (output.id, balance, output.causality.parent_ids.clone()))
        })
        .collect();
    StageLineage { inputs, outputs }
}

fn first_inputs(lineage: &StageLineage, run_inputs: &[EventId]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut firsts: Vec<(EventId, String)> = run_inputs
        .iter()
        .filter_map(|id| {
            let account = lineage.inputs[id].clone();
            seen.insert(account.clone()).then_some((*id, account))
        })
        .collect();
    firsts.sort();
    firsts.into_iter().map(|(_, account)| account).collect()
}

fn assert_keyed_lineage(lineage: &StageLineage, cap: usize, one_input_per_output: bool) {
    let own: HashMap<EventId, &str> = lineage
        .outputs
        .iter()
        .map(|(id, balance, _)| (*id, balance.account.as_str()))
        .collect();
    let parents_of: HashMap<EventId, &[EventId]> = lineage
        .outputs
        .iter()
        .map(|(id, _, parents)| (*id, parents.as_slice()))
        .collect();
    let mut previous: HashMap<&str, EventId> = HashMap::new();

    for (id, balance, parents) in &lineage.outputs {
        let account = balance.account.as_str();
        let (inputs, links): (Vec<EventId>, Vec<EventId>) = parents
            .iter()
            .partition(|parent| lineage.inputs.contains_key(parent));
        assert!(
            inputs.iter().all(|input| lineage.inputs[input] == account),
            "{account}'s output {id} cites another key's input: {parents:?}"
        );
        assert!(
            links.iter().all(|link| own.get(link) == Some(&account)),
            "{account}'s output {id} names a parent that is no output of its key: {parents:?}"
        );
        let expected_link: Vec<EventId> = previous.get(account).copied().into_iter().collect();
        assert_eq!(
            links, expected_link,
            "{account}'s output {id} links to its key's previous output alone"
        );
        assert_eq!(parents.last(), expected_link.last().or(inputs.last()));
        assert!(
            inputs.len() <= cap,
            "{account}'s output {id} cites past the cap"
        );
        if one_input_per_output {
            assert_eq!(
                inputs.len(),
                1,
                "{account}'s output {id} cites its own input"
            );
        }
        previous.insert(account, *id);
    }

    for (account, last) in &previous {
        let mut reached = HashSet::new();
        let mut next = Some(*last);
        while let Some(output) = next {
            let parents = parents_of[&output];
            reached.extend(
                parents
                    .iter()
                    .filter(|parent| lineage.inputs.contains_key(parent)),
            );
            next = parents
                .iter()
                .copied()
                .find(|parent| own.contains_key(parent));
        }
        let expected: HashSet<EventId> = lineage
            .inputs
            .iter()
            .filter(|(_, input_account)| input_account == account)
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(
            reached, expected,
            "following {account}'s links reaches every one of its inputs"
        );
    }
}

async fn assert_run(run_dir: &Path, cap: usize) {
    let source_order: Vec<EventId> =
        replay_testkit::read_stage_envelopes_appended(run_dir, "entries")
            .await
            .iter()
            .filter(|record| Entry::from_event(&record.authored()).is_some())
            .map(|record| record.envelope.provenance.event.id)
            .collect();
    assert_eq!(source_order.len(), ENTRIES as usize);

    let updates = read_lineage(run_dir, "updates").await;
    assert_eq!(
        updates.outputs.len(),
        ENTRIES as usize,
        "one output per update"
    );
    assert_keyed_lineage(&updates, cap, true);
    let published: Vec<String> = updates
        .outputs
        .iter()
        .map(|(_, balance, _)| balance.account.clone())
        .collect();
    let expected: Vec<String> = (0..ENTRIES).map(|index| account(index).into()).collect();
    assert_eq!(
        published, expected,
        "each input publishes the key it updated"
    );

    let snapshots = read_lineage(run_dir, "snapshots").await;
    assert_keyed_lineage(&snapshots, cap, false);
    let order = first_inputs(&snapshots, &source_order);
    let published: Vec<&str> = snapshots
        .outputs
        .iter()
        .map(|(_, balance, _)| balance.account.as_str())
        .collect();
    assert_eq!(
        published.len(),
        1 + 2 * (ENTRIES as usize - 1),
        "each later input publishes both keys, and end of input repeats none"
    );
    assert_eq!(published[0], "a");
    assert!(
        published[1..].chunks(2).all(|pair| pair == order),
        "every emission publishes its keys in first-input order {order:?}"
    );

    for lineage in [&updates, &snapshots] {
        let mut finals = HashMap::new();
        for (_, balance, _) in &lineage.outputs {
            finals.insert(balance.account.clone(), balance.clone());
        }
        for key in ["a", "b"] {
            let indexes = (0..ENTRIES).filter(|index| account(*index) == key);
            let expected = Balance {
                account: key.into(),
                total: indexes.clone().map(|index| index + 1).sum(),
                entries: indexes.count() as u64,
            };
            assert_eq!(finals.get(key), Some(&expected));
        }
    }
}

async fn assert_verified_replay(
    journal_base: &Path,
    config: Option<&Path>,
    live: &Path,
    cap: usize,
) {
    let replay = run(journal_base, config, Some(live)).await;
    assert_ne!(replay, live);
    let outcome =
        verify_run_dirs(live, &replay, &VerifyOptions::default()).expect("verification runs");
    assert_eq!(
        outcome.exit_code(),
        0,
        "a strict replay matches its archive: {}",
        render_verdict(&outcome)
    );
    assert_run(&replay, cap).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn keyed_outputs_cite_their_own_key_and_link_past_the_default_cap() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");

    let live = run(&journal_base, None, None).await;
    assert_run(&live, obzenflow_core::config::DEFAULT_MAX_LINEAGE_DEPTH).await;
    assert_verified_replay(
        &journal_base,
        None,
        &live,
        obzenflow_core::config::DEFAULT_MAX_LINEAGE_DEPTH,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn keyed_outputs_link_past_a_stage_cap_of_one() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let config = temp.path().join("obzenflow.toml");
    std::fs::write(
        &config,
        "[runtime.stages.updates]\nmax_lineage_depth = 1\n\n\
         [runtime.stages.snapshots]\nmax_lineage_depth = 1\n",
    )
    .expect("stage cap configuration");

    let live = run(&journal_base, Some(&config), None).await;
    assert_run(&live, 1).await;
    assert_verified_replay(&journal_base, Some(&config), &live, 1).await;
}
