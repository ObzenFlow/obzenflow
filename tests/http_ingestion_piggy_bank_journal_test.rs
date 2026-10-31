// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../examples/http_ingestion_piggy_bank_demo/support.rs"]
pub mod support;

use obzenflow::application::ingress::{
    ingress_source, IngestionConfig, IngressOutcome, IngressRequestId,
};
use obzenflow::application::{
    verify_run_dirs, FlowApplication, LogLevel, VerifyOptions, VerifyOutcome,
};
use obzenflow::journal::read::{
    open_disk_run, read_config_changes, ChainPayload, RunJournalKind, RunRecordData,
};
use obzenflow::journal::JournalRecord;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;
use support::domain::AchDebit;
use support::flow::{assemble_flow, AccountIngress, AchIngress, LedgerIngress};

const RETRY_KEY: &str = "ach-20260306-000412";

const RETURNS: [(&str, &str, u64); 6] = [
    ("chk-58213", "NORTHSIDE FIT", 1),
    ("chk-60472", "SPARKLE TV", 1),
    ("chk-44108", "BLUE RIVER INS", 1),
    ("chk-60472", "SPARKLE TV", 2),
    ("chk-60472", "SPARKLE TV", 3),
    ("chk-58213", "NORTHSIDE FIT", 2),
];

fn example_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/http_ingestion_piggy_bank_demo")
}

fn app_config(dir: &Path, shipped: &str) -> PathBuf {
    let mut config: toml::Table = std::fs::read_to_string(example_dir().join(shipped))
        .expect("the shipped configuration is readable")
        .parse()
        .expect("the shipped configuration parses");
    let Some(toml::Value::Table(mut app)) = config.remove("app") else {
        panic!("{shipped} has no [app] table");
    };
    for key in ["accounts_file", "transactions_file", "ach_debits_file"] {
        let file = Path::new(env!("CARGO_MANIFEST_DIR")).join(
            app.get(key)
                .and_then(toml::Value::as_str)
                .unwrap_or_else(|| panic!("{shipped} names {key}")),
        );
        app.insert(
            key.to_owned(),
            toml::Value::String(file.display().to_string()),
        );
    }
    let path = dir.join(shipped);
    let config = toml::Table::from_iter([("app".to_owned(), toml::Value::Table(app))]);
    std::fs::write(
        &path,
        toml::to_string(&config).expect("configuration serializes"),
    )
    .expect("configuration is written");
    path
}

fn ingress_config(base_path: &str) -> IngestionConfig {
    IngestionConfig {
        base_path: base_path.to_owned(),
        ..Default::default()
    }
}

fn retry() -> AchDebit {
    AchDebit {
        account_id: "chk-58213".to_owned(),
        company_name: "NORTHSIDE FIT".to_owned(),
        company_id: "1834577921".to_owned(),
        entry_description: "RETRY PYMT".to_owned(),
        amount_cents: 4999,
    }
}

fn is_retry_return(record: &JournalRecord<ChainPayload>) -> bool {
    let payload = record.payload();
    record.event_type() == "bank.payment_returned"
        && payload["account_id"] == "chk-58213"
        && payload["presentment"] == 2
}

async fn record_fee_run(config: &Path, journal_root: &Path) -> (IngressOutcome, IngressOutcome) {
    let mut app = FlowApplication::builder()
        .with_log_level(LogLevel::Error)
        .with_cli_args([
            OsString::from("http_ingestion_piggy_bank_demo"),
            OsString::from("--config"),
            config.as_os_str().to_os_string(),
        ]);
    let accounts = app.ingress(ingress_source(
        AccountIngress,
        ingress_config("/api/bank/accounts"),
    ));
    let entries = app.ingress(ingress_source(
        LedgerIngress,
        ingress_config("/api/bank/tx"),
    ));
    let ach = ingress_source(AchIngress, ingress_config("/api/bank/ach"));
    let submit = ach.handle();
    let debits = app.ingress(ach);
    let (outcomes_tx, outcomes_rx) = tokio::sync::oneshot::channel();
    let outcomes_tx = std::sync::Mutex::new(Some(outcomes_tx));
    let app = app.with_flow_handle_hook(move |flow| {
        let flow = flow.clone();
        let submit = submit.clone();
        let outcomes_tx = outcomes_tx.lock().unwrap().take();
        tokio::spawn(async move {
            let key = IngressRequestId::parse(RETRY_KEY).expect("a request id");
            let committed = loop {
                match submit.submit_identified(key.clone(), retry()).await {
                    IngressOutcome::Refused {
                        retry_after: Some(wait),
                        ..
                    }
                    | IngressOutcome::Unknown { retry_after: wait } => {
                        tokio::time::sleep(wait.min(Duration::from_millis(50))).await;
                    }
                    outcome => break outcome,
                }
            };
            let resent = submit.submit_identified(key, retry()).await;
            'decided: loop {
                for (_, journal) in flow.stage_journals() {
                    let tail = journal.read_last_n(4).await.expect("a journal tail reads");
                    if tail.iter().any(is_retry_return) {
                        break 'decided;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            flow.stop().await.expect("the run stops");
            if let Some(outcomes_tx) = outcomes_tx {
                let _ = outcomes_tx.send((committed, resent));
            }
        })
    });
    tokio::time::timeout(
        Duration::from_secs(60),
        app.run_async(assemble_flow(
            journal_root.to_path_buf(),
            accounts,
            entries,
            debits,
        )),
    )
    .await
    .expect("the fee run stops within a minute")
    .expect("the fee run ends cleanly");
    outcomes_rx.await.expect("the retry was submitted")
}

async fn replay(config: &Path, journal_root: &Path, archive: &Path) {
    let mut app = FlowApplication::builder()
        .with_log_level(LogLevel::Error)
        .with_cli_args([
            OsString::from("http_ingestion_piggy_bank_demo"),
            OsString::from("--config"),
            config.as_os_str().to_os_string(),
            OsString::from("--replay-from"),
            archive.as_os_str().to_os_string(),
        ]);
    let accounts = app.ingress(ingress_source(
        AccountIngress,
        ingress_config("/api/bank/accounts"),
    ));
    let entries = app.ingress(ingress_source(
        LedgerIngress,
        ingress_config("/api/bank/tx"),
    ));
    let debits = app.ingress(ingress_source(AchIngress, ingress_config("/api/bank/ach")));
    tokio::time::timeout(
        Duration::from_secs(60),
        app.run_async(assemble_flow(
            journal_root.to_path_buf(),
            accounts,
            entries,
            debits,
        )),
    )
    .await
    .expect("the replay ends within a minute")
    .expect("the replay records no stage fatal");
}

fn runs(journal_root: &Path) -> Vec<PathBuf> {
    let mut runs: Vec<_> = std::fs::read_dir(journal_root.join("flows"))
        .expect("flows directory")
        .map(|entry| entry.expect("run entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    runs.sort();
    runs
}

async fn rows(run: &Path, stage: &str) -> Vec<JournalRecord<ChainPayload>> {
    let mut snapshot = open_disk_run(run).await.expect("the archive opens");
    let mut rows = Vec::new();
    while let Some(record) = snapshot.next().await.expect("the archive reads") {
        let Some(owner) = record.journal.stage else {
            continue;
        };
        if owner.key != stage {
            continue;
        }
        if let (RunJournalKind::Data, RunRecordData::Chain(row)) =
            (record.journal.kind, record.record)
        {
            rows.push(*row);
        }
    }
    rows
}

async fn facts(run: &Path, stage: &str, event_type: &str) -> Vec<serde_json::Value> {
    rows(run, stage)
        .await
        .into_iter()
        .filter(|row| matches!(row.payload, ChainPayload::Fact(_)))
        .filter(|row| row.event_type() == event_type)
        .map(|row| row.payload())
        .collect()
}

async fn balances_chain_from_their_opening_balances(run: &Path) {
    let entries: HashMap<_, _> = rows(run, "posted")
        .await
        .into_iter()
        .filter(|row| row.event_type() == "bank.posted_entry")
        .map(|row| (row.envelope.provenance.event.id, row.payload()))
        .collect();
    let mut balances: BTreeMap<String, i64> = BTreeMap::new();
    for row in rows(run, "assess").await {
        if row.event_type() != "bank.balance_moved" {
            continue;
        }
        let moved = row.payload();
        let entry = row
            .envelope
            .provenance
            .event
            .causality
            .parent_ids
            .first()
            .and_then(|parent| entries.get(parent))
            .unwrap_or_else(|| panic!("{moved} names the posted entry that moved it"));
        assert_eq!(entry["account_id"], moved["account_id"], "{moved}");
        let account = moved["account_id"].as_str().expect("an account");
        let before = moved["balance_before_cents"]
            .as_i64()
            .expect("a balance before");
        let opening = entry["initial_balance_cents"]
            .as_i64()
            .expect("an opening balance");
        assert_eq!(
            before,
            *balances.get(account).unwrap_or(&opening),
            "{moved}"
        );
        let amount = moved["amount_cents"].as_i64().expect("an amount");
        let after = match moved["kind"].as_str() {
            Some("Credit") => before + amount,
            Some("Debit") => before - amount,
            other => panic!("an entry kind, not {other:?}"),
        };
        assert_eq!(moved["balance_cents"].as_i64(), Some(after), "{moved}");
        balances.insert(account.to_owned(), after);
    }
    assert!(!balances.is_empty(), "the run moved balances");
}

async fn printed_returns(run: &Path) -> Vec<serde_json::Value> {
    let returns: HashMap<_, _> = rows(run, "assess")
        .await
        .into_iter()
        .filter(|row| row.event_type() == "bank.payment_returned")
        .map(|row| (row.envelope.provenance.event.id, row.payload()))
        .collect();
    rows(run, "fee_report")
        .await
        .into_iter()
        .filter(|row| row.event_type() == "delivery.succeeded")
        .map(|row| {
            row.envelope
                .provenance
                .event
                .causality
                .parent_ids
                .first()
                .and_then(|parent| returns.get(parent))
                .cloned()
                .expect("each printed line follows a return")
        })
        .collect()
}

fn decisions(returned: &[serde_json::Value]) -> Vec<(String, String, u64, u64, String)> {
    returned
        .iter()
        .map(|fact| {
            (
                fact["account_id"].as_str().expect("account").to_owned(),
                fact["company_name"].as_str().expect("company").to_owned(),
                fact["presentment"].as_u64().expect("presentment"),
                fact["fee_cents"].as_u64().expect("fee"),
                fact["fee_rule"].as_str().expect("rule").to_owned(),
            )
        })
        .collect()
}

fn expected(fees: [u64; 6], rule: &str) -> Vec<(String, String, u64, u64, String)> {
    RETURNS
        .iter()
        .zip(fees)
        .map(|((account, company, presentment), fee)| {
            (
                (*account).to_owned(),
                (*company).to_owned(),
                *presentment,
                fee,
                rule.to_owned(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_charged_under_v1_replays_under_v2_with_no_fee() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_root = dir.path().join("journals");
    let v1 = app_config(dir.path(), "obzenflow.toml");
    let v2 = app_config(dir.path(), "obzenflow.fee-rule-v2.toml");

    let (committed, resent) = record_fee_run(&v1, &journal_root).await;
    let IngressOutcome::Committed { event_id } = committed else {
        panic!("the retry commits: {committed:?}");
    };
    assert!(
        matches!(resent, IngressOutcome::Duplicate { event_id: original } if original == event_id),
        "a resent retry under the same key posts once: {resent:?}"
    );

    let baseline = runs(&journal_root).pop().expect("the fee run's archive");
    let charged = facts(&baseline, "assess", "bank.payment_returned").await;
    assert_eq!(
        decisions(&charged),
        expected([3500; 6], "fee_per_return v1"),
        "rule v1 charges every return, the retry included"
    );
    balances_chain_from_their_opening_balances(&baseline).await;

    replay(&v2, &journal_root, &baseline).await;
    let candidate = runs(&journal_root).pop().expect("the replay's archive");
    assert_ne!(candidate, baseline);
    let corrected = facts(&candidate, "assess", "bank.payment_returned").await;
    assert_eq!(
        decisions(&corrected),
        expected([3500, 3500, 3500, 0, 0, 0], "one_fee_per_payment v2"),
        "rule v2 charges one fee per unpaid payment"
    );
    assert_eq!(
        decisions(&printed_returns(&candidate).await),
        decisions(&corrected),
        "the replay's fee report prints every corrected return"
    );
    balances_chain_from_their_opening_balances(&candidate).await;

    let changes = read_config_changes(&candidate).expect("the replay's manifest reads");
    assert_eq!(
        changes
            .iter()
            .map(|compared| compared.changes.keys())
            .collect::<Vec<_>>(),
        [vec!["app.fee_rule"]]
    );
    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification runs");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("verification completes: {outcome:?}");
    };
    assert_eq!(outcome.exit_code(), 1, "the corrected fees diverge");
    let diverged: BTreeSet<&str> = report
        .stages
        .iter()
        .filter(|(_, stage)| stage.divergence_count > 0)
        .map(|(stage, _)| stage.as_str())
        .collect();
    assert_eq!(diverged, BTreeSet::from(["assess", "checkbook"]));
}
