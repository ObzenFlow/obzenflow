// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../examples/fx_reporting_view/support.rs"]
pub mod support;

use obzenflow::application::{
    verify_run_dirs, FlowApplication, LogLevel, VerifyOptions, VerifyOutcome,
};
use obzenflow::journal::read::{open_disk_run, ChainPayload, RunJournalKind, RunRecordData};
use obzenflow::journal::JournalRecord;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

const REPORTING: [u64; 5] = [3100002, 3100007, 3100010, 3100015, 3100021];
const BEFORE_FIRST_RATE: u64 = 3100004;
const NOT_QUOTED: u64 = 3100016;
const BOUNDARY: [(u64, &str); 3] = [
    (3100011, "2022-09-05T13:00:00"),
    (3100012, "2022-09-05T14:00:00"),
    (3100013, "2022-09-05T14:00:00"),
];
const FIRST_YUAN_RATE: (u64, &str) = (3100005, "2022-09-05T06:00:00");

fn example_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/fx_reporting_view")
}

fn fixture(name: &str) -> PathBuf {
    example_dir().join("fixtures").join(name)
}

fn csv_rows(path: &Path) -> Vec<BTreeMap<String, String>> {
    let text = std::fs::read_to_string(path).expect("fixture is readable");
    let mut lines = text.lines();
    let headers: Vec<&str> = lines.next().expect("header").split(',').collect();
    lines
        .map(|line| {
            headers
                .iter()
                .zip(line.split(','))
                .map(|(header, value)| ((*header).to_owned(), value.to_owned()))
                .collect()
        })
        .collect()
}

fn config(dir: &Path, rates: &Path) -> PathBuf {
    let path = dir.join("obzenflow.toml");
    let mut app = toml::Table::new();
    for (key, value) in [
        ("rates", rates.to_path_buf()),
        ("transactions", fixture("transactions.csv")),
        ("report", dir.join("report.csv")),
    ] {
        app.insert(
            key.to_owned(),
            toml::Value::String(value.display().to_string()),
        );
    }
    let mut config = toml::Table::new();
    config.insert("app".to_owned(), toml::Value::Table(app));
    std::fs::write(
        &path,
        toml::to_string(&config).expect("configuration serializes"),
    )
    .expect("configuration is written");
    path
}

fn run(config: &Path, journal_root: &Path, replay_from: Option<&Path>) -> PathBuf {
    let before = runs(journal_root);
    let mut args = vec![
        OsString::from("fx_reporting_view"),
        OsString::from("--config"),
        config.as_os_str().to_os_string(),
    ];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    FlowApplication::builder()
        .with_log_level(LogLevel::Error)
        .with_cli_args(args)
        .run_blocking(support::flow::assemble_flow(journal_root.to_path_buf()))
        .expect("the flow completes");
    runs(journal_root)
        .into_iter()
        .find(|run| !before.contains(run))
        .expect("the run's archive")
}

fn runs(journal_root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(journal_root.join("flows")) else {
        return Vec::new();
    };
    entries
        .map(|entry| entry.expect("run entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect()
}

fn price_facts(run: &Path) -> Vec<(String, serde_json::Value)> {
    tokio::runtime::Runtime::new()
        .expect("a reader runtime")
        .block_on(async {
            let mut snapshot = open_disk_run(run).await.expect("the archive opens");
            let mut facts = Vec::new();
            while let Some(record) = snapshot.next().await.expect("the archive reads") {
                if record
                    .journal
                    .stage
                    .as_ref()
                    .map(|stage| stage.key.as_str())
                    != Some("price")
                    || record.journal.kind != RunJournalKind::Data
                {
                    continue;
                }
                if let RunRecordData::Chain(row) = record.record {
                    if matches!(row.payload, ChainPayload::Fact(_)) {
                        facts.push((row.event_type().to_owned(), JournalRecord::payload(&row)));
                    }
                }
            }
            facts
        })
}

fn txn(fact: &serde_json::Value) -> u64 {
    fact["txn_id"].as_u64().expect("txn_id")
}

fn rate_in_force(rates: &[BTreeMap<String, String>], currency: &str, booked_at: &str) -> String {
    rates
        .iter()
        .filter(|rate| rate["currency"] == currency && rate["effective_at"].as_str() <= booked_at)
        .map(|rate| rate["effective_at"].clone())
        .max()
        .expect("a rate in force")
}

fn verify(baseline: &Path, candidate: &Path) -> BTreeMap<String, u64> {
    let outcome =
        verify_run_dirs(baseline, candidate, &VerifyOptions::default()).expect("verification runs");
    let VerifyOutcome::Completed { report, .. } = outcome else {
        panic!("verification completes")
    };
    report
        .stages
        .iter()
        .map(|(stage, stage_report)| (stage.clone(), stage_report.divergence_count))
        .collect()
}

#[test]
fn the_fixture_prices_each_payment_once_at_the_rate_in_force_when_it_was_booked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_root = dir.path().join("journals");
    let config = config(dir.path(), &fixture("simulated-treasury-rates.csv"));
    let baseline = run(&config, &journal_root, None);
    let facts = price_facts(&baseline);

    let transactions = csv_rows(&fixture("transactions.csv"));
    let rates = csv_rows(&fixture("simulated-treasury-rates.csv"));
    let booked: BTreeMap<u64, &BTreeMap<String, String>> = transactions
        .iter()
        .map(|row| (row["txn_id"].parse().expect("txn_id"), row))
        .collect();

    let outputs: Vec<u64> = facts.iter().map(|(_, fact)| txn(fact)).collect();
    assert_eq!(outputs.len(), booked.len(), "one output per payment");
    assert_eq!(
        outputs.iter().copied().collect::<BTreeSet<_>>(),
        booked.keys().copied().collect(),
        "every payment is priced or named once"
    );

    let by_type = |event_type: &str| -> Vec<&serde_json::Value> {
        facts
            .iter()
            .filter(|(kind, _)| kind == event_type)
            .map(|(_, fact)| fact)
            .collect()
    };
    let in_reporting: Vec<u64> = by_type("fx_reporting.in_reporting_currency")
        .into_iter()
        .map(txn)
        .collect();
    assert_eq!(in_reporting, REPORTING);
    let missing: Vec<u64> = by_type("fx_reporting.rate_missing")
        .into_iter()
        .map(txn)
        .collect();
    assert_eq!(missing, [BEFORE_FIRST_RATE]);
    let not_quoted: Vec<u64> = by_type("fx_reporting.currency_not_quoted")
        .into_iter()
        .map(txn)
        .collect();
    assert_eq!(not_quoted, [NOT_QUOTED]);

    let priced = by_type("fx_reporting.transaction_priced");
    assert_eq!(priced.len(), booked.len() - REPORTING.len() - 2);
    let effective: BTreeMap<u64, String> = priced
        .iter()
        .map(|fact| {
            let row = booked[&txn(fact)];
            let booked_at =
                chrono::NaiveDateTime::parse_from_str(&row["timestamp"], "%Y/%m/%d %H:%M")
                    .expect("booked time")
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string();
            let effective_at = fact["rate"]["effective_at"]
                .as_str()
                .expect("effective time");
            assert_eq!(
                effective_at,
                rate_in_force(&rates, &row["payment_currency"], &booked_at),
                "{} takes the latest rate at or before its booking time",
                txn(fact)
            );
            (txn(fact), effective_at.to_owned())
        })
        .collect();
    for (txn_id, effective_at) in BOUNDARY.into_iter().chain([FIRST_YUAN_RATE]) {
        assert_eq!(effective[&txn_id], effective_at, "{txn_id}");
    }

    for (_, fact) in &facts {
        let row = booked[&txn(fact)];
        assert_eq!(
            fact["amount_paid"].as_f64(),
            row["amount_paid"].parse::<f64>().ok(),
            "{} keeps its booked amount",
            txn(fact)
        );
    }

    let replay = run(&config, &journal_root, Some(&baseline));
    assert!(
        verify(&baseline, &replay).values().all(|count| *count == 0),
        "strict replay reproduces every price"
    );
}

#[test]
fn a_corrected_rate_in_a_fresh_run_diverges_only_where_it_priced() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_root = dir.path().join("journals");
    let baseline = run(
        &config(dir.path(), &fixture("simulated-treasury-rates.csv")),
        &journal_root,
        None,
    );

    let corrected = dir.path().join("corrected-rates.csv");
    let original = std::fs::read_to_string(fixture("simulated-treasury-rates.csv"))
        .expect("rates are readable");
    assert!(original.contains("Euro,2022-09-05T14:00:00,0.9946\n"));
    std::fs::write(
        &corrected,
        original.replace(
            "Euro,2022-09-05T14:00:00,0.9946\n",
            "Euro,2022-09-05T14:00:00,0.9960\n",
        ),
    )
    .expect("corrected rates are written");
    let fresh = run(&config(dir.path(), &corrected), &journal_root, None);

    let divergences = verify(&baseline, &fresh);
    let diverged: BTreeMap<&str, u64> = divergences
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(stage, count)| (stage.as_str(), *count))
        .collect();
    assert_eq!(
        diverged,
        BTreeMap::from([("fx_rates", 1), ("price", 2)]),
        "{divergences:?}"
    );
}
