// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../examples/aml_overnight_monitoring/support.rs"]
pub mod support;

use obzenflow::application::{verify_run_dirs, FlowApplication, LogLevel, VerifyOptions};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use support::vendors::VendorStats;

const PLANTED: [(u64, &[&str]); 13] = [
    (2800016, &["first_payment_large_cash"]),
    (2800018, &["first_payment_large_cash"]),
    (2800028, &[]),
    (2800037, &["format_switch"]),
    (2800079, &[]),
    (2800096, &["fan_out_large_payment"]),
    (2800105, &[]),
    (2800113, &["thin_history_large_transfer"]),
    (2800117, &["format_switch", "thin_history_large_transfer"]),
    (2800182, &["thin_history_large_transfer"]),
    (2800215, &[]),
    (2800216, &[]),
    (2800224, &["format_switch", "fan_out_large_payment"]),
];

fn example_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/aml_overnight_monitoring")
}

fn fixture_config(dir: &Path) -> PathBuf {
    let example = example_dir();
    let mut config: toml::Table = std::fs::read_to_string(example.join("obzenflow.toml"))
        .expect("shipped configuration is readable")
        .parse()
        .expect("shipped configuration parses");
    let flow = &config["runtime"]["backpressure"]["flow"];
    assert_eq!(flow["mode"].as_str(), Some("enforce"));
    assert!(flow["window"].as_integer().expect("window") < 240);

    let app = config
        .get_mut("app")
        .and_then(toml::Value::as_table_mut)
        .expect("[app] table");
    for (key, path) in [
        (
            "account_history",
            example.join("fixtures/account-history.csv"),
        ),
        ("transactions", example.join("fixtures/transactions.csv")),
        ("risk_ledger", dir.join("risk-calls.jsonl")),
        ("case_ledger", dir.join("case-calls.jsonl")),
        ("cleared_register", dir.join("cleared.csv")),
        ("case_register", dir.join("cases.csv")),
    ] {
        app.insert(
            key.to_owned(),
            toml::Value::String(path.display().to_string()),
        );
    }
    let path = dir.join("obzenflow.toml");
    std::fs::write(
        &path,
        toml::to_string(&config).expect("configuration serializes"),
    )
    .expect("configuration is written");
    path
}

fn run(config: &Path, journal_root: &Path, replay_from: Option<&Path>, stats: Arc<VendorStats>) {
    let mut args = vec![
        OsString::from("aml_overnight_monitoring"),
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
        .run_blocking(support::flow::assemble_flow(
            journal_root.to_path_buf(),
            stats,
        ))
        .expect("the flow completes");
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

fn facts(run: &Path, scratch: &Path) -> BTreeMap<String, Vec<serde_json::Value>> {
    let export = scratch.join("export.jsonl");
    obzenflow_infra::journal::disk::inspect::export_jsonl(run, Some(&export))
        .expect("journal exports");
    let mut facts: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();
    for line in std::fs::read_to_string(&export)
        .expect("export is readable")
        .lines()
    {
        let row: serde_json::Value = serde_json::from_str(line).expect("export row is JSON");
        let event = &row["envelope"]["provenance"]["event"];
        if event["event_kind"] == "fact" {
            if let Some(event_type) = event["event_type"].as_str() {
                facts
                    .entry(event_type.to_owned())
                    .or_default()
                    .push(row["payload"].clone());
            }
        }
    }
    facts
}

fn of<'a>(
    facts: &'a BTreeMap<String, Vec<serde_json::Value>>,
    event_type: &str,
) -> &'a [serde_json::Value] {
    facts.get(event_type).map(Vec::as_slice).unwrap_or_default()
}

fn txn_id(fact: &serde_json::Value) -> u64 {
    fact["txn_id"].as_u64().expect("txn_id")
}

fn reasons(fact: &serde_json::Value) -> Vec<&str> {
    fact["reasons"]
        .as_array()
        .expect("reasons")
        .iter()
        .map(|reason| reason.as_str().expect("reason code"))
        .collect()
}

fn line_count(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .expect("ledger is readable")
        .lines()
        .count()
}

fn fixture_txn_ids() -> BTreeSet<u64> {
    std::fs::read_to_string(example_dir().join("fixtures/transactions.csv"))
        .expect("fixture is readable")
        .lines()
        .skip(1)
        .map(|line| {
            line.split(',')
                .next()
                .and_then(|id| id.parse().ok())
                .expect("txn_id column")
        })
        .collect()
}

#[test]
fn the_fixture_reaches_every_planted_verdict_and_strict_replay_calls_neither_system() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = fixture_config(dir.path());
    let journal_root = dir.path().join("journals");

    let live = Arc::new(VendorStats::default());
    run(&config, &journal_root, None, live.clone());
    assert_eq!(live.engines_built(), 1);
    assert_eq!(live.case_systems_built(), 1);

    let baseline = runs(&journal_root).pop().expect("the live run's archive");
    let facts = facts(&baseline, dir.path());
    let cleared = of(&facts, "aml.transaction_cleared");
    let alerts = of(&facts, "aml.alert_raised");
    let cases = of(&facts, "aml.case_opened");
    assert!(of(&facts, "aml.score_unavailable").is_empty());
    assert!(of(&facts, "aml.case_unavailable").is_empty());

    let verdicts: Vec<u64> = cleared.iter().chain(alerts).map(txn_id).collect();
    let decided: BTreeSet<u64> = verdicts.iter().copied().collect();
    assert_eq!(verdicts.len(), decided.len(), "each transaction ends once");
    assert_eq!(decided, fixture_txn_ids());

    let flagged: BTreeMap<u64, Vec<&str>> = alerts
        .iter()
        .map(|alert| (txn_id(alert), reasons(alert)))
        .collect();
    for (txn, expected) in PLANTED {
        assert!(decided.contains(&txn), "planted {txn} has a verdict");
        assert_eq!(
            flagged.get(&txn).cloned().unwrap_or_default(),
            expected.to_vec(),
            "planted {txn}"
        );
    }
    let planted_flags: BTreeSet<u64> = PLANTED
        .iter()
        .filter(|(_, reasons)| !reasons.is_empty())
        .map(|(txn, _)| *txn)
        .collect();
    assert_eq!(
        flagged.keys().copied().collect::<BTreeSet<_>>(),
        planted_flags
    );

    for verdict in cleared.iter().chain(alerts).chain(cases) {
        assert_eq!(verdict["rules_version"], "sim-2026.10");
    }

    let mut opened: Vec<(u64, u64)> = cases
        .iter()
        .map(|case| {
            (
                txn_id(case),
                case["case_number"].as_u64().expect("case number"),
            )
        })
        .collect();
    opened.sort();
    assert_eq!(
        opened,
        planted_flags
            .iter()
            .zip(1..)
            .map(|(txn, number)| (*txn, number))
            .collect::<Vec<_>>()
    );
    for case in cases {
        assert_eq!(reasons(case), flagged[&txn_id(case)]);
    }

    let risk_calls = line_count(&dir.path().join("risk-calls.jsonl"));
    let case_calls = line_count(&dir.path().join("case-calls.jsonl"));
    assert_eq!(risk_calls, 240);
    assert_eq!(case_calls, planted_flags.len());

    let replay = Arc::new(VendorStats::default());
    run(&config, &journal_root, Some(&baseline), replay.clone());
    assert_eq!(
        replay.engines_built(),
        0,
        "strict replay builds no risk engine"
    );
    assert_eq!(
        replay.case_systems_built(),
        0,
        "strict replay builds no case system"
    );
    assert_eq!(line_count(&dir.path().join("risk-calls.jsonl")), risk_calls);
    assert_eq!(line_count(&dir.path().join("case-calls.jsonl")), case_calls);

    let candidate = runs(&journal_root).pop().expect("the replay's archive");
    assert_ne!(candidate, baseline);
    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification runs");
    assert_eq!(outcome.exit_code(), 0);
}
