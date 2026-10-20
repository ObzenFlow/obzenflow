// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../examples/aml_overnight_monitoring/support.rs"]
pub mod support;

use chrono::NaiveDateTime;
use obzenflow::application::{
    verify_run_dirs, FlowApplication, LogLevel, VerifyOptions, VerifyOutcome,
};
use obzenflow::journal::read::{
    open_disk_run, ChainPayload, ExecutionPayload, FlowControlPayload, RunJournalKind,
    RunRecordData,
};
use obzenflow::journal::JournalRecord;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::{StageId, WriterId};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use support::domain::AMLWORLD_TIME;
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

const WHOLE_DAY: [(&str, &[&str]); 4] = [
    ("day:2022/09/05:12/80890E60D", &["fan_in"]),
    ("day:2022/09/05:12/80B74F97B", &["fan_in"]),
    ("day:2022/09/05:48/81F0A0001", &["fan_out"]),
    ("day:2022/09/05:48/81F0A0003", &["fan_out", "fan_in"]),
];

const ENGINE_RULES: &str = "sim-2026.10";
const WHOLE_DAY_RULES: &str = "aml-daily-profile-v1";

const REPORTING_CURRENCY: &str = "US Dollar";
const BEFORE_THE_FIRST_YUAN_RATE: [u64; 3] = [2800007, 2800020, 2800045];
const BOOKED_AS_A_RATE_TAKES_EFFECT: (u64, &str) = (2800011, "2022-09-05T01:00:00");
const CORRECTED_RATE: (&str, &str) = (
    "Euro,2022-09-05T13:00:00,0.9916\n",
    "Euro,2022-09-05T13:00:00,0.9930\n",
);

fn example_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/aml_overnight_monitoring")
}

fn shipped_rates() -> PathBuf {
    example_dir().join("fixtures/simulated-treasury-rates.csv")
}

struct Rate {
    currency: String,
    effective_at: NaiveDateTime,
    usd_per_unit: String,
}

fn rates() -> Vec<Rate> {
    std::fs::read_to_string(shipped_rates())
        .expect("rates are readable")
        .lines()
        .skip(1)
        .map(|line| {
            let row: Vec<&str> = line.split(',').collect();
            Rate {
                currency: row[0].to_owned(),
                effective_at: row[1].parse().expect("effective_at column"),
                usd_per_unit: row[2].to_owned(),
            }
        })
        .collect()
}

fn decimal(text: &str) -> (u128, u32) {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let places = u32::try_from(fraction.len()).expect("few decimal places");
    let units = format!("{whole}{fraction}")
        .parse()
        .expect("a plain decimal");
    (units, places)
}

fn decimal_value(text: &str) -> (u128, u32) {
    let (units, places) = decimal(text);
    (units * 10u128.pow(8 - places), 8)
}

fn booked_cents(amount: &str) -> u128 {
    let (units, places) = decimal(amount);
    units * 10u128.pow(2 - places)
}

fn cents_half_up(amount: &str, rate: &str) -> u128 {
    let (amount, amount_places) = decimal(amount);
    let (rate, rate_places) = decimal(rate);
    let divisor = 10u128.pow(amount_places + rate_places - 2);
    let product = amount * rate;
    product / divisor + u128::from(product % divisor * 2 >= divisor)
}

fn usd(cents: u128) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

fn rate_in_force<'a>(rates: &'a [Rate], currency: &str, booked: NaiveDateTime) -> Option<&'a Rate> {
    rates
        .iter()
        .filter(|rate| rate.currency == currency && rate.effective_at <= booked)
        .max_by_key(|rate| rate.effective_at)
}

fn booked_at(row: &[String]) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(&row[1], AMLWORLD_TIME).expect("timestamp column")
}

fn iso(time: NaiveDateTime) -> String {
    time.format("%Y-%m-%dT%H:%M:%S").to_string()
}

fn fixture_rows() -> Vec<Vec<String>> {
    std::fs::read_to_string(example_dir().join("fixtures/transactions.csv"))
        .expect("fixture is readable")
        .lines()
        .skip(1)
        .map(|line| line.split(',').map(str::to_owned).collect())
        .collect()
}

fn fixture_txn_ids() -> BTreeSet<u64> {
    fixture_rows()
        .iter()
        .map(|row| row[0].parse().expect("txn_id column"))
        .collect()
}

fn fixture_config(dir: &Path, fx_rates: &Path) -> PathBuf {
    let example = example_dir();
    let mut config: toml::Table = std::fs::read_to_string(example.join("obzenflow.toml"))
        .expect("shipped configuration is readable")
        .parse()
        .expect("shipped configuration parses");
    let backpressure = &config["runtime"]["backpressure"];
    assert_eq!(backpressure["flow"]["mode"].as_str(), Some("enforce"));
    let window = backpressure["flow"]["window"].as_integer().expect("window");
    assert!(
        usize::try_from(window).expect("a positive window") < fixture_rows().len(),
        "the window holds the transactions source mid-file"
    );
    assert_eq!(
        backpressure["stages"]["daily_profile"]["edges"]["alerts"]["mode"].as_str(),
        Some("track")
    );

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
        ("fx_rates", fx_rates.to_path_buf()),
        ("risk_ledger", dir.join("risk-calls.jsonl")),
        ("case_ledger", dir.join("case-calls.jsonl")),
        ("cleared_register", dir.join("cleared.csv")),
        ("unscored_register", dir.join("unscored.csv")),
        ("case_register", dir.join("cases.csv")),
        ("unopened_register", dir.join("unopened-alerts.csv")),
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

struct Archive {
    rows: Vec<(String, JournalRecord<ChainPayload>)>,
    stage_ids: BTreeMap<String, StageId>,
}

impl Archive {
    fn read(run: &Path) -> Self {
        tokio::runtime::Runtime::new()
            .expect("a reader runtime")
            .block_on(async {
                let mut snapshot = open_disk_run(run).await.expect("the archive opens");
                let mut archive = Self {
                    rows: Vec::new(),
                    stage_ids: BTreeMap::new(),
                };
                while let Some(record) = snapshot.next().await.expect("the archive reads") {
                    let Some(stage) = record.journal.stage else {
                        continue;
                    };
                    archive.stage_ids.insert(stage.key.clone(), stage.id);
                    if let (RunJournalKind::Data, RunRecordData::Chain(row)) =
                        (record.journal.kind, record.record)
                    {
                        archive.rows.push((stage.key, *row));
                    }
                }
                archive
            })
    }

    fn rows<'a>(
        &'a self,
        stage: &'a str,
    ) -> impl Iterator<Item = &'a JournalRecord<ChainPayload>> + 'a {
        self.rows
            .iter()
            .filter(move |(key, _)| key == stage)
            .map(|(_, row)| row)
    }

    fn facts(&self, stage: &str, event_type: &str) -> Vec<serde_json::Value> {
        self.rows(stage)
            .filter(|row| matches!(row.payload, ChainPayload::Fact(_)))
            .filter(|row| row.event_type() == event_type)
            .map(JournalRecord::payload)
            .collect()
    }

    fn all_facts(&self, event_type: &str) -> usize {
        self.rows
            .iter()
            .filter(|(_, row)| {
                matches!(row.payload, ChainPayload::Fact(_)) && row.event_type() == event_type
            })
            .count()
    }

    fn natural_eof_from(&self, stage: &str, author: &str) -> usize {
        let author = self.stage_ids[author];
        self.rows(stage)
            .position(|row| {
                matches!(&row.payload, ChainPayload::FlowControl(FlowControlPayload::Eof {
                    kind: EofKind::Natural,
                    writer_id: Some(WriterId::Stage(writer)),
                    ..
                }) if *writer == author)
            })
            .unwrap_or_else(|| panic!("{stage} passes on {author}'s natural EOF"))
    }

    fn passing_verdicts(&self) -> BTreeMap<String, usize> {
        let names: BTreeMap<StageId, &str> = self
            .stage_ids
            .iter()
            .map(|(name, id)| (*id, name.as_str()))
            .collect();
        let mut verdicts = BTreeMap::new();
        for (_, row) in &self.rows {
            if let ChainPayload::Execution(ExecutionPayload::ContractStatus {
                upstream,
                reader,
                pass: true,
                ..
            }) = &row.payload
            {
                *verdicts
                    .entry(format!("{} -> {}", names[upstream], names[reader]))
                    .or_insert(0) += 1;
            }
        }
        verdicts
    }
}

fn txn_id(verdict: &serde_json::Value) -> u64 {
    verdict["txn_id"]
        .as_u64()
        .or_else(|| verdict["subject"]["transaction"].as_u64())
        .expect("a transaction verdict")
}

fn alert_id(fact: &serde_json::Value) -> &str {
    fact["alert_id"].as_str().expect("alert_id")
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

fn divergences(baseline: &Path, candidate: &Path) -> BTreeMap<String, u64> {
    let outcome =
        verify_run_dirs(baseline, candidate, &VerifyOptions::default()).expect("verification runs");
    let VerifyOutcome::Completed { report, .. } = outcome else {
        panic!("verification completes")
    };
    report
        .stages
        .iter()
        .map(|(stage, stage_report)| (stage.clone(), stage_report.divergence_count))
        .filter(|(_, count)| *count > 0)
        .collect()
}

#[test]
fn the_fixture_reaches_every_planted_verdict_and_strict_replay_calls_neither_system() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = fixture_config(dir.path(), &shipped_rates());
    let journal_root = dir.path().join("journals");

    let live = Arc::new(VendorStats::default());
    run(&config, &journal_root, None, live.clone());
    assert_eq!(live.engines_built(), 1);
    assert_eq!(live.case_systems_built(), 1);

    let baseline = runs(&journal_root).pop().expect("the live run's archive");
    let archive = Archive::read(&baseline);
    assert_eq!(archive.all_facts("aml.score_unavailable"), 0);
    assert_eq!(archive.all_facts("aml.case_unavailable"), 0);

    let cleared = archive.facts("score", "aml.transaction_cleared");
    let flagged = archive.facts("score", "aml.alert_raised");
    let verdicts: Vec<u64> = cleared.iter().chain(&flagged).map(txn_id).collect();
    let decided: BTreeSet<u64> = verdicts.iter().copied().collect();
    assert_eq!(verdicts.len(), decided.len(), "each transaction ends once");
    assert_eq!(decided, fixture_txn_ids());

    let flags: BTreeMap<u64, Vec<&str>> = flagged
        .iter()
        .map(|alert| (txn_id(alert), reasons(alert)))
        .collect();
    for (txn, expected) in PLANTED {
        assert!(decided.contains(&txn), "planted {txn} has a verdict");
        assert_eq!(
            flags.get(&txn).cloned().unwrap_or_default(),
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
        flags.keys().copied().collect::<BTreeSet<_>>(),
        planted_flags
    );
    for verdict in cleared.iter().chain(&flagged) {
        assert_eq!(verdict["rules_version"], ENGINE_RULES);
    }
    for alert in &flagged {
        assert_eq!(alert_id(alert), format!("txn:{}", txn_id(alert)));
    }

    let whole_day = archive.facts("daily_profile", "aml.alert_raised");
    assert_eq!(
        whole_day
            .iter()
            .map(|alert| (alert_id(alert), reasons(alert)))
            .collect::<Vec<_>>(),
        WHOLE_DAY
            .iter()
            .map(|(id, reasons)| (*id, reasons.to_vec()))
            .collect::<Vec<_>>()
    );
    for alert in &whole_day {
        assert_eq!(alert["subject"]["day"], "2022/09/05");
        assert_eq!(alert["rules_version"], WHOLE_DAY_RULES);
    }

    let totals = archive.facts("day_totals", "aml.day_totals");
    assert_eq!(totals.len(), 1, "the day's totals are journaled once");
    let totals = &totals[0];
    let transactions = archive.facts("transactions", "aml.transaction").len();
    assert_eq!(transactions, fixture_rows().len());
    assert_eq!(totals["transactions"].as_u64(), Some(transactions as u64));
    let mut fields = totals
        .as_object()
        .expect("day totals are an object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    fields.sort();
    assert_eq!(
        fields,
        [
            "by_format",
            "labelled_laundering",
            "paid_usd",
            "transactions",
            "unpriced"
        ]
    );

    let rates = rates();
    let rows = fixture_rows();
    let booked: BTreeMap<u64, &Vec<String>> = rows
        .iter()
        .map(|row| (row[0].parse().expect("txn_id column"), row))
        .collect();
    let priced = archive.facts("price", "aml.transaction_priced");
    assert_eq!(
        priced.len(),
        transactions,
        "one priced fact per transaction"
    );
    let mut paid_cents = 0;
    let mut unpriced = Vec::new();
    let mut effective = BTreeMap::new();
    for fact in &priced {
        let transaction = &fact["transaction"];
        let txn = transaction["txn_id"].as_u64().expect("txn_id");
        let row = booked[&txn];
        let paid = &transaction["paid"];
        assert_eq!(paid["currency"], row[9].as_str());
        assert_eq!(
            decimal_value(paid["amount"].as_str().expect("an exact amount")),
            decimal_value(&row[8]),
            "{txn} keeps its booked amount"
        );
        assert_eq!(transaction["timestamp"], row[1].as_str());
        let value = &fact["value"];
        if row[9] == REPORTING_CURRENCY {
            let cents = booked_cents(&row[8]);
            assert_eq!(value["as_booked"], usd(cents).as_str(), "{txn}");
            paid_cents += cents;
            continue;
        }
        match rate_in_force(&rates, &row[9], booked_at(row)) {
            Some(rate) => {
                let converted = &value["converted"];
                assert_eq!(
                    converted["rate"]["effective_at"],
                    iso(rate.effective_at).as_str(),
                    "{txn} takes the latest rate at or before its booking time"
                );
                assert_eq!(
                    converted["rate"]["usd_per_unit"],
                    rate.usd_per_unit.as_str()
                );
                let cents = cents_half_up(&row[8], &rate.usd_per_unit);
                assert_eq!(
                    converted["usd"],
                    usd(cents).as_str(),
                    "{txn} rounds once, half up, to the cent"
                );
                effective.insert(txn, iso(rate.effective_at));
                paid_cents += cents;
            }
            None => {
                assert_eq!(*value, "unpriced", "{txn} has no rate in force");
                unpriced.push(txn);
            }
        }
    }
    assert_eq!(unpriced, BEFORE_THE_FIRST_YUAN_RATE);
    let (on_the_hour, its_rate) = BOOKED_AS_A_RATE_TAKES_EFFECT;
    assert_eq!(effective[&on_the_hour], its_rate);
    assert_eq!(totals["unpriced"].as_u64(), Some(unpriced.len() as u64));
    assert_eq!(
        totals["paid_usd"],
        usd(paid_cents).as_str(),
        "the day's total is the sum of the rounded payments"
    );
    assert_eq!(
        totals["by_format"]
            .as_object()
            .expect("by_format")
            .values()
            .map(|count| count.as_u64().expect("a count"))
            .sum::<u64>(),
        transactions as u64
    );
    let labelled = fixture_rows()
        .iter()
        .filter(|row| row.last().map(String::as_str) == Some("1"))
        .count();
    assert_eq!(
        totals["labelled_laundering"].as_u64(),
        Some(labelled as u64)
    );

    let boundary = archive.natural_eof_from("alerts", "score");
    let merged: Vec<(usize, String)> = archive
        .rows("alerts")
        .enumerate()
        .filter(|(_, row)| row.event_type() == "aml.alert_raised")
        .map(|(position, row)| (position, alert_id(&row.payload()).to_owned()))
        .collect();
    assert_eq!(merged.len(), planted_flags.len() + WHOLE_DAY.len());
    for (position, id) in &merged {
        if id.starts_with("txn:") {
            assert!(*position < boundary, "{id} precedes score's natural EOF");
        } else {
            assert!(*position > boundary, "{id} follows score's natural EOF");
        }
    }

    let cases = archive.facts("open_case", "aml.case_opened");
    let opened: Vec<(u64, &str)> = cases
        .iter()
        .map(|case| {
            (
                case["case_number"].as_u64().expect("case number"),
                alert_id(case),
            )
        })
        .collect();
    let expected_ids: Vec<String> = planted_flags
        .iter()
        .map(|txn| format!("txn:{txn}"))
        .chain(WHOLE_DAY.iter().map(|(id, _)| (*id).to_owned()))
        .collect();
    assert_eq!(
        opened,
        expected_ids
            .iter()
            .zip(1..)
            .map(|(id, number)| (number, id.as_str()))
            .collect::<Vec<_>>(),
        "per-transaction cases open first, whole-day cases after them"
    );
    let raised: BTreeMap<&str, &serde_json::Value> = flagged
        .iter()
        .chain(&whole_day)
        .map(|alert| (alert_id(alert), alert))
        .collect();
    for case in &cases {
        let alert = raised[alert_id(case)];
        assert_eq!(reasons(case), reasons(alert));
        assert_eq!(case["rules_version"], alert["rules_version"]);
    }

    let risk_calls = line_count(&dir.path().join("risk-calls.jsonl"));
    let case_calls = line_count(&dir.path().join("case-calls.jsonl"));
    assert_eq!(risk_calls, fixture_rows().len());
    assert_eq!(case_calls, expected_ids.len());

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

#[test]
fn a_corrected_rate_in_a_fresh_run_diverges_only_where_it_priced() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_root = dir.path().join("journals");
    let first = dir.path().join("first");
    let corrected = dir.path().join("corrected");
    std::fs::create_dir_all(&first).expect("first run directory");
    std::fs::create_dir_all(&corrected).expect("corrected run directory");

    run(
        &fixture_config(&first, &shipped_rates()),
        &journal_root,
        None,
        Arc::new(VendorStats::default()),
    );
    let baseline = runs(&journal_root).pop().expect("the first run's archive");

    let (original, replacement) = CORRECTED_RATE;
    let shipped = std::fs::read_to_string(shipped_rates()).expect("rates are readable");
    assert!(shipped.contains(original));
    let corrected_rates = corrected.join("rates.csv");
    std::fs::write(&corrected_rates, shipped.replace(original, replacement))
        .expect("corrected rates are written");
    run(
        &fixture_config(&corrected, &corrected_rates),
        &journal_root,
        None,
        Arc::new(VendorStats::default()),
    );
    let fresh = runs(&journal_root)
        .pop()
        .expect("the corrected run's archive");
    assert_ne!(fresh, baseline);

    let rates = rates();
    let priced_from_it = fixture_rows()
        .iter()
        .filter(|row| {
            rate_in_force(&rates, &row[9], booked_at(row)).is_some_and(|rate| {
                original.starts_with(&format!("{},{},", rate.currency, iso(rate.effective_at)))
            })
        })
        .count() as u64;
    assert!(priced_from_it > 0, "the corrected rate prices a payment");
    assert_eq!(
        divergences(&baseline, &fresh),
        BTreeMap::from([
            ("day_totals".to_owned(), 1),
            ("fx_rates".to_owned(), 1),
            ("price".to_owned(), priced_from_it),
        ])
    );
}

#[test]
fn every_edge_records_one_passing_contract_verdict() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = fixture_config(dir.path(), &shipped_rates());
    let journal_root = dir.path().join("journals");
    run(
        &config,
        &journal_root,
        None,
        Arc::new(VendorStats::default()),
    );
    let archive = Archive::read(&runs(&journal_root).pop().expect("the run's archive"));

    let edges = [
        "account_history -> profile",
        "transactions -> profile",
        "fx_rates -> price",
        "transactions -> price",
        "transactions -> daily_profile",
        "price -> day_totals",
        "profile -> score",
        "score -> cleared",
        "score -> unscored",
        "score -> alerts",
        "daily_profile -> alerts",
        "alerts -> open_case",
        "open_case -> case_register",
        "open_case -> unopened_alerts",
        "day_totals -> totals_report",
    ];
    assert_eq!(
        archive.passing_verdicts(),
        edges
            .iter()
            .map(|edge| (edge.to_string(), 1))
            .collect::<BTreeMap<_, _>>()
    );
}
