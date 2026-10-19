// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use chrono::{NaiveDate, NaiveDateTime};
use obzenflow::application::{FlowApplication, LogLevel};
use obzenflow::flow::{flow, join, sink, source, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::journal::read::{
    open_disk_run, ChainPayload, ExecutionPayload, RunJournalKind, RunRecordData,
    StageLifecycleFact,
};
use obzenflow::schema::TypedPayload;
use obzenflow::stages::joins::{self, AsOf};
use obzenflow::stages::sinks::DiscardSink;
use obzenflow::stages::sources::ValuesSource;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FxRate {
    currency: String,
    effective_at: NaiveDateTime,
    rate: f64,
}

impl TypedPayload for FxRate {
    const EVENT_TYPE: &'static str = "as_of_join_test.fx_rate";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Payment {
    txn_id: u64,
    currency: String,
    booked_at: NaiveDateTime,
}

impl TypedPayload for Payment {
    const EVENT_TYPE: &'static str = "as_of_join_test.payment";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Priced {
    txn_id: u64,
    rate: Option<f64>,
}

impl TypedPayload for Priced {
    const EVENT_TYPE: &'static str = "as_of_join_test.priced";
}

fn at(hour: u32, minute: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2022, 9, 5)
        .and_then(|date| date.and_hms_opt(hour, minute, 0))
        .expect("a valid time")
}

fn rate(currency: &str, effective_at: NaiveDateTime, rate: f64) -> FxRate {
    FxRate {
        currency: currency.to_string(),
        effective_at,
        rate,
    }
}

fn priced_flow(journal_root: PathBuf, rates: Vec<FxRate>) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let rates = ValuesSource::new(rates.clone());
        let payments = ValuesSource::new(vec![Payment {
            txn_id: 1,
            currency: "EUR".to_string(),
            booked_at: at(14, 30),
        }]);
        let price = joins::left_as_of(
            |rate: &FxRate| rate.currency.clone(),
            |rate: &FxRate| rate.effective_at,
            |payment: &Payment| payment.currency.clone(),
            |payment: &Payment| payment.booked_at,
            |rate: AsOf<FxRate>, payment: Payment| Priced {
                txn_id: payment.txn_id,
                rate: match rate {
                    AsOf::Found(rate) => Some(rate.rate),
                    AsOf::BeforeFirstVersion | AsOf::NoVersions => None,
                },
            },
        );
        let report = DiscardSink::<Priced>::new();
        Ok(flow! {
            name: "as_of_join_test",
            journals: disk_journals(journal_root.clone()),

            stages: {
                fx_rates = source!(FxRate => rates);
                payments = source!(Payment => payments);
                price = join!(catalog fx_rates: FxRate, Payment -> Priced => price);
                report = sink!(Priced => report);
            },

            topology: {
                (fx_rates, payments) |> price;
                price |> report;
            }
        })
    })
}

fn run(journal_root: &Path, rates: Vec<FxRate>) -> bool {
    FlowApplication::builder()
        .with_log_level(LogLevel::Error)
        .with_cli_args(vec![OsString::from("as_of_join_test")])
        .run_blocking(priced_flow(journal_root.to_path_buf(), rates))
        .is_ok()
}

struct PriceJournal {
    facts: Vec<serde_json::Value>,
    left_hydration: bool,
    failures: Vec<String>,
    fatal_details: Vec<String>,
}

fn price_journal(journal_root: &Path) -> PriceJournal {
    let run = std::fs::read_dir(journal_root.join("flows"))
        .expect("flows directory")
        .map(|entry| entry.expect("run entry").path())
        .find(|path| path.join("run_manifest.json").exists())
        .expect("one run");
    tokio::runtime::Runtime::new()
        .expect("a reader runtime")
        .block_on(async {
            let mut snapshot = open_disk_run(&run).await.expect("the archive opens");
            let mut journal = PriceJournal {
                facts: Vec::new(),
                left_hydration: false,
                failures: Vec::new(),
                fatal_details: Vec::new(),
            };
            while let Some(record) = snapshot.next().await.expect("the archive reads") {
                if record
                    .journal
                    .stage
                    .as_ref()
                    .map(|stage| stage.key.as_str())
                    != Some("price")
                {
                    continue;
                }
                let RunRecordData::Chain(row) = record.record else {
                    continue;
                };
                match (&record.journal.kind, &row.payload) {
                    (RunJournalKind::Data, ChainPayload::Fact(fact)) => {
                        journal.facts.push(fact.clone());
                    }
                    (
                        RunJournalKind::Data,
                        ChainPayload::Execution(ExecutionPayload::JoinPhase(_)),
                    ) => {
                        journal.left_hydration = true;
                    }
                    (
                        RunJournalKind::Data,
                        ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                            StageLifecycleFact::Failed { error, .. },
                        )),
                    ) => journal.failures.push(error.clone()),
                    (
                        RunJournalKind::Error,
                        ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal)),
                    ) => journal.fatal_details.push(fatal.detail.clone()),
                    _ => {}
                }
            }
            journal
        })
}

#[test]
fn a_second_version_at_one_key_and_time_fails_the_join_before_it_reads_a_payment() {
    let dir = tempfile::tempdir().expect("tempdir");
    let completed = run(
        dir.path(),
        vec![
            rate("EUR", at(0, 0), 1.0012),
            rate("EUR", at(14, 0), 1.0031),
            rate("GBP", at(0, 0), 1.1520),
            rate("EUR", at(14, 0), 1.0040),
        ],
    );
    assert!(
        !completed,
        "the join refuses its catalogue and the run fails"
    );

    let diagnostic = "join `price` refused its catalogue `fx_rates` while hydrating: two \
                      versions of \"EUR\" take effect at 2022-09-05T14:00:00, so a row as of \
                      that time has no single version. Keep one version per key and effective \
                      time, then start a fresh run.";
    let journal = price_journal(dir.path());
    assert_eq!(
        journal.failures,
        [diagnostic],
        "the join's failure carries the diagnostic"
    );
    assert_eq!(
        journal.fatal_details,
        [diagnostic],
        "the error journal records it"
    );
    assert!(
        journal.facts.is_empty(),
        "no payment is priced: {:?}",
        journal.facts
    );
    assert!(!journal.left_hydration, "the join never leaves hydration");
}

#[test]
fn a_catalogue_with_one_version_per_key_and_time_prices_the_payment() {
    let dir = tempfile::tempdir().expect("tempdir");
    let completed = run(
        dir.path(),
        vec![
            rate("EUR", at(0, 0), 1.0012),
            rate("EUR", at(14, 0), 1.0031),
            rate("EUR", at(15, 0), 1.0040),
        ],
    );
    assert!(completed, "the run completes");

    let journal = price_journal(dir.path());
    assert_eq!(
        journal.facts,
        [serde_json::json!({ "txn_id": 1, "rate": 1.0031 })],
        "the 14:30 payment takes the 14:00 rate"
    );
    assert!(journal.failures.is_empty());
}
