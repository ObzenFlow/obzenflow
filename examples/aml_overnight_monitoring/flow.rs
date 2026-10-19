// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::console;
use super::domain::{
    AccountHistory, AlertRaised, CaseOpened, CaseUnavailable, DayTotals, FxRate, PricedTransaction,
    ProfiledTransaction, ScoreUnavailable, Transaction, TransactionCleared,
};
use super::stages::{AccountProfile, DailyProfile, OpenCaseHandler, ScoreHandler};
use super::vendors::{
    OpenCase, ScoreTransaction, VendorBindings, VendorBindingsError, VendorStats,
};
use obzenflow::error::HandlerError;
use obzenflow::flow::{
    effectful_transform, flow, join, merge, sink, source, stateful, FlowBuildError, FlowDefinition,
};
use obzenflow::journal::disk_journals;
use obzenflow::stages::joins;
use obzenflow::stages::merges;
use obzenflow::stages::sinks::{ConsoleSink, CsvProjection, CsvSink};
use obzenflow::stages::sources::{CsvDecoder, CsvSource};
use obzenflow::stages::stateful;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const DEMO_JOURNAL_ROOT: &str = "target/aml-overnight-monitoring-logs";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AmlApp {
    pub account_history: PathBuf,
    pub transactions: PathBuf,
    pub fx_rates: PathBuf,
    pub risk_ledger: PathBuf,
    pub case_ledger: PathBuf,
    pub cleared_register: PathBuf,
    pub case_register: PathBuf,
    pub rules: AmlRules,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AmlRules {
    pub fan_out_counterparties: NonZeroUsize,
    pub fan_in_counterparties: NonZeroUsize,
}

#[derive(Clone, Debug)]
struct AccountHistoryCsv;

impl CsvDecoder for AccountHistoryCsv {
    type Output = AccountHistory;
}

#[derive(Clone, Debug)]
struct TransactionCsv;

impl CsvDecoder for TransactionCsv {
    type Output = Transaction;
}

#[derive(Clone, Debug)]
struct RateCsv;

impl CsvDecoder for RateCsv {
    type Output = FxRate;
}

#[derive(Clone, Debug)]
struct ClearedRow;

#[derive(Serialize)]
struct ClearedRecord {
    txn_id: u64,
    rules_version: String,
}

impl CsvProjection for ClearedRow {
    type Input = TransactionCleared;
    type Row = ClearedRecord;

    fn project(&self, cleared: TransactionCleared) -> Result<ClearedRecord, HandlerError> {
        Ok(ClearedRecord {
            txn_id: cleared.txn_id.0,
            rules_version: cleared.rules_version.0,
        })
    }
}

#[derive(Clone, Debug)]
struct CaseRow;

#[derive(Serialize)]
struct CaseRecord {
    case_number: u64,
    alert_id: String,
    bank: String,
    account: String,
    reasons: String,
    rules_version: String,
}

impl CsvProjection for CaseRow {
    type Input = CaseOpened;
    type Row = CaseRecord;

    fn project(&self, case: CaseOpened) -> Result<CaseRecord, HandlerError> {
        Ok(CaseRecord {
            case_number: case.case_number.0,
            alert_id: case.alert_id.0,
            reasons: case
                .reasons
                .iter()
                .map(|reason| reason.0.as_str())
                .collect::<Vec<_>>()
                .join(";"),
            bank: case.account.bank,
            account: case.account.account,
            rules_version: case.rules_version.0,
        })
    }
}

fn resources_failed(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(error.to_string())
}

fn bindings_failed(error: VendorBindingsError) -> FlowBuildError {
    FlowBuildError::BindingConfiguration {
        binding: "vendors".to_string(),
        detail: error.to_string(),
    }
}

fn register_dir(register: &Path) -> Result<(), String> {
    match register
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        Some(parent) => std::fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display())),
        None => Ok(()),
    }
}

pub fn build_flow() -> FlowDefinition {
    assemble_flow(
        PathBuf::from(DEMO_JOURNAL_ROOT),
        Arc::new(VendorStats::default()),
    )
}

pub fn assemble_flow(journal_root: PathBuf, stats: Arc<VendorStats>) -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, app: AmlApp| {
        let history_feed = CsvSource::builder(AccountHistoryCsv)
            .path(&app.account_history)
            .build()
            .map_err(resources_failed)?;
        let transactions_feed = CsvSource::builder(TransactionCsv)
            .path(&app.transactions)
            .build()
            .map_err(resources_failed)?;
        let rates_feed = CsvSource::builder(RateCsv)
            .path(&app.fx_rates)
            .build()
            .map_err(resources_failed)?;
        let profile = AccountProfile;
        let price = joins::left_as_of(
            |rate: &FxRate| rate.currency.clone(),
            |rate: &FxRate| rate.effective_at,
            |payment: &Transaction| payment.payment_currency.clone(),
            |payment: &Transaction| payment.timestamp,
            PricedTransaction::at,
        );
        let VendorBindings {
            risk_engine,
            case_system,
        } = VendorBindings::simulated(&app.risk_ledger, &app.case_ledger, stats.clone())
            .map_err(bindings_failed)?;
        let score = ScoreHandler;
        let daily_profile = DailyProfile::new(
            app.rules.fan_out_counterparties,
            app.rules.fan_in_counterparties,
        );
        let day_totals = stateful::reduce(DayTotals::default(), DayTotals::add).emit_on_eof();
        let open_case = OpenCaseHandler;
        register_dir(&app.cleared_register).map_err(resources_failed)?;
        let cleared_register = CsvSink::builder(ClearedRow)
            .path(&app.cleared_register)
            .columns(["txn_id", "rules_version"])
            .build()
            .map_err(resources_failed)?;
        register_dir(&app.case_register).map_err(resources_failed)?;
        let case_register = CsvSink::builder(CaseRow)
            .path(&app.case_register)
            .columns([
                "case_number",
                "alert_id",
                "bank",
                "account",
                "reasons",
                "rules_version",
            ])
            .build()
            .map_err(resources_failed)?;
        let totals_report = ConsoleSink::new(console::format_totals);

        Ok(flow! {
            name: "aml_overnight_monitoring",
            journals: disk_journals(journal_root.clone()),

            stages: {
                account_history = source!(AccountHistory => history_feed);
                transactions    = source!(Transaction => transactions_feed);
                fx_rates        = source!(FxRate => rates_feed);
                profile         = join!(
                    catalog account_history: AccountHistory, Transaction -> ProfiledTransaction => profile
                );
                price           = join!(
                    catalog fx_rates: FxRate, Transaction -> PricedTransaction => price
                );
                score           = effectful_transform!(
                    ProfiledTransaction -> { TransactionCleared, AlertRaised, ScoreUnavailable }
                    uses ScoreTransaction via risk_engine
                    => score
                );
                cleared         = sink!(TransactionCleared => cleared_register);
                daily_profile   = stateful!(Transaction -> AlertRaised => daily_profile);
                day_totals      = stateful!(PricedTransaction -> DayTotals => day_totals);
                alerts          = merge!(first score: AlertRaised => merges::concat);
                open_case       = effectful_transform!(
                    AlertRaised -> { CaseOpened, CaseUnavailable }
                    uses OpenCase via case_system
                    => open_case
                );
                case_register   = sink!(CaseOpened => case_register);
                totals_report   = sink!(DayTotals => totals_report, delivery: idempotent);
            },

            topology: {
                (account_history, transactions) |> profile;
                (fx_rates, transactions) |> price;
                transactions |> daily_profile;
                price |> day_totals;
                profile |> score;
                score |> cleared;
                score |> alerts;
                daily_profile |> alerts;
                alerts |> open_case;
                open_case |> case_register;
                day_totals |> totals_report;
            }
        })
    })
}
