// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{
    price_payment, CurrencyNotQuoted, FxRate, InReportingCurrency, RateMissing, Transaction,
    TransactionPriced, TxnId,
};
use chrono::NaiveDateTime;
use obzenflow::error::HandlerError;
use obzenflow::flow::{flow, join, sink, source, FlowBuildError, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::stages::joins;
use obzenflow::stages::sinks::{CsvProjection, CsvSink};
use obzenflow::stages::sources::{CsvDecodeError, CsvDecoder, CsvRecord, CsvSource};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const DEMO_JOURNAL_ROOT: &str = "target/fx-reporting-view-logs";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FxApp {
    pub rates: PathBuf,
    pub transactions: PathBuf,
    pub report: PathBuf,
}

#[derive(Clone, Debug)]
struct RateCsv;

impl CsvDecoder for RateCsv {
    type Output = FxRate;
}

#[derive(Deserialize)]
struct TransactionRow {
    txn_id: u64,
    timestamp: String,
    from_bank: String,
    from_account: String,
    to_bank: String,
    to_account: String,
    amount_paid: f64,
    payment_currency: String,
}

#[derive(Clone, Debug)]
struct TransactionCsv;

impl CsvDecoder for TransactionCsv {
    type Output = Transaction;

    fn decode(&self, record: CsvRecord<'_>) -> Result<Transaction, CsvDecodeError> {
        let row: TransactionRow = record.deserialize()?;
        let booked_at = NaiveDateTime::parse_from_str(&row.timestamp, "%Y/%m/%d %H:%M")
            .map_err(|_| CsvDecodeError::invalid_value("timestamp"))?;
        Ok(Transaction {
            txn_id: TxnId(row.txn_id),
            booked_at,
            from_bank: row.from_bank,
            from_account: row.from_account,
            to_bank: row.to_bank,
            to_account: row.to_account,
            amount_paid: row.amount_paid,
            payment_currency: row.payment_currency,
        })
    }
}

#[derive(Clone, Debug)]
struct ReportRow;

#[derive(Serialize)]
struct ReportRecord {
    txn_id: u64,
    booked_at: String,
    payment_currency: String,
    amount_paid: f64,
    usd_per_unit: f64,
    rate_effective_at: String,
    amount_usd: f64,
}

impl CsvProjection for ReportRow {
    type Input = TransactionPriced;
    type Row = ReportRecord;

    fn project(&self, priced: TransactionPriced) -> Result<ReportRecord, HandlerError> {
        Ok(ReportRecord {
            txn_id: priced.txn_id.0,
            booked_at: priced.booked_at.format("%Y-%m-%d %H:%M").to_string(),
            payment_currency: priced.payment_currency,
            amount_paid: priced.amount_paid,
            usd_per_unit: priced.rate.usd_per_unit,
            rate_effective_at: priced
                .rate
                .effective_at
                .format("%Y-%m-%d %H:%M")
                .to_string(),
            amount_usd: priced.amount_usd,
        })
    }
}

fn resources_failed(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(error.to_string())
}

fn report_dir(report: &Path) -> Result<(), String> {
    match report
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        Some(parent) => std::fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display())),
        None => Ok(()),
    }
}

pub fn build_flow() -> FlowDefinition {
    assemble_flow(PathBuf::from(DEMO_JOURNAL_ROOT))
}

pub fn assemble_flow(journal_root: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, app: FxApp| {
        let treasury_rates = CsvSource::builder(RateCsv)
            .path(&app.rates)
            .build()
            .map_err(resources_failed)?;
        let transactions_feed = CsvSource::builder(TransactionCsv)
            .path(&app.transactions)
            .build()
            .map_err(resources_failed)?;
        let price = joins::left_as_of(
            |rate: &FxRate| rate.currency.clone(),
            |rate: &FxRate| rate.effective_at,
            |payment: &Transaction| payment.payment_currency.clone(),
            |payment: &Transaction| payment.booked_at,
            price_payment,
        );
        report_dir(&app.report).map_err(resources_failed)?;
        let report = CsvSink::builder(ReportRow)
            .path(&app.report)
            .columns([
                "txn_id",
                "booked_at",
                "payment_currency",
                "amount_paid",
                "usd_per_unit",
                "rate_effective_at",
                "amount_usd",
            ])
            .build()
            .map_err(resources_failed)?;

        Ok(flow! {
            name: "fx_reporting_view",
            journals: disk_journals(journal_root.clone()),

            stages: {
                fx_rates     = source!(FxRate => treasury_rates);
                transactions = source!(Transaction => transactions_feed);
                price        = join!(
                    catalog fx_rates: FxRate,
                    Transaction -> {
                        TransactionPriced, InReportingCurrency, RateMissing, CurrencyNotQuoted
                    } => price
                );
                report       = sink!(TransactionPriced => report);
            },

            topology: {
                (fx_rates, transactions) |> price;
                price |> report;
            }
        })
    })
}
