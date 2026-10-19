// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use chrono::NaiveDateTime;
use obzenflow::schema::{StageOutputFacts, TypedPayload};
use obzenflow::stages::joins::AsOf;
use serde::{Deserialize, Serialize};
use std::fmt;

pub const REPORTING_CURRENCY: &str = "US Dollar";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TxnId(pub u64);

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FxRate {
    pub currency: String,
    pub effective_at: NaiveDateTime,
    pub usd_per_unit: f64,
}

impl TypedPayload for FxRate {
    const EVENT_TYPE: &'static str = "fx_reporting.fx_rate";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    pub txn_id: TxnId,
    pub booked_at: NaiveDateTime,
    pub from_bank: String,
    pub from_account: String,
    pub to_bank: String,
    pub to_account: String,
    pub amount_paid: f64,
    pub payment_currency: String,
}

impl TypedPayload for Transaction {
    const EVENT_TYPE: &'static str = "fx_reporting.transaction";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionPriced {
    pub txn_id: TxnId,
    pub booked_at: NaiveDateTime,
    pub payment_currency: String,
    pub amount_paid: f64,
    pub rate: FxRate,
    pub amount_usd: f64,
}

impl TransactionPriced {
    pub fn at(payment: &Transaction, rate: FxRate) -> Self {
        let amount_usd = (payment.amount_paid * rate.usd_per_unit * 100.0).round() / 100.0;
        Self {
            txn_id: payment.txn_id,
            booked_at: payment.booked_at,
            payment_currency: payment.payment_currency.clone(),
            amount_paid: payment.amount_paid,
            rate,
            amount_usd,
        }
    }
}

impl TypedPayload for TransactionPriced {
    const EVENT_TYPE: &'static str = "fx_reporting.transaction_priced";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InReportingCurrency {
    pub txn_id: TxnId,
    pub booked_at: NaiveDateTime,
    pub amount_paid: f64,
}

impl InReportingCurrency {
    pub fn for_payment(payment: &Transaction) -> Self {
        Self {
            txn_id: payment.txn_id,
            booked_at: payment.booked_at,
            amount_paid: payment.amount_paid,
        }
    }
}

impl TypedPayload for InReportingCurrency {
    const EVENT_TYPE: &'static str = "fx_reporting.in_reporting_currency";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RateMissing {
    pub txn_id: TxnId,
    pub booked_at: NaiveDateTime,
    pub payment_currency: String,
    pub amount_paid: f64,
}

impl RateMissing {
    pub fn for_payment(payment: &Transaction) -> Self {
        Self {
            txn_id: payment.txn_id,
            booked_at: payment.booked_at,
            payment_currency: payment.payment_currency.clone(),
            amount_paid: payment.amount_paid,
        }
    }
}

impl TypedPayload for RateMissing {
    const EVENT_TYPE: &'static str = "fx_reporting.rate_missing";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CurrencyNotQuoted {
    pub txn_id: TxnId,
    pub booked_at: NaiveDateTime,
    pub payment_currency: String,
    pub amount_paid: f64,
}

impl CurrencyNotQuoted {
    pub fn for_payment(payment: &Transaction) -> Self {
        Self {
            txn_id: payment.txn_id,
            booked_at: payment.booked_at,
            payment_currency: payment.payment_currency.clone(),
            amount_paid: payment.amount_paid,
        }
    }
}

impl TypedPayload for CurrencyNotQuoted {
    const EVENT_TYPE: &'static str = "fx_reporting.currency_not_quoted";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, StageOutputFacts)]
pub enum PricingOutcome {
    Priced(TransactionPriced),
    InReportingCurrency(InReportingCurrency),
    RateMissing(RateMissing),
    CurrencyNotQuoted(CurrencyNotQuoted),
}

pub fn price_payment(rate: AsOf<FxRate>, payment: Transaction) -> PricingOutcome {
    match rate {
        AsOf::Found(rate) => PricingOutcome::Priced(TransactionPriced::at(&payment, rate)),
        AsOf::BeforeFirstVersion => PricingOutcome::RateMissing(RateMissing::for_payment(&payment)),
        AsOf::NoVersions if payment.payment_currency == REPORTING_CURRENCY => {
            PricingOutcome::InReportingCurrency(InReportingCurrency::for_payment(&payment))
        }
        AsOf::NoVersions => {
            PricingOutcome::CurrencyNotQuoted(CurrencyNotQuoted::for_payment(&payment))
        }
    }
}
