// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::schema::{StageOutputFacts, TypedPayload};
use serde::{Deserialize, Serialize};
use std::fmt;

pub const RETURNED_PAYMENT_FEE_CENTS: u64 = 3500;
pub const RETRY_DESCRIPTION: &str = "RETRY PYMT";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountOpened {
    pub account_id: String,
    pub owner: String,
    pub initial_balance_cents: i64,
}

impl TypedPayload for AccountOpened {
    const EVENT_TYPE: &'static str = "bank.account_opened";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    Credit,
    Debit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub account_id: String,
    pub kind: EntryKind,
    pub amount_cents: u64,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub presentment: Option<Presentment>,
}

impl TypedPayload for LedgerEntry {
    const EVENT_TYPE: &'static str = "bank.ledger_entry";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AchDebit {
    pub account_id: String,
    pub company_name: String,
    pub company_id: String,
    pub entry_description: String,
    pub amount_cents: u64,
}

impl TypedPayload for AchDebit {
    const EVENT_TYPE: &'static str = "bank.ach_debit";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Presentment {
    pub company_name: String,
    pub company_id: String,
    pub entry_description: String,
}

impl Presentment {
    pub fn is_retry(&self) -> bool {
        self.entry_description == RETRY_DESCRIPTION
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostedEntry {
    pub account_id: String,
    pub owner: String,
    pub kind: EntryKind,
    pub amount_cents: u64,
    pub initial_balance_cents: i64,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub presentment: Option<Presentment>,
}

impl TypedPayload for PostedEntry {
    const EVENT_TYPE: &'static str = "bank.posted_entry";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeRule {
    #[default]
    FeePerReturn,
    OneFeePerPayment,
}

impl FeeRule {
    pub fn fee_cents(self, presentment: u32) -> Option<u64> {
        match self {
            Self::OneFeePerPayment if presentment > 1 => None,
            Self::FeePerReturn | Self::OneFeePerPayment => Some(RETURNED_PAYMENT_FEE_CENTS),
        }
    }
}

impl fmt::Display for FeeRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::FeePerReturn => "fee_per_return v1",
            Self::OneFeePerPayment => "one_fee_per_payment v2",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceMoved {
    pub account_id: String,
    pub owner: String,
    pub kind: EntryKind,
    pub amount_cents: u64,
    #[serde(default)]
    pub note: Option<String>,
    pub balance_before_cents: i64,
    pub balance_cents: i64,
}

impl TypedPayload for BalanceMoved {
    const EVENT_TYPE: &'static str = "bank.balance_moved";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeCharged {
    pub account_id: String,
    pub company_name: String,
    pub company_id: String,
    pub amount_cents: u64,
    pub presentment: u32,
    pub balance_cents: i64,
    pub fee_cents: u64,
    pub fee_rule: String,
}

impl TypedPayload for FeeCharged {
    const EVENT_TYPE: &'static str = "bank.fee_charged";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryReturned {
    pub account_id: String,
    pub company_name: String,
    pub company_id: String,
    pub amount_cents: u64,
    pub presentment: u32,
    pub balance_cents: i64,
    pub fee_rule: String,
}

impl TypedPayload for RetryReturned {
    const EVENT_TYPE: &'static str = "bank.retry_returned";
}

#[derive(Debug, Clone, StageOutputFacts)]
pub enum Assessed {
    Moved(BalanceMoved),
    FeeCharged(FeeCharged),
    RetryReturned(RetryReturned),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckbookEntry {
    pub index: u64,
    pub kind: EntryKind,
    pub amount_cents: u64,
    pub balance_cents: i64,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckbookSnapshot {
    pub account_id: String,
    pub owner: String,
    pub current_balance_cents: i64,
    pub available_balance_cents: i64,
    pub total_credits_cents: i64,
    pub total_debits_cents: i64,
    pub transactions: Vec<CheckbookEntry>,
}

impl TypedPayload for CheckbookSnapshot {
    const EVENT_TYPE: &'static str = "bank.checkbook";
}

pub fn format_cents(cents: i64) -> String {
    let sign = if cents < 0 { "-" } else { "" };
    let abs = cents.abs();
    format!("{sign}${}.{:02}", abs / 100, abs % 100)
}

pub fn format_unsigned_cents(cents: u64) -> String {
    format!("${}.{:02}", cents / 100, cents % 100)
}
