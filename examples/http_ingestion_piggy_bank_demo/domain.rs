// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::money::{Balance, UsdCents};
use obzenflow::schema::{StageOutputFacts, TypedPayload};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::num::NonZeroU32;

pub const RETURNED_PAYMENT_FEE: UsdCents = UsdCents::whole_dollars(35);
pub const RETRY_DESCRIPTION: &str = "RETRY PYMT";

/// An account number, "chk-58213".
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccountId(pub String);

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}

/// A merchant's Nacha company identification, "1834577921".
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CompanyId(pub String);

impl fmt::Display for CompanyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}

/// Which time a payment has been presented. A retry of a payment still unpaid
/// is the next attempt; a payment that cleared starts over at the first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Attempt(NonZeroU32);

impl Attempt {
    pub const FIRST: Self = Self(NonZeroU32::MIN);

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn is_first(self) -> bool {
        self == Self::FIRST
    }
}

impl fmt::Display for Attempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountOpened {
    pub account: AccountId,
    pub owner: String,
    pub opening_balance: Balance,
}

impl TypedPayload for AccountOpened {
    const EVENT_TYPE: &'static str = "bank.account_opened";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    Credit,
    Debit,
}

/// The merchant behind an ACH debit, in the fields Nacha requires a retry to
/// repeat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Originator {
    pub company_name: String,
    pub company_id: CompanyId,
    pub entry_description: String,
}

impl Originator {
    pub fn is_retry(&self) -> bool {
        self.entry_description == RETRY_DESCRIPTION
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub account: AccountId,
    pub kind: EntryKind,
    pub amount: UsdCents,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub originator: Option<Originator>,
}

impl TypedPayload for LedgerEntry {
    const EVENT_TYPE: &'static str = "bank.ledger_entry";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AchDebit {
    pub account: AccountId,
    pub company_name: String,
    pub company_id: CompanyId,
    pub entry_description: String,
    pub amount: UsdCents,
}

impl TypedPayload for AchDebit {
    const EVENT_TYPE: &'static str = "bank.ach_debit";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostedEntry {
    pub account: AccountId,
    pub owner: String,
    pub kind: EntryKind,
    pub amount: UsdCents,
    pub opening_balance: Balance,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub originator: Option<Originator>,
}

impl TypedPayload for PostedEntry {
    const EVENT_TYPE: &'static str = "bank.posted_entry";
}

/// The same merchant asking the same account for the same amount.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PaymentKey {
    pub account: AccountId,
    pub company_id: CompanyId,
    pub amount: UsdCents,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeRule {
    #[default]
    FeePerReturn,
    OneFeePerPayment,
}

impl FeeRule {
    pub fn fee(self, attempt: Attempt) -> Option<UsdCents> {
        match self {
            Self::OneFeePerPayment if !attempt.is_first() => None,
            Self::FeePerReturn | Self::OneFeePerPayment => Some(RETURNED_PAYMENT_FEE),
        }
    }
}

impl fmt::Display for FeeRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::FeePerReturn => "fee_per_return",
            Self::OneFeePerPayment => "one_fee_per_payment",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceMoved {
    pub account: AccountId,
    pub owner: String,
    pub kind: EntryKind,
    pub amount: UsdCents,
    #[serde(default)]
    pub note: Option<String>,
    pub balance_before: Balance,
    pub balance: Balance,
}

impl TypedPayload for BalanceMoved {
    const EVENT_TYPE: &'static str = "bank.balance_moved";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeCharged {
    pub account: AccountId,
    pub company_id: CompanyId,
    pub company_name: String,
    pub amount: UsdCents,
    pub attempt: Attempt,
    pub balance: Balance,
    pub fee: UsdCents,
    pub fee_rule: FeeRule,
}

impl TypedPayload for FeeCharged {
    const EVENT_TYPE: &'static str = "bank.fee_charged";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryReturned {
    pub account: AccountId,
    pub company_id: CompanyId,
    pub company_name: String,
    pub amount: UsdCents,
    pub attempt: Attempt,
    pub balance: Balance,
    pub fee_rule: FeeRule,
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
    pub amount: UsdCents,
    pub balance: Balance,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckbookSnapshot {
    pub account: AccountId,
    pub owner: String,
    pub current_balance: Balance,
    pub available_balance: Balance,
    pub total_credits: UsdCents,
    pub total_debits: UsdCents,
    pub transactions: Vec<CheckbookEntry>,
}

impl TypedPayload for CheckbookSnapshot {
    const EVENT_TYPE: &'static str = "bank.checkbook";
}
