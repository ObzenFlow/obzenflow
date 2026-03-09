// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::schema::TypedPayload;
use serde::{Deserialize, Serialize};

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
}

impl TypedPayload for LedgerEntry {
    const EVENT_TYPE: &'static str = "bank.ledger_entry";
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
}

impl TypedPayload for PostedEntry {
    const EVENT_TYPE: &'static str = "bank.posted_entry";
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
