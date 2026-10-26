// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;

#[derive(Clone, Debug, Default)]
pub struct AccountLedger {
    owner: String,
    current_balance_cents: i64,
    total_credits_cents: i64,
    total_debits_cents: i64,
    transactions: Vec<CheckbookEntry>,
}

impl AccountLedger {
    pub fn record(&mut self, entry: &PostedEntry) {
        if self.transactions.is_empty() {
            self.current_balance_cents = entry.initial_balance_cents;
        }
        self.owner = entry.owner.clone();
        let amount = entry.amount_cents as i64;
        match entry.kind {
            EntryKind::Credit => {
                self.total_credits_cents = self.total_credits_cents.saturating_add(amount);
                self.current_balance_cents = self.current_balance_cents.saturating_add(amount);
            }
            EntryKind::Debit => {
                self.total_debits_cents = self.total_debits_cents.saturating_add(amount);
                self.current_balance_cents = self.current_balance_cents.saturating_sub(amount);
            }
        }
        self.transactions.push(CheckbookEntry {
            index: self.transactions.len() as u64 + 1,
            kind: entry.kind,
            amount_cents: entry.amount_cents,
            balance_cents: self.current_balance_cents,
            note: entry.note.clone(),
        });
    }

    pub fn snapshot(&self, account_id: &str) -> CheckbookSnapshot {
        CheckbookSnapshot {
            account_id: account_id.to_string(),
            owner: self.owner.clone(),
            current_balance_cents: self.current_balance_cents,
            available_balance_cents: self.current_balance_cents,
            total_credits_cents: self.total_credits_cents,
            total_debits_cents: self.total_debits_cents,
            transactions: self.transactions.clone(),
        }
    }
}
