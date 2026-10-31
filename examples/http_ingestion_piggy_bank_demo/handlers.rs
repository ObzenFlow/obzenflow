// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;
use obzenflow::stages::stateful::{StatefulEmission, TypedStatefulHandler};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct AccountLedger {
    owner: String,
    current_balance_cents: i64,
    total_credits_cents: i64,
    total_debits_cents: i64,
    transactions: Vec<CheckbookEntry>,
}

impl AccountLedger {
    pub fn record(&mut self, entry: &BalanceMoved) {
        if self.transactions.is_empty() {
            self.current_balance_cents = entry.balance_before_cents;
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

#[derive(Clone, Debug)]
pub struct Assess {
    fee_rule: FeeRule,
}

impl Assess {
    pub fn new(fee_rule: FeeRule) -> Self {
        Self { fee_rule }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Payment {
    account_id: String,
    company_name: String,
    company_id: String,
    amount_cents: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Assessment {
    balances: BTreeMap<String, i64>,
    unpaid: BTreeMap<Payment, u32>,
    pending: Vec<Assessed>,
}

impl Assessment {
    fn balance(&mut self, entry: &PostedEntry) -> &mut i64 {
        self.balances
            .entry(entry.account_id.clone())
            .or_insert(entry.initial_balance_cents)
    }

    fn post(
        &mut self,
        entry: &PostedEntry,
        kind: EntryKind,
        amount_cents: u64,
        note: Option<String>,
    ) {
        let balance = self.balance(entry);
        let balance_before_cents = *balance;
        let amount = amount_cents as i64;
        *balance = match kind {
            EntryKind::Credit => balance_before_cents.saturating_add(amount),
            EntryKind::Debit => balance_before_cents.saturating_sub(amount),
        };
        let balance_cents = *balance;
        self.pending.push(Assessed::Moved(BalanceMoved {
            account_id: entry.account_id.clone(),
            owner: entry.owner.clone(),
            kind,
            amount_cents,
            note,
            balance_before_cents,
            balance_cents,
        }));
    }
}

impl TypedStatefulHandler for Assess {
    type State = Assessment;
    type Input = PostedEntry;
    type Output = Assessed;

    fn initial_state(&self) -> Self::State {
        Assessment::default()
    }

    fn accumulate(&self, state: &mut Self::State, entry: PostedEntry) {
        let Some(presented) = &entry.presentment else {
            state.post(&entry, entry.kind, entry.amount_cents, entry.note.clone());
            return;
        };
        let payment = Payment {
            account_id: entry.account_id.clone(),
            company_name: presented.company_name.clone(),
            company_id: presented.company_id.clone(),
            amount_cents: entry.amount_cents,
        };
        let presentment = match state.unpaid.get(&payment) {
            Some(earlier) if presented.is_retry() => earlier + 1,
            _ => 1,
        };
        let balance_cents = *state.balance(&entry);
        if balance_cents >= entry.amount_cents as i64 {
            state.unpaid.remove(&payment);
            let note = Some(payment.company_name);
            state.post(&entry, EntryKind::Debit, entry.amount_cents, note);
            return;
        }
        let fee_cents = self.fee_rule.fee_cents(presentment);
        state.pending.push(Assessed::Returned(PaymentReturned {
            account_id: payment.account_id.clone(),
            company_name: payment.company_name.clone(),
            company_id: payment.company_id.clone(),
            amount_cents: payment.amount_cents,
            presentment,
            balance_cents,
            fee_cents,
            fee_rule: self.fee_rule.to_string(),
        }));
        if fee_cents > 0 {
            let note = Some(format!("NSF fee: {}", payment.company_name));
            state.post(&entry, EntryKind::Debit, fee_cents, note);
        }
        state.unpaid.insert(payment, presentment);
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        !state.pending.is_empty()
    }

    fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
        StatefulEmission::ResetEpoch {
            outputs: state.pending.clone(),
        }
    }

    fn post_emit(&self, state: &mut Self::State) {
        state.pending.clear();
    }
}
