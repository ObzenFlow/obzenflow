// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;
use super::money::{Balance, UsdCents};
use obzenflow::stages::stateful::{StatefulEmission, TypedStatefulHandler};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct AccountLedger {
    owner: String,
    current_balance: Balance,
    total_credits: UsdCents,
    total_debits: UsdCents,
    transactions: Vec<CheckbookEntry>,
}

impl AccountLedger {
    pub fn record(&mut self, entry: &BalanceMoved) {
        if self.transactions.is_empty() {
            self.current_balance = entry.balance_before;
        }
        self.owner = entry.owner.clone();
        match entry.kind {
            EntryKind::Credit => {
                self.total_credits = self.total_credits.plus(entry.amount);
                self.current_balance = self.current_balance.credited(entry.amount);
            }
            EntryKind::Debit => {
                self.total_debits = self.total_debits.plus(entry.amount);
                self.current_balance = self.current_balance.debited(entry.amount);
            }
        }
        self.transactions.push(CheckbookEntry {
            index: self.transactions.len() as u64 + 1,
            kind: entry.kind,
            amount: entry.amount,
            balance: self.current_balance,
            note: entry.note.clone(),
        });
    }

    pub fn snapshot(&self, account: &AccountId) -> CheckbookSnapshot {
        CheckbookSnapshot {
            account: account.clone(),
            owner: self.owner.clone(),
            current_balance: self.current_balance,
            available_balance: self.current_balance,
            total_credits: self.total_credits,
            total_debits: self.total_debits,
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

#[derive(Clone, Debug, Default)]
pub struct Assessment {
    balances: BTreeMap<AccountId, Balance>,
    unpaid: BTreeMap<PaymentKey, Attempt>,
    decided: Vec<Assessed>,
}

impl Assessment {
    fn balance(&mut self, entry: &PostedEntry) -> Balance {
        *self
            .balances
            .entry(entry.account.clone())
            .or_insert(entry.opening_balance)
    }

    fn post(
        &mut self,
        entry: &PostedEntry,
        kind: EntryKind,
        amount: UsdCents,
        note: Option<String>,
    ) {
        let balance_before = self.balance(entry);
        let balance = match kind {
            EntryKind::Credit => balance_before.credited(amount),
            EntryKind::Debit => balance_before.debited(amount),
        };
        self.balances.insert(entry.account.clone(), balance);
        self.decided.push(Assessed::Moved(BalanceMoved {
            account: entry.account.clone(),
            owner: entry.owner.clone(),
            kind,
            amount,
            note,
            balance_before,
            balance,
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
        let Some(originator) = &entry.originator else {
            state.post(&entry, entry.kind, entry.amount, entry.note.clone());
            return;
        };
        let payment = PaymentKey {
            account: entry.account.clone(),
            company_id: originator.company_id.clone(),
            amount: entry.amount,
        };
        let attempt = match state.unpaid.get(&payment) {
            Some(earlier) if originator.is_retry() => earlier.next(),
            _ => Attempt::FIRST,
        };
        let balance = state.balance(&entry);
        if balance.covers(entry.amount) {
            state.unpaid.remove(&payment);
            let note = Some(originator.company_name.clone());
            state.post(&entry, EntryKind::Debit, entry.amount, note);
            return;
        }
        match self.fee_rule.fee(attempt) {
            Some(fee) => {
                state.decided.push(Assessed::FeeCharged(FeeCharged {
                    account: entry.account.clone(),
                    company_id: originator.company_id.clone(),
                    company_name: originator.company_name.clone(),
                    amount: entry.amount,
                    attempt,
                    balance,
                    fee,
                    fee_rule: self.fee_rule,
                }));
                let note = Some(format!("NSF fee: {}", originator.company_name));
                state.post(&entry, EntryKind::Debit, fee, note);
            }
            None => state.decided.push(Assessed::RetryReturned(RetryReturned {
                account: entry.account.clone(),
                company_id: originator.company_id.clone(),
                company_name: originator.company_name.clone(),
                amount: entry.amount,
                attempt,
                balance,
                fee_rule: self.fee_rule,
            })),
        }
        state.unpaid.insert(payment, attempt);
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        !state.decided.is_empty()
    }

    fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
        StatefulEmission::ResetEpoch {
            outputs: state.decided.clone(),
        }
    }

    fn post_emit(&self, state: &mut Self::State) {
        state.decided.clear();
    }
}
