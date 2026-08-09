// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{StatefulEmission, TypedStatefulHandler};
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct CheckbookState {
    ledgers: HashMap<String, AccountLedger>,
    last_snapshot: Option<CheckbookSnapshot>,
}

#[derive(Clone, Debug, Default)]
struct AccountLedger {
    owner: String,
    current_balance_cents: i64,
    total_credits_cents: i64,
    total_debits_cents: i64,
    transactions: Vec<CheckbookEntry>,
}

#[derive(Clone, Debug)]
pub struct Checkbook;

impl Checkbook {
    pub fn new() -> Self {
        Self
    }
}

impl TypedStatefulHandler for Checkbook {
    type State = CheckbookState;
    type Input = PostedEntry;
    type Output = CheckbookSnapshot;

    fn accumulate(&self, state: &mut Self::State, entry: PostedEntry) {
        let ledger = state
            .ledgers
            .entry(entry.account_id.clone())
            .or_insert_with(|| AccountLedger {
                owner: entry.owner.clone(),
                current_balance_cents: entry.initial_balance_cents,
                total_credits_cents: 0,
                total_debits_cents: 0,
                transactions: Vec::new(),
            });

        ledger.owner = entry.owner.clone();

        match entry.kind {
            EntryKind::Credit => {
                ledger.total_credits_cents = ledger
                    .total_credits_cents
                    .saturating_add(entry.amount_cents as i64);
                ledger.current_balance_cents = ledger
                    .current_balance_cents
                    .saturating_add(entry.amount_cents as i64);
            }
            EntryKind::Debit => {
                ledger.total_debits_cents = ledger
                    .total_debits_cents
                    .saturating_add(entry.amount_cents as i64);
                ledger.current_balance_cents = ledger
                    .current_balance_cents
                    .saturating_sub(entry.amount_cents as i64);
            }
        }

        let checkbook_entry = CheckbookEntry {
            index: ledger.transactions.len() as u64 + 1,
            kind: entry.kind,
            amount_cents: entry.amount_cents,
            balance_cents: ledger.current_balance_cents,
            note: entry.note,
        };
        ledger.transactions.push(checkbook_entry);

        state.last_snapshot = Some(CheckbookSnapshot {
            account_id: entry.account_id,
            owner: ledger.owner.clone(),
            current_balance_cents: ledger.current_balance_cents,
            available_balance_cents: ledger.current_balance_cents,
            total_credits_cents: ledger.total_credits_cents,
            total_debits_cents: ledger.total_debits_cents,
            transactions: ledger.transactions.clone(),
        });
    }

    fn initial_state(&self) -> Self::State {
        CheckbookState::default()
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        state.last_snapshot.is_some()
    }

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        let mut next_state = state.clone();
        let outputs = next_state.last_snapshot.take().into_iter().collect();
        Ok(StatefulEmission::RetainEpoch {
            next_state,
            outputs,
        })
    }
}
