// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;
use super::handlers::{AccountBook, Checkbook};
use obzenflow::flow::{
    async_infinite_source, flow, join, sink, source, stateful, transform, FlowBuildError,
    FlowDefinition,
};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::rate_limit;
use obzenflow::stages::sinks::SnapshotTableFormatter;
use obzenflow::stages::sources::{
    CsvDecodeError, CsvDecoder, CsvRecord, CsvSource, HostedIngressSource, IngressDecodeError,
    IngressDecoder, IngressRecord, YamlDecodeError, YamlDecoder, YamlRecord, YamlSelection,
    YamlSource,
};
use obzenflow::stages::{joins, sinks};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiggyBankSettings {
    accounts_file: PathBuf,
    transactions_file: PathBuf,
}

/// Decodes one entry of `accounts.yaml`; a negative opening balance rejects
/// only that account.
#[derive(Clone, Debug)]
pub(crate) struct AccountYaml;

impl YamlDecoder for AccountYaml {
    type Output = AccountOpened;

    fn decode(&self, record: YamlRecord<'_>) -> Result<AccountOpened, YamlDecodeError> {
        let account: AccountOpened = record.deserialize()?;
        if account.initial_balance_cents < 0 {
            return Err(YamlDecodeError::invalid_value("initial_balance_cents"));
        }
        Ok(account)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LedgerCsv;

impl CsvDecoder for LedgerCsv {
    type Output = LedgerEntry;

    fn decode(&self, record: CsvRecord<'_>) -> Result<LedgerEntry, CsvDecodeError> {
        let entry: LedgerEntry = record.deserialize()?;
        if entry.amount_cents == 0 {
            return Err(CsvDecodeError::invalid_value("amount_cents"));
        }
        Ok(entry)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AccountIngress;

impl IngressDecoder for AccountIngress {
    type Output = AccountOpened;

    fn decode(&self, record: IngressRecord<'_>) -> Result<AccountOpened, IngressDecodeError> {
        let account: AccountOpened = record.deserialize()?;
        if account.initial_balance_cents < 0 {
            return Err(IngressDecodeError::invalid_value("initial_balance_cents"));
        }
        Ok(account)
    }
}

/// Decodes one posted ledger entry; a zero amount rejects only that entry.
#[derive(Clone, Debug)]
pub(crate) struct LedgerIngress;

impl IngressDecoder for LedgerIngress {
    type Output = LedgerEntry;

    fn decode(&self, record: IngressRecord<'_>) -> Result<LedgerEntry, IngressDecodeError> {
        let entry: LedgerEntry = record.deserialize()?;
        if entry.amount_cents == 0 {
            return Err(IngressDecodeError::invalid_value("amount_cents"));
        }
        Ok(entry)
    }
}

fn post(account: AccountOpened, entry: LedgerEntry) -> PostedEntry {
    PostedEntry {
        account_id: entry.account_id,
        owner: account.owner,
        kind: entry.kind,
        amount_cents: entry.amount_cents,
        initial_balance_cents: account.initial_balance_cents,
        note: entry.note,
    }
}

pub fn build_flow(
    new_accounts_source: HostedIngressSource<AccountIngress>,
    tx_source: HostedIngressSource<LedgerIngress>,
) -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, settings: PiggyBankSettings| {
        let accounts_source = YamlSource::builder(AccountYaml)
            .path(settings.accounts_file)
            .selection(YamlSelection::SequenceAt("/accounts".into()))
            .build()
            .map_err(|error| FlowBuildError::StageResourcesFailed(error.to_string()))?;
        let ledger_source = CsvSource::builder(LedgerCsv)
            .path(settings.transactions_file)
            .chunk_size(1)
            .build()
            .map_err(|error| FlowBuildError::StageResourcesFailed(error.to_string()))?;
        let account_book = AccountBook;
        let post_batch = joins::inner(
            |account: &AccountOpened| account.account_id.clone(),
            |entry: &LedgerEntry| entry.account_id.clone(),
            post,
        );
        let post_live = joins::inner_live(
            |account: &AccountOpened| account.account_id.clone(),
            |entry: &LedgerEntry| entry.account_id.clone(),
            post,
        );
        let checkbook_handler = Checkbook;
        let printer_sink = sinks::ConsoleSink::<CheckbookSnapshot, _>::new(
            SnapshotTableFormatter::new(
                &["#", "Kind", "Amount", "Credit", "Debit", "Balance", "Note"],
                |snapshot: &CheckbookSnapshot| {
                    snapshot
                        .transactions
                        .iter()
                        .map(|entry| {
                            let amount = format_unsigned_cents(entry.amount_cents);
                            let (credit, debit) = match entry.kind {
                                EntryKind::Credit => (amount.clone(), String::new()),
                                EntryKind::Debit => (String::new(), amount.clone()),
                            };

                            let kind_label = match entry.kind {
                                EntryKind::Credit => "Credit",
                                EntryKind::Debit => "Debit",
                            };

                            vec![
                                entry.index.to_string(),
                                kind_label.to_string(),
                                amount,
                                credit,
                                debit,
                                format_cents(entry.balance_cents),
                                entry.note.as_deref().unwrap_or("").to_string(),
                            ]
                        })
                        .collect()
                },
            )
            .with_header(|snapshot: &CheckbookSnapshot| {
                vec![
                    format!("Account: {} ({})", snapshot.account_id, snapshot.owner,),
                    format!(
                        "Current: {} | Available: {}",
                        format_cents(snapshot.current_balance_cents),
                        format_cents(snapshot.available_balance_cents),
                    ),
                ]
            })
            .with_footer(|snapshot: &CheckbookSnapshot| {
                vec![format!(
                    "Credits: {} | Debits: {} | Tx: {}",
                    format_cents(snapshot.total_credits_cents),
                    format_cents(snapshot.total_debits_cents),
                    snapshot.transactions.len(),
                )]
            }),
        );

        Ok(flow! {
            name: "http_ingestion_piggy_bank_demo",
            journals: disk_journals(PathBuf::from(
                "target/http-ingestion-piggy-bank-demo-logs"
            )),

            stages: {
                // Ingestion
                accounts = source!(AccountOpened => accounts_source);
                ledger = source!(LedgerEntry => ledger_source with { rate_limit(2.0) });
                new_accounts = async_infinite_source!(AccountOpened => new_accounts_source);
                tx = async_infinite_source!(
                    LedgerEntry => tx_source with { rate_limit(50.0) }
                );

                // Processing
                all_accounts = transform!(AccountOpened -> AccountOpened => account_book);
                posted_batch = join!(
                    catalog accounts: AccountOpened,
                    LedgerEntry -> PostedEntry => post_batch
                );
                posted_live = join!(
                    catalog all_accounts: AccountOpened,
                    LedgerEntry -> PostedEntry => post_live
                );
                checkbook = stateful!(
                    PostedEntry -> CheckbookSnapshot => checkbook_handler
                );

                // Delivery
                printer = sink!(
                    CheckbookSnapshot => printer_sink
                );
            },

            topology: {
                accounts |> all_accounts;
                new_accounts |> all_accounts;
                (accounts, ledger) |> posted_batch;
                (all_accounts, tx) |> posted_live;
                posted_batch |> checkbook;
                posted_live |> checkbook;
                checkbook |> printer;
            }
        })
    })
}
