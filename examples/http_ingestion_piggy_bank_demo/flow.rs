// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HTTP Ingestion Piggy Bank Demo (FLOWIP-084d, FLOWIP-084n)
//!
//! A curl-friendly end-to-end demo that uses:
//! - A finite YAML source (accounts, read once from `accounts.yaml`)
//! - HTTP ingestion (push-based transactions)
//! - Join stage (accounts as reference catalog; ledger entries as stream)
//! - Stateful stage (materializes a checkbook snapshot per posted entry)
//! - Console sink (prints balances + a transaction table)
//!
//! Run with localhost-only defaults:
//! `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features prometheus,web-host`
//!
//! Recommended control-plane auth example:
//! Provision `OBZENFLOW_PIGGY_BANK_CONTROL_PLANE_AUTH` out of band with the complete
//! expected `Authorization` header value before starting the process. The repository
//! guide at `crates/obzenflow_infra/src/web/README.md` explains the authentication scopes.
//! `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features prometheus,web-host -- --config examples/http_ingestion_piggy_bank_demo/obzenflow.auth.toml`
//!
//! Accounts `acct-1` (Alice, $10.00) and `acct-2` (Bob, $0.00) open from
//! `accounts.yaml` before any transaction joins. Post credits and debits:
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/tx/events -H 'content-type: application/json' -d '{"event_type":"bank.ledger_entry","data":{"account_id":"acct-1","kind":"Credit","amount_cents":250,"note":"paycheck"}}'`
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/tx/events -H 'content-type: application/json' -d '{"event_type":"bank.ledger_entry","data":{"account_id":"acct-1","kind":"Debit","amount_cents":99,"note":"coffee"}}'`
//!
//! Notes:
//! - Accounts are fixed at startup. `joins::inner` hydrates every account before
//!   any transaction joins, so there is no startup race for unknown accounts.
//! - Entries for accounts missing from `accounts.yaml` are dropped by the join.
//! - A zero-amount or malformed entry is refused with `400`, and the system journal
//!   records the refusal with the field it names.
//! - Strict replay (`-- --replay-from <run-dir>`) reuses the archived accounts and
//!   never reopens `accounts.yaml`.
//! - Transaction ingress has its own rate limiter.
//! - The stateful stage emits a `bank.checkbook` snapshot for every posted entry.
//! - The `{accepted,rejected}` response is per-request (single POST => accepted=1). For cumulative counts, check `/metrics`.
//! - Ingress POSTs above stay unauthenticated in this example. Control-plane auth protects built-ins such as `/metrics`, `/api/topology`, and `/api/flow/*`.
//! - With control-plane auth enabled, configure the client's `Authorization` header
//!   through its protected credential configuration before querying `/metrics` or
//!   `/api/topology`. Keep credential values out of command arguments and shell history.
//! - In this example, `runner.rs` owns the HTTP ingress bundle, hosting shell and
//!   accounts file, while `build_flow(...)` stays pipeline-only and accepts the
//!   typed sources. That split is intentional: hosting concerns stay outside `flow!`.

use super::domain::*;
use super::handlers::Checkbook;
use obzenflow::flow::{async_infinite_source, flow, join, sink, source, stateful, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::rate_limit;
use obzenflow::stages::sinks::SnapshotTableFormatter;
use obzenflow::stages::sources::{
    HostedIngressSource, IngressDecodeError, IngressDecoder, IngressRecord, YamlDecodeError,
    YamlDecoder, YamlRecord, YamlSource,
};
use obzenflow::stages::{joins, sinks};
use std::path::PathBuf;

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

pub fn build_flow(
    accounts_source: YamlSource<AccountYaml>,
    tx_source: HostedIngressSource<LedgerIngress>,
) -> FlowDefinition {
    // This function takes only typed sources, not `HttpIngress<D>` bundles.
    // The runner owns HTTP hosting; the flow owns pipeline topology.
    FlowDefinition::materialize(move |_runtime_config| {
        let post_entry = joins::inner(
            |account: &AccountOpened| account.account_id.clone(),
            |entry: &LedgerEntry| entry.account_id.clone(),
            |account, entry| PostedEntry {
                account_id: entry.account_id,
                owner: account.owner,
                kind: entry.kind,
                amount_cents: entry.amount_cents,
                initial_balance_cents: account.initial_balance_cents,
                note: entry.note,
            },
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
                tx = async_infinite_source!(
                    LedgerEntry => tx_source with { rate_limit(50.0) }
                );

                // Processing
                posted = join!(
                    catalog accounts: AccountOpened,
                    LedgerEntry -> PostedEntry => post_entry
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
                (accounts, tx) |> posted;
                posted |> checkbook;
                checkbook |> printer;
            }
        })
    })
}
