// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HTTP Ingestion Piggy Bank Demo (FLOWIP-084d)
//!
//! A curl-friendly end-to-end demo that uses:
//! - HTTP ingestion (push-based source)
//! - Join stage (accounts as reference catalog; ledger entries as stream)
//! - Stateful stage (materializes a checkbook snapshot per posted entry)
//! - Console sink (prints balances + a transaction table)
//!
//! Run with localhost-only defaults:
//! `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server`
//!
//! Recommended control-plane auth example:
//! `export OBZENFLOW_PIGGY_BANK_CONTROL_PLANE_AUTH='Bearer piggy-bank-demo-secret'`
//! `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --config examples/http_ingestion_piggy_bank_demo/obzenflow.auth.toml`
//!
//! 1) Open accounts (reference side; required before ledger entries):
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/accounts/events -H 'content-type: application/json' -d '{"event_type":"bank.account_opened","data":{"account_id":"acct-1","owner":"Alice","initial_balance_cents":1000}}'`
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/accounts/events -H 'content-type: application/json' -d '{"event_type":"bank.account_opened","data":{"account_id":"acct-2","owner":"Bob","initial_balance_cents":0}}'`
//!
//! 2) Post credits and debits (stream side):
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/tx/events -H 'content-type: application/json' -d '{"event_type":"bank.ledger_entry","data":{"account_id":"acct-1","kind":"Credit","amount_cents":250,"note":"paycheck"}}'`
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/tx/events -H 'content-type: application/json' -d '{"event_type":"bank.ledger_entry","data":{"account_id":"acct-1","kind":"Debit","amount_cents":99,"note":"coffee"}}'`
//!
//! Notes:
//! - Entries for unknown accounts are dropped until the account exists.
//! - Accounts can be submitted at any time; the join catalog updates continuously.
//! - The stateful stage emits a `bank.checkbook` snapshot for every posted entry.
//! - The `{accepted,rejected}` response is per-request (single POST => accepted=1). For cumulative counts, check `/metrics`.
//! - Ingress POSTs above stay unauthenticated in this example. Control-plane auth protects built-ins such as `/metrics`, `/api/topology`, and `/api/flow/*`.
//! - With the recommended auth example above, query built-ins with:
//!   - `curl http://127.0.0.1:9090/metrics -H 'Authorization: Bearer piggy-bank-demo-secret'`
//!   - `curl http://127.0.0.1:9090/api/topology -H 'Authorization: Bearer piggy-bank-demo-secret'`
//! - In this example, `runner.rs` owns the HTTP ingress bundles and hosting shell,
//!   while `build_flow(...)` stays pipeline-only and accepts the extracted typed
//!   sources. That split is intentional: hosting concerns stay outside `flow!`.

use super::domain::*;
use super::handlers::Checkbook;
use obzenflow::typed::{joins, sinks};
use obzenflow_adapters::sinks::SnapshotTableFormatter;
use obzenflow_adapters::sources::http::HttpSourceTyped;
use obzenflow_dsl::{async_infinite_source, flow, join, sink, stateful, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::StatefulHandlerExt;
use obzenflow_runtime::stages::stateful::strategies::emissions::EmitAlways;
use std::path::PathBuf;

pub fn build_flow(
    accounts_source: HttpSourceTyped<AccountOpened>,
    tx_source: HttpSourceTyped<LedgerEntry>,
) -> FlowDefinition {
    // This function takes only typed sources, not `HttpIngress<T>` bundles.
    // The runner owns HTTP hosting; the flow owns pipeline topology.
    flow! {
        name: "http_ingestion_piggy_bank_demo",
        journals: disk_journals(PathBuf::from("target/http-ingestion-piggy-bank-demo-logs")),
        middleware: [],

        stages: {
            accounts = async_infinite_source!(AccountOpened => accounts_source);
            tx = async_infinite_source!(LedgerEntry => tx_source);

            posted = join!(
                catalog accounts: AccountOpened,
                LedgerEntry -> PostedEntry => joins::inner_live(
                    |account| account.account_id.clone(),
                    |entry| entry.account_id.clone(),
                    |account, entry| PostedEntry {
                        account_id: entry.account_id,
                        owner: account.owner,
                        kind: entry.kind,
                        amount_cents: entry.amount_cents,
                        initial_balance_cents: account.initial_balance_cents,
                        note: entry.note,
                    },
                )
            );

            checkbook = stateful!(PostedEntry -> CheckbookSnapshot => Checkbook::new().with_emission(EmitAlways));

            printer = sink!(CheckbookSnapshot => sinks::console(
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
                        format!(
                            "Account: {} ({})",
                            snapshot.account_id,
                            snapshot.owner,
                        ),
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
                })
            ));
        },

        topology: {
            tx |> posted;
            posted |> checkbook;
            checkbook |> printer;
        }
    }
}
