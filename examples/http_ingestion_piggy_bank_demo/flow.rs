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
//! Run with:
//! `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --server --server-port 9090`
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

use super::domain::*;
use super::handlers::Checkbook;
use anyhow::Result;
use obzenflow_adapters::sinks::{ConsoleSink, SnapshotTableFormatter};
use obzenflow_adapters::sources::http::HttpSource;
use obzenflow_dsl::{async_infinite_source, flow, join, sink, stateful};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::web::endpoints::event_ingestion::{
    create_ingestion_endpoints, IngestionConfig, TypedValidator, ValidationConfig,
};
use obzenflow_runtime::stages::common::handlers::StatefulHandlerExt;
use obzenflow_runtime::stages::join::strategies::InnerJoinBuilder;
use obzenflow_runtime::stages::stateful::strategies::emissions::EmitAlways;
use std::path::PathBuf;
use std::sync::Arc;

pub fn run_example() -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    Ok(())
}

async fn run() -> Result<()> {
    // Required for --server mode (FlowApplication enforces metrics for extra endpoints).
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "prometheus");

    let accounts_config = IngestionConfig {
        base_path: "/api/bank/accounts".to_string(),
        validation: Some(ValidationConfig::Single {
            validator: Arc::new(TypedValidator::<AccountOpened>::new()),
        }),
        ..Default::default()
    };
    let (accounts_endpoints, accounts_rx, accounts_state) =
        create_ingestion_endpoints(accounts_config);
    let accounts_hook_state = accounts_state.clone();
    let accounts_source = HttpSource::with_telemetry(accounts_rx, accounts_state.telemetry());

    let tx_config = IngestionConfig {
        base_path: "/api/bank/tx".to_string(),
        validation: Some(ValidationConfig::Single {
            validator: Arc::new(TypedValidator::<LedgerEntry>::new()),
        }),
        ..Default::default()
    };
    let (tx_endpoints, tx_rx, tx_state) = create_ingestion_endpoints(tx_config);
    let tx_hook_state = tx_state.clone();
    let tx_source = HttpSource::with_telemetry(tx_rx, tx_state.telemetry());

    let mut endpoints = Vec::new();
    endpoints.extend(accounts_endpoints);
    endpoints.extend(tx_endpoints);

    let join_handler = InnerJoinBuilder::<AccountOpened, LedgerEntry, PostedEntry>::new()
        .catalog_key(|account: &AccountOpened| account.account_id.clone())
        .stream_key(|entry: &LedgerEntry| entry.account_id.clone())
        .live()
        .build(|account: AccountOpened, entry: LedgerEntry| PostedEntry {
            account_id: entry.account_id,
            owner: account.owner,
            kind: entry.kind,
            amount_cents: entry.amount_cents,
            initial_balance_cents: account.initial_balance_cents,
            note: entry.note,
        });

    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_web_endpoints(endpoints)
        .with_flow_handle_hook(move |flow_handle| {
            accounts_hook_state.watch_pipeline_state(flow_handle.state_receiver())
        })
        .with_flow_handle_hook(move |flow_handle| {
            tx_hook_state.watch_pipeline_state(flow_handle.state_receiver())
        })
        .run_async(flow! {
            name: "http_ingestion_piggy_bank_demo",
            journals: disk_journals(PathBuf::from("target/http-ingestion-piggy-bank-demo-logs")),
            middleware: [],

            stages: {
                accounts = async_infinite_source!(AccountOpened => accounts_source);
                tx = async_infinite_source!(LedgerEntry => tx_source);

                posted = join!(catalog accounts: AccountOpened, LedgerEntry -> PostedEntry => join_handler);

                checkbook = stateful!(PostedEntry -> CheckbookSnapshot => Checkbook::new().with_emission(EmitAlways));

                printer = sink!(CheckbookSnapshot => ConsoleSink::<CheckbookSnapshot>::new(
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
        })
        .await?;

    Ok(())
}
