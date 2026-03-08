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

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::sinks::{ConsoleSink, SnapshotTableFormatter};
use obzenflow_adapters::sources::http::HttpSource;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_dsl::{async_infinite_source, flow, join, sink, stateful, with_ref};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::web::endpoints::event_ingestion::{
    create_ingestion_endpoints, IngestionConfig, TypedValidator, ValidationConfig,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{StatefulHandler, StatefulHandlerExt};
use obzenflow_runtime::stages::join::strategies::InnerJoinBuilder;
use obzenflow_runtime::stages::stateful::strategies::emissions::EmitAlways;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Domain events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccountOpened {
    account_id: String,
    owner: String,
    initial_balance_cents: i64,
}

impl TypedPayload for AccountOpened {
    const EVENT_TYPE: &'static str = "bank.account_opened";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum EntryKind {
    Credit,
    Debit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerEntry {
    account_id: String,
    kind: EntryKind,
    amount_cents: u64,
    #[serde(default)]
    note: Option<String>,
}

impl TypedPayload for LedgerEntry {
    const EVENT_TYPE: &'static str = "bank.ledger_entry";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostedEntry {
    account_id: String,
    owner: String,
    kind: EntryKind,
    amount_cents: u64,
    initial_balance_cents: i64,
    #[serde(default)]
    note: Option<String>,
}

impl TypedPayload for PostedEntry {
    const EVENT_TYPE: &'static str = "bank.posted_entry";
}

// ---------------------------------------------------------------------------
// Projection types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckbookEntry {
    index: u64,
    kind: EntryKind,
    amount_cents: u64,
    balance_cents: i64,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckbookSnapshot {
    account_id: String,
    owner: String,
    current_balance_cents: i64,
    available_balance_cents: i64,
    total_credits_cents: i64,
    total_debits_cents: i64,
    transactions: Vec<CheckbookEntry>,
}

impl TypedPayload for CheckbookSnapshot {
    const EVENT_TYPE: &'static str = "bank.checkbook";
}

// ---------------------------------------------------------------------------
// Stateful handler — accumulates posted entries into a checkbook projection
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
struct CheckbookState {
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
struct Checkbook {
    writer_id: WriterId,
}

impl Checkbook {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for Checkbook {
    type State = CheckbookState;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let Some(entry) = PostedEntry::from_event(&event) else {
            return;
        };

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

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let Some(snapshot) = state.last_snapshot.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(vec![snapshot.clone().to_event(self.writer_id)])
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let Some(snapshot) = state.last_snapshot.take() else {
            return Ok(Vec::new());
        };
        Ok(vec![snapshot.to_event(self.writer_id)])
    }
}

fn format_cents(cents: i64) -> String {
    let sign = if cents < 0 { "-" } else { "" };
    let abs = cents.abs();
    format!("{sign}${}.{:02}", abs / 100, abs % 100)
}

fn format_unsigned_cents(cents: u64) -> String {
    format!("${}.{:02}", cents / 100, cents % 100)
}

#[tokio::main]
async fn main() -> Result<()> {
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
        .build(
            |account: AccountOpened, entry: LedgerEntry| PostedEntry {
                account_id: entry.account_id,
                owner: account.owner,
                kind: entry.kind,
                amount_cents: entry.amount_cents,
                initial_balance_cents: account.initial_balance_cents,
                note: entry.note,
            },
        );

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
                accounts = async_infinite_source!("accounts" => accounts_source);
                tx = async_infinite_source!("tx" => tx_source);

                posted = join!("posted" => with_ref!(accounts, join_handler));

                checkbook = stateful!("checkbook" => Checkbook::new().with_emission(EmitAlways));

                printer = sink!("printer" => ConsoleSink::<CheckbookSnapshot>::new(
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
