// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HTTP Ingestion Piggy Bank Demo (FLOWIP-084d)
//!
//! A curl-friendly end-to-end demo that uses:
//! - HTTP ingestion (push-based source)
//! - Join stage (accounts as reference catalog; transactions as stream)
//! - Stateful stage (materializes a checkbook snapshot per tx)
//! - Console sink (prints balances + a transaction table)
//!
//! Run with:
//! `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --server --server-port 9090`
//!
//! 1) Create accounts (reference side; required before transactions):
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/accounts/events -H 'content-type: application/json' -d '{"event_type":"bank.account","data":{"account_id":"acct-1","owner":"Alice","initial_balance_cents":1000}}'`
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/accounts/events -H 'content-type: application/json' -d '{"event_type":"bank.account","data":{"account_id":"acct-2","owner":"Bob","initial_balance_cents":0}}'`
//!
//! 2) Post transactions (stream side):
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/tx/events -H 'content-type: application/json' -d '{"event_type":"bank.tx","data":{"account_id":"acct-1","delta_cents":250,"note":"paycheck"}}'`
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/tx/events -H 'content-type: application/json' -d '{"event_type":"bank.tx","data":{"account_id":"acct-1","delta_cents":-99,"note":"coffee"}}'`
//!
//! Notes:
//! - Transactions for unknown accounts are dropped until the account exists.
//! - Accounts can be submitted at any time; the join catalog updates continuously.
//! - The stateful stage emits a `bank.checkbook` snapshot for every accepted transaction.
//! - The `{accepted,rejected}` response is per-request (single POST => accepted=1). For cumulative counts, check `/metrics`.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::sinks::{ConsoleSink, SnapshotTableFormatter};
use obzenflow_adapters::sources::http::HttpSource;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_dsl_infra::{async_infinite_source, flow, join, sink, stateful, with_ref};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::web::endpoints::event_ingestion::{
    create_ingestion_endpoints, IngestionConfig, TypedValidator, ValidationConfig,
};
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{StatefulHandler, StatefulHandlerExt};
use obzenflow_runtime_services::stages::join::strategies::InnerJoinBuilder;
use obzenflow_runtime_services::stages::stateful::strategies::emissions::EmitAlways;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BankAccount {
    account_id: String,
    owner: String,
    initial_balance_cents: i64,
}

impl TypedPayload for BankAccount {
    const EVENT_TYPE: &'static str = "bank.account";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BankTransaction {
    account_id: String,
    delta_cents: i64,
    #[serde(default)]
    note: Option<String>,
}

impl TypedPayload for BankTransaction {
    const EVENT_TYPE: &'static str = "bank.tx";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnrichedBankTransaction {
    account_id: String,
    owner: String,
    initial_balance_cents: i64,
    delta_cents: i64,
    #[serde(default)]
    note: Option<String>,
}

impl TypedPayload for EnrichedBankTransaction {
    const EVENT_TYPE: &'static str = "bank.tx_enriched";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckbookEntry {
    index: u64,
    delta_cents: i64,
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
        let Some(tx) = EnrichedBankTransaction::from_event(&event) else {
            return;
        };

        let ledger = state
            .ledgers
            .entry(tx.account_id.clone())
            .or_insert_with(|| AccountLedger {
                owner: tx.owner.clone(),
                current_balance_cents: tx.initial_balance_cents,
                total_credits_cents: 0,
                total_debits_cents: 0,
                transactions: Vec::new(),
            });

        ledger.owner = tx.owner.clone();

        if tx.delta_cents >= 0 {
            ledger.total_credits_cents = ledger.total_credits_cents.saturating_add(tx.delta_cents);
        } else {
            ledger.total_debits_cents = ledger
                .total_debits_cents
                .saturating_add(tx.delta_cents.saturating_abs());
        }

        ledger.current_balance_cents = ledger.current_balance_cents.saturating_add(tx.delta_cents);

        let entry = CheckbookEntry {
            index: ledger.transactions.len() as u64 + 1,
            delta_cents: tx.delta_cents,
            balance_cents: ledger.current_balance_cents,
            note: tx.note,
        };
        ledger.transactions.push(entry);

        state.last_snapshot = Some(CheckbookSnapshot {
            account_id: tx.account_id,
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

#[tokio::main]
async fn main() -> Result<()> {
    // Required for --server mode (FlowApplication enforces metrics for extra endpoints).
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "prometheus");

    let accounts_config = IngestionConfig {
        base_path: "/api/bank/accounts".to_string(),
        validation: Some(ValidationConfig::Single {
            validator: Arc::new(TypedValidator::<BankAccount>::new()),
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
            validator: Arc::new(TypedValidator::<BankTransaction>::new()),
        }),
        ..Default::default()
    };
    let (tx_endpoints, tx_rx, tx_state) = create_ingestion_endpoints(tx_config);
    let tx_hook_state = tx_state.clone();
    let tx_source = HttpSource::with_telemetry(tx_rx, tx_state.telemetry());

    let mut endpoints = Vec::new();
    endpoints.extend(accounts_endpoints);
    endpoints.extend(tx_endpoints);

    let join_handler =
        InnerJoinBuilder::<BankAccount, BankTransaction, EnrichedBankTransaction>::new()
            .catalog_key(|account: &BankAccount| account.account_id.clone())
            .stream_key(|tx: &BankTransaction| tx.account_id.clone())
            .live()
            .build(
                |account: BankAccount, tx: BankTransaction| EnrichedBankTransaction {
                    account_id: tx.account_id,
                    owner: account.owner,
                    initial_balance_cents: account.initial_balance_cents,
                    delta_cents: tx.delta_cents,
                    note: tx.note,
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

                enriched = join!("enriched" => with_ref!(accounts, join_handler));

                checkbook = stateful!("checkbook" => Checkbook::new().with_emission(EmitAlways));

                printer = sink!("printer" => ConsoleSink::<CheckbookSnapshot>::new(
                    SnapshotTableFormatter::new(
                        &["#", "Δ", "Credit", "Debit", "Balance", "Note"],
                        |snapshot: &CheckbookSnapshot| {
                            snapshot
                                .transactions
                                .iter()
                                .map(|tx| {
                                    let (credit, debit) = match tx.delta_cents.cmp(&0) {
                                        std::cmp::Ordering::Greater => (format_cents(tx.delta_cents), String::new()),
                                        std::cmp::Ordering::Less => (String::new(), format_cents(tx.delta_cents.saturating_abs())),
                                        std::cmp::Ordering::Equal => (String::new(), String::new()),
                                    };

                                    vec![
                                        tx.index.to_string(),
                                        format_cents(tx.delta_cents),
                                        credit,
                                        debit,
                                        format_cents(tx.balance_cents),
                                        tx.note.as_deref().unwrap_or("").to_string(),
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
                tx |> enriched;
                enriched |> checkbook;
                checkbook |> printer;
            }
        })
        .await?;

    Ok(())
}
