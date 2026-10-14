// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HTTP Ingestion Piggy Bank Demo
//!
//! A curl-friendly end-to-end demo that fuses batch and live data:
//! - Batch: accounts read once from `accounts.yaml`, and ledger entries read
//!   from `transactions.csv`, paced at about two rows a second
//! - Live: accounts and ledger entries posted over HTTP ingress
//! - `all_accounts`: every account, batch or live, in one catalog
//! - `posted_batch`: a hydrating join that posts each CSV entry after every
//!   YAML account has loaded
//! - `posted_live`: a live join that posts each HTTP entry against
//!   `all_accounts`
//! - `checkbook`: one stateful fold over both joins' postings, printed by a
//!   console sink
//!
//! `all_accounts`, `posted_live` and `checkbook` each have a live root, so they
//! record their delivery order in their own journals and never wait on a quiet
//! input.
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
//! `accounts.yaml`, and the entries in `transactions.csv` post against them.
//! Post live data once the checkbook has printed the last CSV entry. Open an
//! account, wait for its `202`, then post to it:
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/accounts/events -H 'content-type: application/json' -H 'Idempotency-Key: "acct-0003"' -d '{"event_type":"bank.account_opened","data":{"account_id":"acct-3","owner":"Carol","initial_balance_cents":0}}'`
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/tx/events -H 'content-type: application/json' -H 'Idempotency-Key: "live-0001"' -d '{"event_type":"bank.ledger_entry","data":{"account_id":"acct-3","kind":"Credit","amount_cents":700,"note":"first deposit"}}'`
//!    `curl -XPOST http://127.0.0.1:9090/api/bank/tx/events -H 'content-type: application/json' -H 'Idempotency-Key: "live-0002"' -d '{"event_type":"bank.ledger_entry","data":{"account_id":"acct-1","kind":"Debit","amount_cents":99,"note":"coffee"}}'`
//!
//! Notes:
//! - A live join drops an entry whose account has not reached `all_accounts`.
//!   Wait for the account's `202` before posting to it.
//! - Entries for unknown accounts are dropped by the joins.
//! - The CSV source rejects a zero-amount or malformed row. Ingress answers `400`
//!   to a zero-amount or malformed entry and a negative opening balance, and the
//!   system journal records the refusal with the field it names.
//! - Strict replay (`-- --replay-from <run-dir>`) reuses the archived batch and
//!   live data and never reopens either file; both ingresses answer
//!   `503 not_ready` throughout.
//! - Resume (`-- --resume-from <run-dir> --allow-incomplete-archive`) continues
//!   the CSV after its last committed row; resent live requests answer
//!   `duplicate` with their original event ids.
//! - Transaction ingress has its own rate limiter.
//! - The stateful stage emits a `bank.checkbook` snapshot for every posted entry.
//! - A `202` names each entry's committed row (`outcome` `committed`). Resending the
//!   same entry with the same `Idempotency-Key` answers `duplicate` with the original
//!   `event_id`; other content under that key answers `422`. A `503 outcome_unknown`
//!   means retry the identical request with the same key.
//! - Ingress POSTs above stay unauthenticated in this example. Control-plane auth protects built-ins such as `/metrics`, `/api/topology`, and `/api/flow/*`.
//! - With control-plane auth enabled, configure the client's `Authorization` header
//!   through its protected credential configuration before querying `/metrics` or
//!   `/api/topology`. Keep credential values out of command arguments and shell history.

mod support;

#[cfg(test)]
fn main() -> anyhow::Result<()> {
    support::runner::run_example_in_tests()
}

#[cfg(not(test))]
fn main() -> anyhow::Result<()> {
    support::runner::run_example()
}
