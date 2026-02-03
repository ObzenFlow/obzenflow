// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HN Ingestion Demo (FLOWIP-084f) — validates the HTTP pull source (FLOWIP-084e).
//!
//! Demonstrates the canonical `source → transform → sink` pattern:
//! - `HttpPullSource<HnStoryDecoder>` pulling JSON from an HTTP API
//! - A small transform that formats stories for display
//! - `ConsoleSink` output
//!
//! Run (default: local mock server):
//! `cargo run -p obzenflow --example hn_ingestion_demo --features obzenflow_infra/reqwest-client`
//!
//! Run against the real HN Firebase API (requires network):
//! `HN_LIVE=1 cargo run -p obzenflow --example hn_ingestion_demo --features obzenflow_infra/reqwest-client`
//!
//! Optional env vars:
//! - `HN_MAX_STORIES=30` (default 30)
//! - `HN_LIVE=1` (default 0)
//! - `HN_POLL_TIMEOUT_SECS=120` (default 120)

mod support;

fn main() -> anyhow::Result<()> {
    support::flow::run_example()
}
