// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HN AI Digest Demo — showcases Rig-backed LLM transforms.
//!
//! Pipeline (map-reduce):
//! `HttpPullSource` → format stories → `ai!` digest → configured sink
//!
//! The `digest` stage is one map-reduce `ai!` stage: each group of at most
//! `group_max_stories` stories gets one LLM call from the map role, and at the end of input
//! the reduce role folds the group summaries into the final digest with one more call.
//!
//! Tutorials: `https://obzenflow.dev/tutorials/`
//!
//! Run (default: local mock HN server + Ollama; requires Ollama running):
//! `cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai postgres" -- --config examples/hn_ai_digest_demo/obzenflow.toml`
//!
//! Ollama quickstart (macOS):
//! - Install: `brew install ollama`
//! - Start the server: `ollama serve` (or open the Ollama desktop app)
//! - Pull the default model: `ollama pull llama3.1:8b`
//!
//! Provider preflight:
//! This example uses lazy provider construction by default. Local configuration errors are
//! surfaced at startup, but network/model availability errors will surface on the first LLM call.
//!
//! Third-party terms note:
//! ObzenFlow only provides a client-side integration (via `rig-core`). It does not redistribute
//! Ollama, model weights, or hosted LLM services. You are responsible for complying with any
//! third-party licenses/terms (including model weight licenses and hosted-provider ToS/usage limits).
//! When using a hosted provider, your prompts and story text will be sent to that provider.
//!
//! Digest output configuration:
//! - `obzenflow.toml` selects `console_sink`.
//! - `obzenflow.postgres.toml` selects `postgres_sink`.
//! - PostgreSQL output reads `OBZENFLOW_POSTGRES_URL`, optionally
//!   `OBZENFLOW_POSTGRES_SCHEMA` (default `obzenflow_example`), and optionally
//!   `OBZENFLOW_POSTGRES_TRANSPORT` (default `verified-tls`).
//! - Final Markdown uses verified `[^n]` story footnotes and appends a `### Links`
//!   section from the fetched titles, article URLs, and Hacker News discussion URLs.
//! - Any externally managed PostgreSQL service may supply those values directly; no `xtask`
//!   process is part of the application contract.
//! - For the optional repository service, run `cargo xtask postgres up`, inspect its current
//!   local connection settings with `cargo xtask postgres connection`, then run PostgreSQL output with
//!   `cargo xtask postgres run -- cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai postgres" -- --config examples/hn_ai_digest_demo/obzenflow.postgres.toml`.
//!
//! AI model configuration:
//! - `[ai.models]` in the selected TOML file picks the endpoint and model; another endpoint
//!   is defined once under `[ai.endpoints.<name>]` with its provider, URL and credential name.
//! - The endpoint and model are checked during flow build. Secret lookup and client
//!   construction wait for the first live call; no provider or model preflight occurs.
//! - Strict replay uses the model the run recorded, reads no secret and constructs no client.

mod config;
mod decoder;
mod domain;
mod flow;
mod mock_server;
mod util;

fn main() -> anyhow::Result<()> {
    flow::run_demo_blocking()
}
