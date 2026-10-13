// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HN AI Digest Demo — showcases Rig-backed LLM transforms.
//!
//! Pipeline (batch + map-reduce):
//! `HttpPullSource` → format stories → accumulate into one batch → `ai_map_reduce!` digest → configured sink
//!
//! The `digest` stage is a single `ai_map_reduce!`: a `by_budget` chunking policy splits the batch
//! into token-budgeted chunks, the `map` arm summarizes each chunk with one LLM call, and the
//! `reduce` arm folds the chunk summaries into the final digest with one more LLM call.
//!
//! Oversize handling: if a single item is larger than the whole per-group budget, the chunker's
//! `oversize: decompose { max_depth, exhaustion }` policy re-renders it at increasing decomposition
//! depths. On exhaustion it either fails the plan or drops and records the item; it never silently
//! truncates. For HN headlines this never fires in practice.
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
//! Run against the real HN Firebase API (requires network) by setting `source = "live"`
//! under `[app]` in a copy of the config and passing the copy with `--config`.
//!
//! Application settings (`[app]` in the selected TOML file; the run prints them at startup):
//! - `source = "mock"` (default) or `"live"`
//! - `max_stories = 60` (default 60)
//! - `poll_timeout_secs = 120` (default 120)
//! - The HN source's pacing (default 10.0 events/sec) is ordinary middleware configuration:
//!   `events_per_second` under `[middleware.stages.hn_stories.rate_limiter]`.
//! - The HN HTTP source also has a fixed source circuit breaker: 3 failures, 2s cooldown.
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
//! AI target configuration:
//! - `[ai.models]` in the selected TOML file supplies provider, model, optional endpoint, and
//!   the credential-reference name.
//! - Local binding shape is validated during flow build. Secret lookup and unchecked client
//!   construction are deferred until the first executable live effect; no provider/model
//!   preflight occurs.
//! - Strict replay resolves the same non-secret target but reads no secret and constructs no
//!   client.
//!
//! Application settings (AI prompt/chunk policy):
//! - `interests = "rust, ai, security"` (optional personalization). It is part of every chat
//!   effect's identity, so replaying or resuming a run with different interests refuses.
//! - `group_budget_tokens = 2500` under `[app.digest]` (optional; per-chunk input budget used for
//!   map-reduce splitting)
//! - `group_max_stories = 10` under `[app.digest]` (cap stories per chunk; set `0` for unlimited)

mod config;
mod decoder;
mod domain;
mod flow;
mod mock_server;
mod util;

fn main() -> anyhow::Result<()> {
    flow::run_demo_blocking()
}
