// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HN AI Digest Demo — showcases Rig-backed LLM transforms.
//!
//! Pipeline (batch + map-reduce):
//! `HttpPullSource` → format stories → accumulate into one batch → `ai_map_reduce!` digest → print markdown digest
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
//! `cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai" -- --config examples/hn_ai_digest_demo/obzenflow.toml`
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
//! Run against the real HN Firebase API (requires network):
//! `HN_LIVE=1 cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai"`
//!
//! Optional env vars (HN fetch):
//! - `HN_MAX_STORIES=60` (default 60)
//! - `HN_LIVE=1` (default 0)
//! - `HN_POLL_TIMEOUT_SECS=120` (default 120)
//! - `HN_SOURCE_RATE_LIMIT=10.0` (default 10.0 events/sec)
//! - The HN HTTP source also has a fixed source circuit breaker: 3 failures, 2s cooldown.
//!
//! AI target configuration:
//! - `[ai.models]` in `obzenflow.toml` supplies provider, model, optional endpoint, and
//!   the credential-reference name.
//! - Local binding shape is validated during flow build. Secret lookup and unchecked client
//!   construction are deferred until the first executable live effect; no provider/model
//!   preflight occurs.
//! - Strict replay resolves the same non-secret target but reads no secret and constructs no
//!   client.
//!
//! Optional env vars (AI prompt/chunk policy):
//! - `HN_AI_INTERESTS="rust, ai, security"` (optional personalization)
//! - `HN_AI_GROUP_BUDGET_TOKENS=2500` (optional; per-chunk input budget used for map-reduce splitting)
//! - `HN_AI_GROUP_MAX_STORIES=10` (optional; cap stories per chunk; set `0` for unlimited)

mod config;
mod decoder;
mod domain;
mod flow;
mod mock_server;
mod util;

fn main() -> anyhow::Result<()> {
    flow::run_demo_blocking()
}
