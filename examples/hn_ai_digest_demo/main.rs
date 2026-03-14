// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HN AI Digest Demo (FLOWIP-086r) — showcases Rig-backed LLM transforms.
//!
//! Pipeline (batch + map-reduce):
//! `HttpPullSource` → format stories → accumulate → split_to_budget → map (LLM) → reduce → digest (LLM) → print markdown digest
//!
//! Oversize decomposition loop (FLOWIP-086x):
//! `split_to_budget` emits oversize chunks when a single item exceeds the per-group token budget.
//! These route through `oversize_sub_split`, which may emit another oversize chunk (with a higher
//! decomposition depth) back into itself via `<|` until it can emit regular chunks.
//! Those chunks are summarized by `oversize_map_llm` (LLM) and converted into summaries for reduction.
//!
//! Tutorials: `https://obzenflow.dev/tutorials/`
//!
//! Run (default: local mock HN server + Ollama; requires Ollama running):
//! `cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai"`
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
//! - `HN_MAX_STORIES=30` (default 30)
//! - `HN_LIVE=1` (default 0)
//! - `HN_POLL_TIMEOUT_SECS=120` (default 120)
//! - `HN_SOURCE_RATE_LIMIT=10.0` (default 10.0 events/sec)
//!
//! Optional env vars (AI):
//! - `HN_AI_PROVIDER=ollama|openai|openai_compatible` (default `ollama`)
//! - `HN_AI_MODEL=llama3.1:8b` (default depends on provider)
//! - `HN_AI_INTERESTS="rust, ai, security"` (optional personalization)
//! - `HN_AI_GROUP_BUDGET_TOKENS=2500` (optional; per-chunk input budget used for map-reduce splitting)
//! - `HN_AI_GROUP_MAX_STORIES=10` (optional; cap stories per chunk; set `0` for unlimited)
//! - `OLLAMA_BASE_URL=http://localhost:11434` (optional; default rig provider base)
//! - `OPENAI_API_KEY=...` (required for `HN_AI_PROVIDER=openai` and `HN_AI_PROVIDER=openai_compatible`)
//! - `OPENAI_BASE_URL=http://localhost:8080/v1` (required for `HN_AI_PROVIDER=openai_compatible`)

mod support;

use obzenflow_infra::application::{Banner, Presentation, RunPresentationOutcome};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::var("OBZENFLOW_METRICS_EXPORTER").is_err() {
        std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");
    }

    let config = support::flow::DemoConfig::from_env().await?;

    let presentation = Presentation::new(
        Banner::new("HN AI Digest Demo")
            .description(
                "Fetch top HN stories, then generate a markdown digest via Rig-backed LLM transforms.",
            )
            .config("mode", &config.mode_label)
            .config("base_url", config.base_url.to_string())
            .config("max_stories", config.max_stories)
            .config("poll_timeout", format!("{}s", config.poll_timeout_secs))
            .config_block(&config.ai)
            .config("group_budget_tokens", config.budget_per_group_tokens)
            .config("group_max_stories", config.group_max_stories_label())
            .config("source_rate_limit", format!("{} events/sec", config.source_rate_limit)),
    )
    .with_footer(|outcome| {
        let is_success = matches!(&outcome, RunPresentationOutcome::Completed { .. });
        let mut out = outcome.default_footer();
        if is_success {
            out.push_str(
                "\n\nThe generated digest was printed above.\
                 \nRe-run with HN_LIVE=1 to fetch from the real Hacker News API.",
            );
        }
        out
    });

    support::flow::run_example(config, presentation).await
}
