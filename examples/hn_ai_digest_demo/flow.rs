// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::decoder::HnStoryDecoder;
use super::domain::{FormattedStory, HnStory};
use super::mock_server::spawn_mock_hn_server;
use super::util::{env_bool, env_usize, truncate_chars};
use anyhow::{anyhow, Result};
use obzenflow::sinks::ConsoleSink;
use obzenflow::sources::{HeaderMap, HttpPullConfig, HttpPullSource, Url};
use obzenflow_adapters::ai::ChatTransform;
use obzenflow_core::ai::{ChatMessage, ChatParams, ChatRequest, ChatRole};
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_dsl_infra::{async_source, async_transform, flow, sink, stateful, transform};
use obzenflow_infra::ai::rig::RigChatClient;
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::http_client::default_http_client;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::stateful::strategies::accumulators::ReduceTyped;
use obzenflow_runtime_services::stages::transform::TryMapWith;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HnTopStories {
    stories: Vec<FormattedStory>,
}

impl TypedPayload for HnTopStories {
    const EVENT_TYPE: &'static str = "hn.top_stories";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnDigestSummary {
    mode: String,
    base_url: String,
    ai_provider: String,
    ai_model: String,
    stories_fetched: usize,
    stories_used: usize,
    prompt_story_limit: usize,
    interests: Option<String>,
    chat_prompt_system: String,
    chat_prompt_user: String,
    input: HnTopStories,
    output_markdown: String,
}

impl TypedPayload for HnDigestSummary {
    const EVENT_TYPE: &'static str = "hn.digest_summary";
}

pub fn run_example() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow!("failed to create tokio runtime: {e:?}"))?;

    runtime.block_on(run_example_async())
}

async fn run_example_async() -> Result<()> {
    if std::env::var("OBZENFLOW_METRICS_EXPORTER").is_err() {
        std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");
    }

    let max_stories = env_usize("HN_MAX_STORIES").unwrap_or(30);
    let poll_timeout_secs = env_usize("HN_POLL_TIMEOUT_SECS").unwrap_or(120);
    let live = env_bool("HN_LIVE").unwrap_or(false);

    let mut _mock_server = None;
    let (base_url, mode_label) = if live {
        (
            Url::parse("https://hacker-news.firebaseio.com/")
                .map_err(|e| anyhow!("invalid HN base URL: {e}"))?,
            "live",
        )
    } else {
        let server = spawn_mock_hn_server().await?;
        let base_url = server.base_url();
        _mock_server = Some(server);
        (base_url, "mock")
    };

    let base_url_for_summary = base_url.to_string();
    let mode_label_for_summary = mode_label.to_string();

    let (rig_client, ai_provider_label, ai_model_label) = build_rig_chat_client_from_env().await?;

    let prompt_story_limit =
        env_usize("HN_AI_PROMPT_STORIES").unwrap_or_else(|| match ai_provider_label.as_str() {
            "ollama" => max_stories.min(15),
            _ => max_stories,
        });
    let prompt_story_limit = prompt_story_limit.max(1);

    println!("HN AI Digest Demo (FLOWIP-086r)");
    println!("===============================");
    println!(
        "Fetch top HN stories, then generate a markdown digest via Rig-backed LLM transforms."
    );
    println!();
    println!("  mode: {mode_label}");
    println!("  base_url: {base_url}");
    println!("  max_stories: {max_stories}");
    println!("  poll_timeout: {poll_timeout_secs}s");
    println!("  ai_provider: {ai_provider_label}");
    println!("  ai_model: {ai_model_label}");
    println!();

    let decoder = HnStoryDecoder::new(base_url, max_stories);
    let client = default_http_client().map_err(|e| anyhow!("HTTP client unavailable: {e}"))?;

    let config = HttpPullConfig {
        client,
        default_headers: HeaderMap::new(),
        max_batch_size: 10,
        retry: Default::default(),
    };

    let formatter = TryMapWith::new(format_story_event)
        .on_error_with(|event, err| Some(event.mark_as_error(err, ErrorKind::Deserialization)));

    let digest_seed = ReduceTyped::new(
        HnTopStories::default(),
        |acc: &mut HnTopStories, story: &FormattedStory| {
            acc.stories.push(story.clone());
        },
    )
    .emit_on_eof();

    let interests = std::env::var("HN_AI_INTERESTS").ok();
    let interests_for_request = interests.clone();
    let interests_for_summary = interests.clone();

    let ai_provider = rig_client.provider().clone();
    let ai_model = rig_client.model().to_string();

    let ai_provider_for_summary = ai_provider.as_str().to_string();
    let ai_model_for_summary = ai_model.clone();

    let system_prompt = "You write concise, skimmable Hacker News digests from a list of headlines + URLs. Be neutral, avoid hype, and do not invent facts beyond what the titles imply."
        .to_string();
    let system_prompt_for_request = system_prompt.clone();
    let system_prompt_for_summary = system_prompt.clone();

    let llm = ChatTransform::new(Arc::new(rig_client), move |event: &ChainEvent| {
        let mut seed = HnTopStories::try_from_event(event).map_err(|err| {
            HandlerError::Validation(format!("hn digest seed decode failed: {err}"))
        })?;

        seed.stories.truncate(prompt_story_limit);

        let prompt = build_digest_prompt(&seed, interests_for_request.as_deref());

        Ok(ChatRequest {
            provider: ai_provider.clone(),
            model: ai_model.clone(),
            messages: vec![
                ChatMessage {
                    role: ChatRole::system(),
                    content: system_prompt_for_request.clone(),
                },
                ChatMessage {
                    role: ChatRole::user(),
                    content: prompt,
                },
            ],
            params: ChatParams {
                temperature: Some(0.2),
                max_tokens: Some(800),
                ..ChatParams::default()
            },
            tools: vec![],
            response_format: None,
        })
    })
    .with_output_mapper(move |input: &ChainEvent, response| {
        let mut seed = HnTopStories::try_from_event(input).map_err(|err| {
            HandlerError::Validation(format!("hn digest seed decode failed: {err}"))
        })?;

        let stories_fetched = seed.stories.len();
        seed.stories.truncate(prompt_story_limit);
        let stories_used = seed.stories.len();

        let user_prompt = build_digest_prompt(&seed, interests_for_summary.as_deref());

        let summary = HnDigestSummary {
            mode: mode_label_for_summary.clone(),
            base_url: base_url_for_summary.clone(),
            ai_provider: ai_provider_for_summary.clone(),
            ai_model: ai_model_for_summary.clone(),
            stories_fetched,
            stories_used,
            prompt_story_limit,
            interests: interests_for_summary.clone(),
            chat_prompt_system: system_prompt_for_summary.clone(),
            chat_prompt_user: user_prompt,
            input: seed,
            output_markdown: response.text,
        };

        let payload = serde_json::to_value(&summary).map_err(|err| {
            HandlerError::Validation(format!("digest summary payload encode failed: {err}"))
        })?;

        Ok(vec![ChainEventFactory::derived_data_event(
            input.writer_id,
            input,
            HnDigestSummary::versioned_event_type(),
            payload,
        )])
    });

    FlowApplication::builder()
        .with_console_subscriber()
        .with_log_level(LogLevel::Info)
        .run_async(flow! {
            name: "hn_ai_digest_demo",
            journals: disk_journals(std::path::PathBuf::from("target/hn-ai-digest-logs")),
            middleware: [],

            stages: {
                source = async_source!("hn_stories" => (
                    HttpPullSource::new(decoder, config),
                    Some(Duration::from_secs(poll_timeout_secs as u64))
                ));
                formatter = transform!("formatter" => formatter);
                batch = stateful!("batch" => digest_seed);
                digest = async_transform!("digest_llm" => llm);
                output = sink!("digest_summary" => ConsoleSink::<HnDigestSummary>::new(format_digest_summary_for_console));
            },

            topology: {
                source |> formatter;
                formatter |> batch;
                batch |> digest;
                digest |> output;
            }
        })
        .await?;

    println!();
    println!("Demo completed!");
    println!("Journal written to: target/hn-ai-digest-logs/");

    Ok(())
}

fn format_digest_summary_for_console(summary: &HnDigestSummary) -> String {
    let mut out = String::new();

    out.push_str("HN AI Digest — Summary\n");
    out.push_str("======================\n");
    out.push_str(&format!("mode: {}\n", summary.mode));
    out.push_str(&format!("base_url: {}\n", summary.base_url));
    out.push_str(&format!("ai_provider: {}\n", summary.ai_provider));
    out.push_str(&format!("ai_model: {}\n", summary.ai_model));
    out.push_str(&format!("stories_fetched: {}\n", summary.stories_fetched));
    out.push_str(&format!(
        "stories_used: {} (prompt limit: {})\n",
        summary.stories_used, summary.prompt_story_limit
    ));

    if let Some(interests) = &summary.interests {
        if !interests.trim().is_empty() {
            out.push_str(&format!("interests: {}\n", interests.trim()));
        }
    }

    out.push('\n');

    out.push_str("Chat prompt (system)\n");
    out.push_str("--------------------\n");
    out.push_str(summary.chat_prompt_system.trim());

    out.push_str("\n\nChat prompt (user)\n");
    out.push_str("------------------\n");
    out.push_str(summary.chat_prompt_user.trim());

    out.push_str("\n\nInput data (stories)\n");
    out.push_str("--------------------\n");
    for (idx, story) in summary.input.stories.iter().enumerate() {
        let title = truncate_chars(story.title.trim(), 140);
        let author = truncate_chars(story.author.trim(), 60);
        let url = truncate_chars(story.url.trim(), 160);
        out.push_str(&format!(
            "{n}. {title} ({points} points, {comments} comments) by {author}\n    {url}\n",
            n = idx + 1,
            points = story.points,
            comments = story.comments,
        ));
    }

    out.push_str("\nOutput (markdown)\n");
    out.push_str("-----------------\n");
    out.push_str(summary.output_markdown.trim());

    out
}

async fn build_rig_chat_client_from_env() -> Result<(RigChatClient, String, String)> {
    let provider = std::env::var("HN_AI_PROVIDER")
        .unwrap_or_else(|_| "ollama".to_string())
        .trim()
        .to_ascii_lowercase();

    match provider.as_str() {
        "ollama" => {
            let model = std::env::var("HN_AI_MODEL").unwrap_or_else(|_| "llama3.1:8b".to_string());
            let base_url = std::env::var("OLLAMA_BASE_URL")
                .ok()
                .map(|v| Url::parse(v.trim()))
                .transpose()
                .map_err(|e| anyhow!("invalid OLLAMA_BASE_URL: {e}"))?;

            let client = RigChatClient::ollama_checked(model.clone(), base_url)
                .await
                .map_err(|e| anyhow!("failed to create ollama client: {e}"))?;
            Ok((client, "ollama".to_string(), model))
        }
        "openai" => {
            let model = std::env::var("HN_AI_MODEL").unwrap_or_else(|_| "gpt-4.1-mini".to_string());
            let api_key = std::env::var("OPENAI_API_KEY")
                .map_err(|_| anyhow!("OPENAI_API_KEY is required when HN_AI_PROVIDER=openai"))?;

            let client = match std::env::var("OPENAI_BASE_URL").ok() {
                None => RigChatClient::openai_checked(model.clone(), api_key)
                    .await
                    .map_err(|e| anyhow!("failed to create openai client: {e}"))?,
                Some(base_url) => {
                    let base_url = Url::parse(base_url.trim())
                        .map_err(|e| anyhow!("invalid OPENAI_BASE_URL: {e}"))?;
                    RigChatClient::openai_compatible_checked(model.clone(), api_key, base_url)
                        .await
                        .map_err(|e| anyhow!("failed to create openai-compatible client: {e}"))?
                }
            };

            Ok((client, "openai".to_string(), model))
        }
        other => Err(anyhow!(
            "unsupported HN_AI_PROVIDER='{other}' (expected 'ollama' or 'openai')"
        )),
    }
}

fn build_digest_prompt(seed: &HnTopStories, interests: Option<&str>) -> String {
    let mut out = String::new();

    if let Some(interests) = interests {
        out.push_str("My interests: ");
        out.push_str(interests.trim());
        out.push_str("\n\n");
    }

    let story_count = seed.stories.len();
    out.push_str(
        "Write a Markdown digest of the following Hacker News stories (headlines + URLs only).\n",
    );
    out.push_str("- Do not invent facts that are not implied by the title.\n");
    out.push_str("- Use a neutral, specific tone (avoid filler adjectives like \"shocking\", \"interesting\", \"concerning\").\n");
    out.push_str(
        "- Start the response immediately with \"## Stories\" (no intro/preamble text).\n",
    );
    out.push_str("- Include *every* story exactly once, in the same order, using the provided URLs verbatim.\n\n");

    out.push_str("Output format (follow exactly):\n\n");
    out.push_str("## Stories\n");
    out.push_str(&format!(
        "1..{story_count}. [Title](URL) — 1–2 sentence summary (≤35 words)\n\n"
    ));
    out.push_str("## What's topical today\n");
    out.push_str("Thesis: 1 sentence that connects 2+ themes, citing story numbers like (2, 7).\n");
    out.push_str("Themes:\n");
    out.push_str("- <theme> (stories: 1, 3, 9): 1 sentence with specific nouns from the titles\n");
    out.push_str("- <theme> (stories: ...): ...\n");
    out.push_str("Watch: 1 sentence on what to pay attention to next.\n\n");
    out.push_str("Avoid generic wrap-ups (do not say: \"range of topics\", \"variety of topics\", \"from X to Y\").\n\n");

    out.push_str("Stories (numbered; use these URLs verbatim):\n");
    for (idx, story) in seed.stories.iter().enumerate() {
        let title = truncate_chars(story.title.trim(), 140);
        let url = story.url.trim();
        out.push_str(&format!("{n}. {title} — {url}\n", n = idx + 1));
    }

    out
}

fn format_story_event(event: ChainEvent) -> std::result::Result<ChainEvent, String> {
    if !HnStory::event_type_matches(&event.event_type()) {
        return Ok(event);
    }

    let story = HnStory::try_from_event(&event).map_err(|e| e.to_string())?;
    let formatted = FormattedStory {
        id: story.id,
        title: story
            .title
            .unwrap_or_else(|| "(untitled)".to_string())
            .trim()
            .to_string(),
        url: story
            .url
            .unwrap_or_else(|| format!("https://news.ycombinator.com/item?id={}", story.id)),
        author: story.by.unwrap_or_else(|| "(anonymous)".to_string()),
        points: story.score.unwrap_or(0),
        comments: story.descendants.unwrap_or(0),
    };

    let payload = serde_json::to_value(&formatted).map_err(|e| e.to_string())?;
    Ok(ChainEventFactory::derived_data_event(
        event.writer_id,
        &event,
        FormattedStory::versioned_event_type(),
        payload,
    ))
}
