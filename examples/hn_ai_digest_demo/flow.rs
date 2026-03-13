// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::decoder::HnStoryDecoder;
use super::domain::{FormattedStory, HnStory};
use super::mock_server::spawn_mock_hn_server;
use super::util::{env_bool, env_usize, truncate_chars};
use anyhow::{anyhow, Result};
use obzenflow::ai::{
    resolve_estimator_for_model, ChatTransform, ChatTransformExt, ChunkEnvelope, EstimateSource,
    OversizeExhaustion, OversizePolicy, TokenCount,
};
use obzenflow::sources::{HeaderMap, HttpPullConfig, HttpPullSource, Url};
use obzenflow::typed::{
    ai as typed_ai, sinks, stateful as typed_stateful, transforms as typed_transforms,
};
use obzenflow_adapters::middleware::control::ai_circuit_breaker;
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_dsl::{async_source, flow, sink, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::http_client::default_http_client;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const DEFAULT_HN_SOURCE_RATE_LIMIT: f64 = 10.0;

fn env_f64(key: &str) -> Option<f64> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

type StoryChunkEnvelope = ChunkEnvelope<FormattedStory>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HnTopStories {
    stories: Vec<FormattedStory>,
}

impl TypedPayload for HnTopStories {
    const EVENT_TYPE: &'static str = "hn.top_stories";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnDigestChunkSummary {
    group_index: usize,
    group_count: usize,
    budget_per_group: TokenCount,
    estimated_tokens: TokenCount,
    oversize: bool,
    decomposition_depth: u32,
    story_numbers: Vec<usize>,
    stories: Vec<FormattedStory>,
    chat_prompt_user: String,
    output_markdown: String,
}

impl TypedPayload for HnDigestChunkSummary {
    const EVENT_TYPE: &'static str = "hn.digest_chunk_summary";
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HnDigestChunkSummaries {
    budget_per_group: TokenCount,
    input_items_total: usize,
    planned_items_total: usize,
    excluded_items_total: usize,
    summaries: Vec<HnDigestChunkSummary>,
}

impl TypedPayload for HnDigestChunkSummaries {
    const EVENT_TYPE: &'static str = "hn.digest_chunk_summaries";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnDigestSummary {
    mode: String,
    base_url: String,
    ai_provider: String,
    ai_model: String,
    token_estimator: EstimateSource,
    stories_fetched: usize,
    budget_per_group: TokenCount,
    groups: usize,
    interests: Option<String>,
    chat_prompt_system: String,
    chat_prompt_user: String,
    input: HnTopStories,
    chunk_summaries: Vec<HnDigestChunkSummary>,
    output_markdown: String,
}

impl TypedPayload for HnDigestSummary {
    const EVENT_TYPE: &'static str = "hn.digest_summary";
}

pub async fn run_example() -> Result<()> {
    if std::env::var("OBZENFLOW_METRICS_EXPORTER").is_err() {
        std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");
    }

    let max_stories = env_usize("HN_MAX_STORIES").unwrap_or(30);
    let poll_timeout_secs = env_usize("HN_POLL_TIMEOUT_SECS").unwrap_or(120);
    let live = env_bool("HN_LIVE").unwrap_or(false);
    let source_rate_limit = env_f64("HN_SOURCE_RATE_LIMIT").unwrap_or(DEFAULT_HN_SOURCE_RATE_LIMIT);
    if source_rate_limit <= 0.0 {
        return Err(anyhow!("HN_SOURCE_RATE_LIMIT must be greater than zero"));
    }

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

    let ai_provider_label = std::env::var("HN_AI_PROVIDER")
        .unwrap_or_else(|_| "ollama".to_string())
        .trim()
        .to_ascii_lowercase();

    let ai_provider_label = match ai_provider_label.as_str() {
        "ollama" => "ollama".to_string(),
        "openai" => "openai".to_string(),
        other => {
            return Err(anyhow!(
                "unsupported HN_AI_PROVIDER='{other}' (expected 'ollama' or 'openai')"
            ))
        }
    };

    let ai_model_label = match ai_provider_label.as_str() {
        "ollama" => std::env::var("HN_AI_MODEL").unwrap_or_else(|_| "llama3.1:8b".to_string()),
        "openai" => std::env::var("HN_AI_MODEL").unwrap_or_else(|_| "gpt-4.1-mini".to_string()),
        _ => unreachable!("provider label validated above"),
    };

    let budget_per_group_tokens =
        env_usize("HN_AI_GROUP_BUDGET_TOKENS").unwrap_or(match ai_provider_label.as_str() {
            "ollama" => 2500,
            _ => 6000,
        }) as u64;
    if budget_per_group_tokens == 0 {
        return Err(anyhow!(
            "HN_AI_GROUP_BUDGET_TOKENS must be greater than zero"
        ));
    }
    let budget_per_group = TokenCount::new(budget_per_group_tokens);

    let max_stories_per_group_raw = env_usize("HN_AI_GROUP_MAX_STORIES").unwrap_or(10);
    let max_stories_per_group = match max_stories_per_group_raw {
        0 => None,
        n => Some(n),
    };

    let estimator_resolution = resolve_estimator_for_model(&ai_model_label);
    let estimator = estimator_resolution.estimator();

    println!("HN AI Digest Demo (FLOWIP-086z)");
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
    println!("  token_estimator: {:?}", estimator_resolution.source());
    if let Some(tokenizer_backend) = estimator_resolution.info().tokenizer_backend.as_deref() {
        println!("  token_estimator_backend: {tokenizer_backend}");
    }
    if let Some(fallback_reason) = estimator_resolution.info().fallback_reason.as_ref() {
        println!("  token_estimator_fallback_reason: {fallback_reason}");
    }
    if let Some(fallback_detail) = estimator_resolution.info().fallback_detail.as_deref() {
        println!("  token_estimator_fallback_detail: {fallback_detail}");
    }
    println!("  group_budget_tokens: {budget_per_group_tokens}");
    match max_stories_per_group {
        None => println!("  group_max_stories: unlimited"),
        Some(v) => println!("  group_max_stories: {v}"),
    }
    println!("  source_rate_limit: {source_rate_limit} events/sec");
    println!();

    let decoder = HnStoryDecoder::new(base_url, max_stories);
    let client = default_http_client().map_err(|e| anyhow!("HTTP client unavailable: {e}"))?;

    let config = HttpPullConfig {
        client,
        default_headers: HeaderMap::new(),
        max_batch_size: 10,
        retry: Default::default(),
    };

    let formatter =
        typed_transforms::try_map_with(|story: HnStory| Ok(format_story(story))).on_error_drop();

    let digest_seed =
        typed_stateful::reduce(HnTopStories::default(), |acc, story: &FormattedStory| {
            acc.stories.push(story.clone())
        })
        .emit_on_eof();

    let interests = std::env::var("HN_AI_INTERESTS").ok();
    let interests_for_map_request = interests.clone();
    let interests_for_map_summary = interests.clone();
    let interests_for_reduce_request = interests.clone();
    let interests_for_reduce_summary = interests.clone();

    let ai_provider_for_summary = ai_provider_label.clone();
    let ai_model_for_summary = ai_model_label.clone();
    let token_estimator_for_summary = estimator.source();

    let system_prompt = "You write concise, skimmable Hacker News digests from a list of headlines + URLs. Be neutral, avoid hype, and do not invent facts beyond what the titles imply."
        .to_string();
    let system_prompt_for_summary = system_prompt.clone();

    let openai_api_key = match ai_provider_label.as_str() {
        "openai" => Some(
            std::env::var("OPENAI_API_KEY")
                .map_err(|_| anyhow!("OPENAI_API_KEY is required when HN_AI_PROVIDER=openai"))?,
        ),
        _ => None,
    };

    let openai_base_url = std::env::var("OPENAI_BASE_URL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    let ollama_base_url = std::env::var("OLLAMA_BASE_URL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    let make_llm_builder = || match ai_provider_label.as_str() {
        "ollama" => {
            let mut builder = ChatTransform::builder().ollama(ai_model_label.clone());
            if let Some(base_url) = &ollama_base_url {
                builder = builder.base_url(base_url.clone());
            }
            builder
        }
        "openai" => {
            let api_key = openai_api_key
                .clone()
                .expect("api key must exist for openai");
            match openai_base_url.as_deref() {
                None => ChatTransform::builder().openai(ai_model_label.clone(), api_key),
                Some(base_url) => ChatTransform::builder().openai_compatible(
                    ai_model_label.clone(),
                    api_key,
                    base_url.to_string(),
                ),
            }
        }
        _ => unreachable!("provider label validated above"),
    };

    let chunker = typed_ai::chunk_by_budget::<HnTopStories, FormattedStory>()
        .estimator(estimator.clone())
        .items(|seed: &HnTopStories| seed.stories.clone())
        .render(|story: &FormattedStory, ctx| {
            render_story_line(ctx.item_ordinal + 1, story, ctx.decomposition_depth)
        })
        .budget(budget_per_group)
        .max_items_per_chunk(max_stories_per_group)
        .oversize(OversizePolicy::Rerender {
            max_depth: 5,
            min_progress_tokens: TokenCount::new(1),
            exhaustion: OversizeExhaustion::Fail,
        })
        .snapshot_excluded_items_limit(25)
        .build();

    let map_llm_handler = make_llm_builder()
        .system(system_prompt.clone())
        .temperature(0.2)
        .max_tokens(800)
        .output_mapper(move |input: &ChainEvent, response| {
            let envelope = StoryChunkEnvelope::try_from_event(input).map_err(|err| {
                HandlerError::Validation(format!("hn digest chunk envelope decode failed: {err}"))
            })?;
            debug_assert_eq!(envelope.item_ordinals.len(), envelope.items.len());
            let story_numbers = envelope
                .item_ordinals
                .iter()
                .map(|ordinal| *ordinal + 1)
                .collect::<Vec<_>>();

            let user_prompt =
                build_chunk_prompt(&envelope, interests_for_map_summary.as_deref());

            let summary = HnDigestChunkSummary {
                group_index: envelope.chunk_index,
                group_count: envelope.chunk_count,
                budget_per_group,
                estimated_tokens: envelope.estimated_tokens,
                oversize: envelope.decomposition_depth > 0,
                decomposition_depth: envelope.decomposition_depth,
                story_numbers,
                stories: envelope.items.clone(),
                chat_prompt_user: user_prompt,
                output_markdown: strip_accidental_story_echo(&response.text),
            };

            let payload = serde_json::to_value(&summary).map_err(|err| {
                HandlerError::Validation(format!("chunk summary payload encode failed: {err}"))
            })?;

            Ok(vec![ChainEventFactory::derived_data_event(
                input.writer_id,
                input,
                HnDigestChunkSummary::versioned_event_type(),
                payload,
            )])
        })
        .build(move |event: &ChainEvent| {
            let envelope = StoryChunkEnvelope::try_from_event(event).map_err(|err| {
                HandlerError::Validation(format!("hn digest chunk envelope decode failed: {err}"))
            })?;

            Ok(build_chunk_prompt(&envelope, interests_for_map_request.as_deref()))
        })
        .await?
        .with_resolved_estimator(estimator_resolution.clone());

    let digest_llm_handler = make_llm_builder()
        .system(system_prompt)
        .temperature(0.2)
        .max_tokens(800)
        .output_mapper(move |input: &ChainEvent, response| {
            let mut batch = HnDigestChunkSummaries::try_from_event(input).map_err(|err| {
                HandlerError::Validation(format!("hn digest chunk summaries decode failed: {err}"))
            })?;

            batch.summaries.sort_by_key(|s| {
                (
                    s.group_index,
                    s.story_numbers.first().copied().unwrap_or(usize::MAX),
                )
            });
            let groups = batch.summaries.len();

            let mut numbered_stories: Vec<(usize, FormattedStory)> = Vec::new();
            for summary in &batch.summaries {
                for (n, story) in summary.story_numbers.iter().zip(summary.stories.iter()) {
                    numbered_stories.push((*n, story.clone()));
                }
            }
            numbered_stories.sort_by_key(|(n, _)| *n);
            let seed = HnTopStories {
                stories: numbered_stories
                    .into_iter()
                    .map(|(_, story)| story)
                    .collect(),
            };

            let stories_fetched = seed.stories.len();
            let user_prompt = build_reduce_prompt(&batch, interests_for_reduce_summary.as_deref());

            let summary = HnDigestSummary {
                mode: mode_label_for_summary.clone(),
                base_url: base_url_for_summary.clone(),
                ai_provider: ai_provider_for_summary.clone(),
                ai_model: ai_model_for_summary.clone(),
                token_estimator: token_estimator_for_summary,
                stories_fetched,
                budget_per_group: batch.budget_per_group,
                groups,
                interests: interests_for_reduce_summary.clone(),
                chat_prompt_system: system_prompt_for_summary.clone(),
                chat_prompt_user: user_prompt,
                input: seed,
                chunk_summaries: batch.summaries.clone(),
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
        })
        .build(move |event: &ChainEvent| {
            let batch = HnDigestChunkSummaries::try_from_event(event).map_err(|err| {
                HandlerError::Validation(format!("hn digest chunk summaries decode failed: {err}"))
            })?;

            Ok(build_reduce_prompt(
                &batch,
                interests_for_reduce_request.as_deref(),
            ))
        })
        .await?
        .with_resolved_estimator(estimator_resolution.clone());

    FlowApplication::run(flow! {
            name: "hn_ai_digest_demo",
            journals: disk_journals(std::path::PathBuf::from("target/hn-ai-digest-logs")),
            middleware: [],

            stages: {
                hn_stories = async_source!(HnStory => (
                    HttpPullSource::new(decoder, config),
                    Some(Duration::from_secs(poll_timeout_secs as u64))
                ), [
                    RateLimiterBuilder::new(source_rate_limit).build()
                ]);
                formatter = transform!(HnStory -> FormattedStory => formatter);
                batch = stateful!(FormattedStory -> HnTopStories => digest_seed);
                digest = typed_ai::map_reduce::<
                    HnTopStories,
                    StoryChunkEnvelope,
                    HnDigestChunkSummary,
                    HnDigestChunkSummaries,
                    HnDigestSummary
                >()
                .chunker(chunker)
                .map(map_llm_handler)
                .collect(
                    typed_ai::collect_by_input(
                        HnDigestChunkSummaries::default(),
                        |acc, summary: &HnDigestChunkSummary| {
                            if acc.budget_per_group == TokenCount::ZERO {
                                acc.budget_per_group = summary.budget_per_group;
                            }
                            acc.summaries.push(summary.clone());
                        },
                    )
                    .with_planning_summary(|acc, planning| {
                        acc.input_items_total = planning.input_items_total;
                        acc.planned_items_total = planning.planned_items_total;
                        acc.excluded_items_total = planning.excluded_items_total;
                    }),
                )
                .finalise(digest_llm_handler)
                .map_middleware([ai_circuit_breaker()])
                .finalise_middleware([ai_circuit_breaker()])
                .build();
                digest_summary = sink!(HnDigestSummary => sinks::console(format_digest_summary_for_console));
            },

            topology: {
                hn_stories |> formatter;
                formatter |> batch;
                batch |> digest;
                digest |> digest_summary;
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
    out.push_str(&format!("token_estimator: {:?}\n", summary.token_estimator));
    out.push_str(&format!("stories_fetched: {}\n", summary.stories_fetched));
    out.push_str(&format!(
        "groups: {} (budget_per_group: {})\n",
        summary.groups, summary.budget_per_group
    ));
    let oversize_groups = summary
        .chunk_summaries
        .iter()
        .filter(|s| s.oversize)
        .count();
    if oversize_groups > 0 {
        out.push_str(&format!("oversize_groups: {}\n", oversize_groups));

        let max_depth = summary
            .chunk_summaries
            .iter()
            .filter(|s| s.oversize)
            .map(|s| s.decomposition_depth)
            .max()
            .unwrap_or(0);
        out.push_str(&format!("oversize_max_depth: {}\n", max_depth));
    }

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

fn build_chunk_prompt(envelope: &StoryChunkEnvelope, interests: Option<&str>) -> String {
    let mut out = String::new();

    if let Some(interests) = interests {
        out.push_str("My interests: ");
        out.push_str(interests.trim());
        out.push_str("\n\n");
    }

    let min_citations = envelope.items.len().min(6);
    out.push_str(
        "Summarise this chunk of Hacker News stories (titles + URLs are provided as input).\n",
    );
    out.push_str("- Do not invent facts that are not implied by the titles.\n");
    out.push_str("- Use a neutral, specific tone.\n");
    out.push_str("- IMPORTANT: Do not repeat the input story list.\n");
    out.push_str("- Cite stories only by number, like (12); do not paste URLs.\n");
    out.push_str(&format!(
        "- Reference at least {min_citations} distinct story numbers across Themes + Notable stories.\n\n",
    ));

    out.push_str("Output format (follow exactly):\n");
    out.push_str(&format!(
        "## Chunk {}/{}\n",
        envelope.chunk_index + 1,
        envelope.chunk_count
    ));
    out.push_str("Themes:\n");
    out.push_str("- <theme> (n, n, n): 1 sentence\n");
    out.push_str("- <theme> (n, n, n): 1 sentence\n");
    out.push_str("- <theme> (n, n, n): 1 sentence\n");
    out.push_str("Notable stories:\n");
    out.push_str("- (n) Title: 1 sentence\n");
    out.push_str("- (n) Title: 1 sentence\n");
    out.push_str("- (n) Title: 1 sentence\n");
    out.push_str("- (n) Title: 1 sentence\n\n");

    out.push_str("Input stories (numbered; do not repeat):\n");
    out.push_str("```text\n");
    debug_assert_eq!(envelope.item_ordinals.len(), envelope.items.len());
    for (ordinal, story) in envelope.item_ordinals.iter().zip(envelope.items.iter()) {
        let story_number = *ordinal + 1;
        out.push_str(&render_story_line(story_number, story, envelope.decomposition_depth));
        out.push('\n');
    }
    out.push_str("```\n");

    out
}

fn strip_accidental_story_echo(markdown: &str) -> String {
    let mut out = String::new();
    for line in markdown.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("Stories:") || trimmed.starts_with("Input stories") {
            break;
        }
        out.push_str(line);
        out.push('\n');
    }
    out.trim_end().to_string()
}

fn build_reduce_prompt(batch: &HnDigestChunkSummaries, interests: Option<&str>) -> String {
    let mut out = String::new();

    if let Some(interests) = interests {
        out.push_str("My interests: ");
        out.push_str(interests.trim());
        out.push_str("\n\n");
    }

    out.push_str("Write a concise Markdown digest of the following Hacker News chunk summaries.\n");
    out.push_str("- Do not invent facts that are not implied by the titles.\n");
    out.push_str("- Start the response immediately with \"## What's topical today\" (no intro).\n");
    out.push_str("- Include: Thesis, Themes (cite story numbers), Notable stories, Watch.\n");
    out.push_str("- Avoid generic wrap-ups.\n\n");

    let mut summaries = batch.summaries.clone();
    summaries.sort_by_key(|s| {
        (
            s.group_index,
            s.story_numbers.first().copied().unwrap_or(usize::MAX),
        )
    });

    out.push_str("Chunk summaries:\n");
    for summary in &summaries {
        let oversize_suffix = if summary.oversize {
            format!(" (oversize depth {})", summary.decomposition_depth)
        } else {
            String::new()
        };
        out.push_str(&format!(
            "\n### Chunk {}/{}{}\n",
            summary.group_index + 1,
            summary.group_count,
            oversize_suffix
        ));
        out.push_str(summary.output_markdown.trim());
        out.push('\n');
    }

    out
}

fn render_story_line(n: usize, story: &FormattedStory, decomposition_depth: u32) -> String {
    let title = story.title.trim();
    let url = story.url.trim();

    match decomposition_depth {
        0 => format!(
            "{n}. {} — {}",
            truncate_chars(title, 140),
            truncate_chars(url, 200)
        ),
        1 => format!(
            "{n}. {} — {}",
            truncate_chars(title, 120),
            truncate_chars(url, 120)
        ),
        2 => format!("{n}. {}", truncate_chars(title, 100)),
        3 => format!("{n}. {}", truncate_chars(title, 80)),
        4 => format!("{n}. {}", truncate_chars(title, 60)),
        _ => format!("{n}. {}", truncate_chars(title, 40)),
    }
}

fn format_story(story: HnStory) -> FormattedStory {
    FormattedStory {
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
    }
}
