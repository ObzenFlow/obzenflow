// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::decoder::HnStoryDecoder;
use super::domain::{FormattedStory, HnStory};
use super::mock_server::spawn_mock_hn_server;
use super::util::{env_bool, env_usize, truncate_chars};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow::ai::{
    estimator_for_model, split_to_budget, ChatTransform, ChatTransformExt, EstimateSource,
    SplitGroup, TokenCount, TokenEstimator,
};
use obzenflow::sinks::ConsoleSink;
use obzenflow::sources::{HeaderMap, HttpPullConfig, HttpPullSource, Url};
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_adapters::middleware::control::ai_circuit_breaker;
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_dsl::{async_source, async_transform, flow, sink, stateful, transform};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::http_client::default_http_client;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
use obzenflow_runtime::stages::stateful::strategies::accumulators::ReduceTyped;
use obzenflow_runtime::stages::transform::TryMapWith;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_HN_SOURCE_RATE_LIMIT: f64 = 10.0;

fn env_f64(key: &str) -> Option<f64> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HnTopStories {
    stories: Vec<FormattedStory>,
}

impl TypedPayload for HnTopStories {
    const EVENT_TYPE: &'static str = "hn.top_stories";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnDigestChunk {
    group_index: usize,
    group_count: usize,
    budget_per_group: TokenCount,
    estimated_tokens: TokenCount,
    oversize: bool,
    decomposition_depth: u32,
    story_numbers: Vec<usize>,
    stories: Vec<FormattedStory>,
}

impl TypedPayload for HnDigestChunk {
    const EVENT_TYPE: &'static str = "hn.digest_chunk";
}

/// An oversize chunk that needs further decomposition before it is safe to send to the LLM.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnDigestOversizeChunk {
    parent_group_index: usize,
    parent_group_count: usize,
    budget_per_group: TokenCount,
    estimated_tokens: TokenCount,
    story_numbers: Vec<usize>,
    stories: Vec<FormattedStory>,
    decomposition_depth: u32,
}

impl TypedPayload for HnDigestOversizeChunk {
    const EVENT_TYPE: &'static str = "hn.digest_oversize_chunk";
}

/// A condensed chunk produced by the oversize sub-pipeline.
///
/// This is fed back into the main pipeline via the `<|` backflow edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnDigestCondensedChunk {
    parent_group_index: usize,
    parent_group_count: usize,
    budget_per_group: TokenCount,
    estimated_tokens: TokenCount,
    story_numbers: Vec<usize>,
    stories: Vec<FormattedStory>,
    condensed_markdown: String,
    decomposition_depth: u32,
}

impl TypedPayload for HnDigestCondensedChunk {
    const EVENT_TYPE: &'static str = "hn.digest_condensed_chunk";
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

    let estimator: Arc<dyn TokenEstimator> = Arc::from(estimator_for_model(&ai_model_label));

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
    println!("  token_estimator: {:?}", estimator.source());
    println!("  group_budget_tokens: {budget_per_group_tokens}");
    match max_stories_per_group {
        None => println!("  group_max_stories: unlimited"),
        Some(v) => println!("  group_max_stories: {v}"),
    }
    println!(
        "  source_rate_limit: {source_rate_limit} events/sec"
    );
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
    let interests_for_map_request = interests.clone();
    let interests_for_map_summary = interests.clone();
    let interests_for_oversize_request = interests.clone();
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

    let splitter =
        HnDigestSplitter::new(estimator.clone(), budget_per_group, max_stories_per_group);
    let oversize_sub_splitter =
        HnDigestOversizeSubSplitter::new(estimator.clone(), budget_per_group);

    let map_llm = make_llm_builder()
        .system(system_prompt.clone())
        .temperature(0.2)
        .max_tokens(800)
        .output_mapper(move |input: &ChainEvent, response| {
            let chunk = HnDigestChunk::try_from_event(input).map_err(|err| {
                HandlerError::Validation(format!("hn digest chunk decode failed: {err}"))
            })?;

            let user_prompt = build_chunk_prompt(&chunk, interests_for_map_summary.as_deref());

            let summary = HnDigestChunkSummary {
                group_index: chunk.group_index,
                group_count: chunk.group_count,
                budget_per_group: chunk.budget_per_group,
                estimated_tokens: chunk.estimated_tokens,
                oversize: chunk.oversize,
                decomposition_depth: chunk.decomposition_depth,
                story_numbers: chunk.story_numbers.clone(),
                stories: chunk.stories.clone(),
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
            let chunk = HnDigestChunk::try_from_event(event).map_err(|err| {
                HandlerError::Validation(format!("hn digest chunk decode failed: {err}"))
            })?;

            Ok(build_chunk_prompt(
                &chunk,
                interests_for_map_request.as_deref(),
            ))
        })
        .await?
        .with_estimator(estimator.clone());

    let map_router = HnDigestMapRouter::new(map_llm);

    let oversize_map_llm = make_llm_builder()
        .system(system_prompt.clone())
        .temperature(0.1)
        .max_tokens(400)
        .output_mapper(move |input: &ChainEvent, response| {
            let chunk = HnDigestChunk::try_from_event(input).map_err(|err| {
                HandlerError::Validation(format!("hn digest chunk decode failed: {err}"))
            })?;

            let condensed = HnDigestCondensedChunk {
                parent_group_index: chunk.group_index,
                parent_group_count: chunk.group_count,
                budget_per_group: chunk.budget_per_group,
                estimated_tokens: chunk.estimated_tokens,
                story_numbers: chunk.story_numbers.clone(),
                stories: chunk.stories.clone(),
                condensed_markdown: response.text,
                decomposition_depth: chunk.decomposition_depth,
            };

            let payload = serde_json::to_value(&condensed).map_err(|err| {
                HandlerError::Validation(format!(
                    "hn digest condensed chunk payload encode failed: {err}"
                ))
            })?;

            Ok(vec![ChainEventFactory::derived_data_event(
                input.writer_id,
                input,
                HnDigestCondensedChunk::versioned_event_type(),
                payload,
            )])
        })
        .build(move |event: &ChainEvent| {
            let chunk = HnDigestChunk::try_from_event(event).map_err(|err| {
                HandlerError::Validation(format!("hn digest chunk decode failed: {err}"))
            })?;

            Ok(build_oversize_chunk_prompt(
                &chunk,
                interests_for_oversize_request.as_deref(),
            ))
        })
        .await?
        .with_estimator(estimator.clone());

    let oversize_map_router = HnDigestOversizeMapRouter::new(oversize_map_llm);

    let chunk_summaries = ReduceTyped::new(
        HnDigestChunkSummaries::default(),
        |acc: &mut HnDigestChunkSummaries, summary: &HnDigestChunkSummary| {
            if acc.budget_per_group == TokenCount::ZERO {
                acc.budget_per_group = summary.budget_per_group;
            }
            acc.summaries.push(summary.clone());
        },
    )
    .emit_on_eof();

    let digest_llm = make_llm_builder()
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
        .with_estimator(estimator.clone());

    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .run_async(flow! {
            name: "hn_ai_digest_demo",
            journals: disk_journals(std::path::PathBuf::from("target/hn-ai-digest-logs")),
            middleware: [],

            stages: {
                source = async_source!("hn_stories" => (
                    HttpPullSource::new(decoder, config),
                    Some(Duration::from_secs(poll_timeout_secs as u64))
                ), [
                    rate_limit(source_rate_limit)
                ]);
                formatter = transform!("formatter" => formatter);
                batch = stateful!("batch" => digest_seed);
                split = transform!("split_to_budget" => splitter);
                map = async_transform!("map_llm" => map_router, [ai_circuit_breaker()]);
                oversize_sub_split = transform!("oversize_sub_split" => oversize_sub_splitter);
                oversize_map = async_transform!("oversize_map_llm" => oversize_map_router, [ai_circuit_breaker()]);
                reduce = stateful!("reduce" => chunk_summaries);
                digest = async_transform!("digest_llm" => digest_llm, [ai_circuit_breaker()]);
                output = sink!("digest_summary" => ConsoleSink::<HnDigestSummary>::new(format_digest_summary_for_console));
            },

            topology: {
                source |> formatter;
                formatter |> batch;
                batch |> split;
                split |> map;
                split |> reduce;
                map |> reduce;
                reduce |> digest;
                digest |> output;

                split |> oversize_sub_split;
                oversize_sub_split |> oversize_map;
                split <| oversize_map;
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

#[derive(Clone)]
struct HnDigestSplitter {
    estimator: Arc<dyn TokenEstimator>,
    budget_per_group: TokenCount,
    max_stories_per_group: Option<usize>,
}

impl std::fmt::Debug for HnDigestSplitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnDigestSplitter")
            .field("estimator_source", &self.estimator.source())
            .field("budget_per_group", &self.budget_per_group)
            .field("max_stories_per_group", &self.max_stories_per_group)
            .finish()
    }
}

impl HnDigestSplitter {
    fn new(
        estimator: Arc<dyn TokenEstimator>,
        budget_per_group: TokenCount,
        max_stories_per_group: Option<usize>,
    ) -> Self {
        Self {
            estimator,
            budget_per_group,
            max_stories_per_group,
        }
    }
}

#[async_trait]
impl TransformHandler for HnDigestSplitter {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        if HnDigestOversizeChunk::event_type_matches(&event.event_type()) {
            let payload = event.payload().clone();
            return Ok(vec![ChainEventFactory::derived_data_event(
                event.writer_id,
                &event,
                HnDigestOversizeChunk::versioned_event_type(),
                payload,
            )]);
        }

        if HnDigestCondensedChunk::event_type_matches(&event.event_type()) {
            let condensed = HnDigestCondensedChunk::try_from_event(&event).map_err(|err| {
                HandlerError::Validation(format!("hn digest condensed chunk decode failed: {err}"))
            })?;

            let summary = HnDigestChunkSummary {
                group_index: condensed.parent_group_index,
                group_count: condensed.parent_group_count,
                budget_per_group: condensed.budget_per_group,
                estimated_tokens: condensed.estimated_tokens,
                oversize: true,
                decomposition_depth: condensed.decomposition_depth,
                story_numbers: condensed.story_numbers.clone(),
                stories: condensed.stories.clone(),
                chat_prompt_user: format!(
                    "Oversize backflow (decomposition_depth={})",
                    condensed.decomposition_depth
                ),
                output_markdown: condensed.condensed_markdown,
            };

            let payload = serde_json::to_value(&summary).map_err(|err| {
                HandlerError::Validation(format!("chunk summary payload encode failed: {err}"))
            })?;

            return Ok(vec![ChainEventFactory::derived_data_event(
                event.writer_id,
                &event,
                HnDigestChunkSummary::versioned_event_type(),
                payload,
            )]);
        }

        if !HnTopStories::event_type_matches(&event.event_type()) {
            return Ok(vec![event]);
        }

        let seed = HnTopStories::try_from_event(&event).map_err(|err| {
            HandlerError::Validation(format!("hn digest seed decode failed: {err}"))
        })?;

        let story_lines = seed
            .stories
            .iter()
            .enumerate()
            .map(|(idx, story)| render_story_line(idx + 1, story, 0))
            .collect::<Vec<_>>();

        let story_line_tokens = story_lines
            .iter()
            .map(|s| self.estimator.estimate_text(s).tokens)
            .collect::<Vec<_>>();

        let story_line_refs = story_lines.iter().map(|s| s.as_str()).collect::<Vec<_>>();
        let mut groups = split_to_budget(
            self.estimator.as_ref(),
            &story_line_refs,
            self.budget_per_group,
        )
        .map_err(|err| HandlerError::Validation(format!("split_to_budget failed: {err}")))?;

        if let Some(max_stories_per_group) = self.max_stories_per_group {
            groups =
                enforce_max_group_story_count(groups, max_stories_per_group, &story_line_tokens);
        }

        let group_count = groups.len();
        let mut outputs = Vec::with_capacity(group_count);

        for (group_index, group) in groups.into_iter().enumerate() {
            let mut story_numbers = Vec::with_capacity(group.indices.len());
            let mut stories = Vec::with_capacity(group.indices.len());

            for idx in group.indices {
                let Some(story) = seed.stories.get(idx) else {
                    return Err(HandlerError::Validation(format!(
                        "split_to_budget returned out-of-range index: {idx}"
                    )));
                };
                story_numbers.push(idx + 1);
                stories.push(story.clone());
            }

            if group.oversize {
                let oversize = HnDigestOversizeChunk {
                    parent_group_index: group_index,
                    parent_group_count: group_count,
                    budget_per_group: self.budget_per_group,
                    estimated_tokens: group.estimated_tokens,
                    story_numbers,
                    stories,
                    decomposition_depth: 0,
                };

                let payload = serde_json::to_value(&oversize).map_err(|err| {
                    HandlerError::Validation(format!(
                        "hn digest oversize chunk payload encode failed: {err}"
                    ))
                })?;

                outputs.push(ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    HnDigestOversizeChunk::versioned_event_type(),
                    payload,
                ));
            } else {
                let chunk = HnDigestChunk {
                    group_index,
                    group_count,
                    budget_per_group: self.budget_per_group,
                    estimated_tokens: group.estimated_tokens,
                    oversize: false,
                    decomposition_depth: 0,
                    story_numbers,
                    stories,
                };

                let payload = serde_json::to_value(&chunk).map_err(|err| {
                    HandlerError::Validation(format!(
                        "hn digest chunk payload encode failed: {err}"
                    ))
                })?;

                outputs.push(ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    HnDigestChunk::versioned_event_type(),
                    payload,
                ));
            }
        }

        Ok(outputs)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone)]
struct HnDigestOversizeSubSplitter {
    estimator: Arc<dyn TokenEstimator>,
    budget_per_group: TokenCount,
}

impl std::fmt::Debug for HnDigestOversizeSubSplitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnDigestOversizeSubSplitter")
            .field("estimator_source", &self.estimator.source())
            .field("budget_per_group", &self.budget_per_group)
            .finish()
    }
}

impl HnDigestOversizeSubSplitter {
    fn new(estimator: Arc<dyn TokenEstimator>, budget_per_group: TokenCount) -> Self {
        Self {
            estimator,
            budget_per_group,
        }
    }
}

#[async_trait]
impl TransformHandler for HnDigestOversizeSubSplitter {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        if !HnDigestOversizeChunk::event_type_matches(&event.event_type()) {
            return Ok(Vec::new());
        }

        let seed = HnDigestOversizeChunk::try_from_event(&event).map_err(|err| {
            HandlerError::Validation(format!("hn digest oversize chunk decode failed: {err}"))
        })?;

        let story_lines = seed
            .stories
            .iter()
            .zip(seed.story_numbers.iter())
            .map(|(story, n)| render_story_line(*n, story, seed.decomposition_depth))
            .collect::<Vec<_>>();

        let story_line_refs = story_lines.iter().map(|s| s.as_str()).collect::<Vec<_>>();
        let groups = split_to_budget(
            self.estimator.as_ref(),
            &story_line_refs,
            self.budget_per_group,
        )
        .map_err(|err| HandlerError::Validation(format!("split_to_budget failed: {err}")))?;

        let mut outputs = Vec::new();

        for group in groups {
            let mut story_numbers = Vec::with_capacity(group.indices.len());
            let mut stories = Vec::with_capacity(group.indices.len());

            for idx in group.indices {
                let Some(n) = seed.story_numbers.get(idx) else {
                    return Err(HandlerError::Validation(format!(
                        "split_to_budget returned out-of-range index: {idx}"
                    )));
                };
                let Some(story) = seed.stories.get(idx) else {
                    return Err(HandlerError::Validation(format!(
                        "split_to_budget returned out-of-range index: {idx}"
                    )));
                };
                story_numbers.push(*n);
                stories.push(story.clone());
            }

            if group.oversize {
                let oversize = HnDigestOversizeChunk {
                    parent_group_index: seed.parent_group_index,
                    parent_group_count: seed.parent_group_count,
                    budget_per_group: seed.budget_per_group,
                    estimated_tokens: group.estimated_tokens,
                    story_numbers,
                    stories,
                    decomposition_depth: seed.decomposition_depth.saturating_add(1),
                };

                let payload = serde_json::to_value(&oversize).map_err(|err| {
                    HandlerError::Validation(format!(
                        "hn digest oversize chunk payload encode failed: {err}"
                    ))
                })?;

                outputs.push(ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    HnDigestOversizeChunk::versioned_event_type(),
                    payload,
                ));
            } else {
                let chunk = HnDigestChunk {
                    group_index: seed.parent_group_index,
                    group_count: seed.parent_group_count,
                    budget_per_group: seed.budget_per_group,
                    estimated_tokens: group.estimated_tokens,
                    oversize: false,
                    decomposition_depth: seed.decomposition_depth,
                    story_numbers,
                    stories,
                };

                let payload = serde_json::to_value(&chunk).map_err(|err| {
                    HandlerError::Validation(format!(
                        "hn digest chunk payload encode failed: {err}"
                    ))
                })?;

                outputs.push(ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    HnDigestChunk::versioned_event_type(),
                    payload,
                ));
            }
        }

        Ok(outputs)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone)]
struct HnDigestMapRouter {
    inner: ChatTransform,
}

impl std::fmt::Debug for HnDigestMapRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnDigestMapRouter").finish()
    }
}

impl HnDigestMapRouter {
    fn new(inner: ChatTransform) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl AsyncTransformHandler for HnDigestMapRouter {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        if !HnDigestChunk::event_type_matches(&event.event_type()) {
            return Ok(Vec::new());
        }

        self.inner.process(event).await
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        self.inner.drain().await
    }
}

#[derive(Clone)]
struct HnDigestOversizeMapRouter {
    inner: ChatTransform,
}

impl std::fmt::Debug for HnDigestOversizeMapRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnDigestOversizeMapRouter").finish()
    }
}

impl HnDigestOversizeMapRouter {
    fn new(inner: ChatTransform) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl AsyncTransformHandler for HnDigestOversizeMapRouter {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        if HnDigestChunk::event_type_matches(&event.event_type()) {
            return self.inner.process(event).await;
        }

        if HnDigestOversizeChunk::event_type_matches(&event.event_type()) {
            let payload = event.payload().clone();
            return Ok(vec![ChainEventFactory::derived_data_event(
                event.writer_id,
                &event,
                HnDigestOversizeChunk::versioned_event_type(),
                payload,
            )]);
        }

        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        self.inner.drain().await
    }
}

fn build_chunk_prompt(chunk: &HnDigestChunk, interests: Option<&str>) -> String {
    let mut out = String::new();

    if let Some(interests) = interests {
        out.push_str("My interests: ");
        out.push_str(interests.trim());
        out.push_str("\n\n");
    }

    let min_citations = chunk.stories.len().min(6);
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
        chunk.group_index + 1,
        chunk.group_count
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
    for (n, story) in chunk.story_numbers.iter().zip(chunk.stories.iter()) {
        out.push_str(&render_story_line(*n, story, chunk.decomposition_depth));
        out.push('\n');
    }
    out.push_str("```\n");

    out
}

fn build_oversize_chunk_prompt(chunk: &HnDigestChunk, interests: Option<&str>) -> String {
    let mut out = String::new();

    if let Some(interests) = interests {
        out.push_str("My interests: ");
        out.push_str(interests.trim());
        out.push_str("\n\n");
    }

    out.push_str(
        "Summarise the following stories very aggressively so they can be reduced later.\n",
    );
    out.push_str("- Do not invent facts that are not implied by the title.\n");
    out.push_str("- Cite stories only by number like (12); do not paste URLs.\n");
    out.push_str("- Keep the output short.\n\n");

    out.push_str("Output format:\n");
    out.push_str(&format!(
        "## Chunk {}/{}\n",
        chunk.group_index + 1,
        chunk.group_count
    ));
    out.push_str("- (n) 1 sentence\n");
    out.push_str("- (n) 1 sentence\n\n");

    out.push_str("Input stories (numbered; do not repeat):\n");
    out.push_str("```text\n");
    for (n, story) in chunk.story_numbers.iter().zip(chunk.stories.iter()) {
        out.push_str(&render_story_line(*n, story, chunk.decomposition_depth));
        out.push('\n');
    }
    out.push_str("```\n");

    out
}

fn enforce_max_group_story_count(
    groups: Vec<SplitGroup>,
    max_stories_per_group: usize,
    item_tokens: &[TokenCount],
) -> Vec<SplitGroup> {
    if max_stories_per_group == 0 {
        return groups;
    }

    let mut out = Vec::new();
    for group in groups {
        if group.oversize || group.indices.len() <= max_stories_per_group {
            out.push(group);
            continue;
        }

        for indices in group.indices.chunks(max_stories_per_group) {
            let mut estimated_tokens = TokenCount::ZERO;
            for idx in indices {
                if let Some(tokens) = item_tokens.get(*idx) {
                    estimated_tokens = estimated_tokens.saturating_add(*tokens);
                }
            }

            out.push(SplitGroup {
                indices: indices.to_vec(),
                estimated_tokens,
                oversize: false,
            });
        }
    }

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
