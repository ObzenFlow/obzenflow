// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::config::DemoConfig;
use super::decoder::HnStoryDecoder;
use super::domain::{FormattedStory, HnStory};
use super::util::truncate_chars;
use anyhow::{anyhow, Result};
use obzenflow::ai::{ChunkInfo, EstimateSource, Prompt, SystemPrompt, TokenCount, UserPrompt};
use obzenflow::sources::{HeaderMap, HttpPullConfig, HttpPullSource};
use obzenflow::typed::{sinks, stateful as typed_stateful, transforms as typed_transforms};
use obzenflow_adapters::middleware::control::ai_circuit_breaker;
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_core::ai::ChatResponse;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{ai_map_reduce, async_source, flow, sink, stateful, transform};
use obzenflow_infra::application::{Banner, FlowApplication, Presentation, RunPresentationOutcome};
use obzenflow_infra::http_client::default_http_client;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HnTopStories {
    stories: Vec<FormattedStory>,
}

impl TypedPayload for HnTopStories {
    const EVENT_TYPE: &'static str = "hn.top_stories";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnDigestGroupSummary {
    output_markdown: String,
}

impl TypedPayload for HnDigestGroupSummary {
    const EVENT_TYPE: &'static str = "hn.digest_group_summary";
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
    chat_prompt_system: SystemPrompt,
    chat_prompt_user: UserPrompt,
    input: HnTopStories,
    group_summaries: Vec<HnDigestGroupSummary>,
    output_markdown: String,
}

impl TypedPayload for HnDigestSummary {
    const EVENT_TYPE: &'static str = "hn.digest_summary";
}

struct DigestMapCtx {
    interests: Option<String>,
}

fn digest_map_prompt(
    ctx: &DigestMapCtx,
    stories: &[FormattedStory],
    chunk_info: &ChunkInfo,
) -> Result<UserPrompt, HandlerError> {
    let min_citations = stories.len().min(6);

    let rules: Vec<String> = vec![
        "Do not invent facts that are not implied by the titles.".to_string(),
        "Use a neutral, specific tone.".to_string(),
        "IMPORTANT: Do not repeat the input story list.".to_string(),
        "Cite stories only by number, like (12); do not paste URLs.".to_string(),
        format!(
            "Reference at least {min_citations} distinct story numbers across Themes + Notable stories."
        ),
    ];

    let mut p = Prompt::new();
    p.text_if(ctx.interests.as_deref(), |i| format!("My interests: {i}"))
        .text("Summarise these Hacker News stories (titles + URLs are provided as input).")
        .rules(rules)
        .labeled(
            "Output format (follow exactly)",
            "Themes:\n\
- <theme> (n, n, n): 1 sentence\n\
- <theme> (n, n, n): 1 sentence\n\
- <theme> (n, n, n): 1 sentence\n\
Notable stories:\n\
- (n) Title: 1 sentence\n\
- (n) Title: 1 sentence\n\
- (n) Title: 1 sentence\n\
- (n) Title: 1 sentence",
        )
        .fenced_lines(
            "Input stories (numbered; do not repeat)",
            chunk_info.iter_rendered(),
        );

    Ok(p.finish())
}

fn digest_map_parse(
    _ctx: &DigestMapCtx,
    response: ChatResponse,
) -> Result<HnDigestGroupSummary, HandlerError> {
    Ok(HnDigestGroupSummary {
        output_markdown: strip_accidental_story_echo(&response.text),
    })
}

struct DigestReduceCtx {
    interests: Option<String>,
    mode_label: String,
    base_url: String,
    ai_provider: String,
    ai_model: String,
    token_estimator: EstimateSource,
    budget_per_group: TokenCount,
    chat_prompt_system: SystemPrompt,
}

fn digest_reduce_prompt(
    ctx: &DigestReduceCtx,
    _seed: &HnTopStories,
    summaries: &[HnDigestGroupSummary],
) -> Result<UserPrompt, HandlerError> {
    let rules: Vec<String> = vec![
        "Do not invent facts that are not implied by the titles.".to_string(),
        "Start the response immediately with \"## What's topical today\" (no intro).".to_string(),
        "Include: Thesis, Themes (cite story numbers), Notable stories, Watch.".to_string(),
        "Avoid generic wrap-ups.".to_string(),
    ];

    let mut p = Prompt::new();
    p.text_if(ctx.interests.as_deref(), |i| format!("My interests: {i}"))
        .text("Write a concise Markdown digest of the following Hacker News chunk summaries.")
        .rules(rules)
        .indexed_sections("Chunk summaries", summaries, |idx, summary| {
            (
                format!("Group {idx}"),
                summary.output_markdown.trim().to_string(),
            )
        });

    Ok(p.finish())
}

fn digest_reduce_parse(
    ctx: &DigestReduceCtx,
    seed: HnTopStories,
    summaries: Vec<HnDigestGroupSummary>,
    user_prompt: UserPrompt,
    response: ChatResponse,
) -> Result<HnDigestSummary, HandlerError> {
    let groups = summaries.len();
    let stories_fetched = seed.stories.len();

    Ok(HnDigestSummary {
        mode: ctx.mode_label.clone(),
        base_url: ctx.base_url.clone(),
        ai_provider: ctx.ai_provider.clone(),
        ai_model: ctx.ai_model.clone(),
        token_estimator: ctx.token_estimator,
        stories_fetched,
        budget_per_group: ctx.budget_per_group,
        groups,
        interests: ctx.interests.clone(),
        chat_prompt_system: ctx.chat_prompt_system.clone(),
        chat_prompt_user: user_prompt,
        input: seed,
        group_summaries: summaries,
        output_markdown: response.text,
    })
}

pub async fn run_example(config: DemoConfig, presentation: Presentation) -> Result<()> {
    let DemoConfig {
        max_stories,
        poll_timeout_secs,
        source_rate_limit,
        ai,
        budget_per_group,
        max_stories_per_group,
        interests,
        mode_label: mode_label_for_summary,
        base_url,
        mock_server: _mock_server,
        ..
    } = config;

    let base_url_for_summary = base_url.to_string();
    let estimator = ai.estimator();

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
            acc.stories.push(story.clone());
        })
        .emit_on_eof();

    let system_prompt: SystemPrompt = "You write concise, skimmable Hacker News digests from a list of headlines + URLs. Be neutral, avoid hype, and do not invent facts beyond what the titles imply."
        .into();

    let map_llm_handler = ai
        .chat()
        .system(system_prompt.clone())
        .temperature(0.2)
        .max_tokens(800)
        .context(DigestMapCtx {
            interests: interests.clone(),
        })
        .build_map_items_with_chunk_info(digest_map_prompt, digest_map_parse)
        .await?;

    let digest_llm_handler = ai
        .chat()
        .system(system_prompt.clone())
        .temperature(0.2)
        .max_tokens(800)
        .context(DigestReduceCtx {
            mode_label: mode_label_for_summary.clone(),
            base_url: base_url_for_summary.clone(),
            ai_provider: ai.provider_label().to_string(),
            ai_model: ai.model_label().to_string(),
            token_estimator: ai.resolved_estimator().source(),
            budget_per_group,
            interests: interests.clone(),
            chat_prompt_system: system_prompt.clone(),
        })
        .build_reduce_seeded_with_prompt(digest_reduce_prompt, digest_reduce_parse)
        .await?;

    FlowApplication::builder()
        .with_presentation(presentation)
        .run_async(flow! {
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

                // Type bridge:
                // - map's `[FormattedStory]` comes from `HnTopStories.stories` via
                //   `items: |seed: &HnTopStories| seed.stories.clone()`.
                // - map's render uses `ChunkRenderContext.item_ordinal` to assign stable story numbers.
                // - reduce's `[HnDigestGroupSummary]` is collected in chunk-index order.
                digest = ai_map_reduce!(
                    HnTopStories -> HnDigestSummary => {
                        map: [FormattedStory] -> HnDigestGroupSummary => map_llm_handler,
                        reduce: (HnTopStories, [HnDigestGroupSummary]) -> HnDigestSummary => digest_llm_handler,
                    },
                    chunking: by_budget {
                        estimator: estimator.clone(),
                        items: |seed: &HnTopStories| seed.stories.clone(),
                        render: |story: &FormattedStory, ctx| render_story_line(ctx.item_ordinal + 1, story),
                        budget: budget_per_group,
                        max_items: max_stories_per_group,
                        oversize: decompose { max_depth: 5, exhaustion: fail },
                        snapshot_excluded_items_limit: 25,
                    },
                    middleware: {
                        map: ai_circuit_breaker(),
                        reduce: ai_circuit_breaker(),
                    }
                );
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

    Ok(())
}

pub fn run_demo_blocking() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async {
        let config = DemoConfig::from_env().await?;
        let presentation = build_presentation(&config);
        run_example(config, presentation).await
    })
}

fn build_presentation(config: &DemoConfig) -> Presentation {
    Presentation::new(
        Banner::new("HN AI Digest Demo")
            .description(
                "Fetch top HN stories, then generate a markdown digest via Rig-backed LLM transforms.",
            )
            .config("mode", &config.mode_label)
            .config("base_url", config.base_url.to_string())
            .config("max_stories", config.max_stories)
            .config("poll_timeout", format!("{}s", config.poll_timeout_secs))
            .section("AI", &config.ai)
            .config("group_budget_tokens", config.budget_per_group_tokens)
            .config("group_max_stories", config.group_max_stories_label())
            .config("source_rate_limit", format!("{} events/sec", config.source_rate_limit)),
    )
    .with_footer(|outcome| {
        let is_success = matches!(&outcome, RunPresentationOutcome::Completed { .. });
        let footer = outcome.into_footer();
        if is_success {
            footer.paragraph(
                "The generated digest was printed above.\nRe-run with HN_LIVE=1 to fetch from the real Hacker News API.",
            )
        } else {
            footer
        }
    })
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

    if let Some(interests) = &summary.interests {
        if !interests.trim().is_empty() {
            out.push_str(&format!("interests: {}\n", interests.trim()));
        }
    }

    out.push('\n');

    out.push_str("Chat prompt (system)\n");
    out.push_str("--------------------\n");
    out.push_str(summary.chat_prompt_system.as_ref().trim());

    out.push_str("\n\nChat prompt (user)\n");
    out.push_str("------------------\n");
    out.push_str(summary.chat_prompt_user.as_ref().trim());

    out.push_str("\n\nInput data (stories)\n");
    out.push_str("--------------------\n");
    for (n, story) in summary.input.stories.iter().enumerate() {
        let n = n + 1;
        let title = truncate_chars(story.title.trim(), 140);
        let author = truncate_chars(story.author.trim(), 60);
        let url = truncate_chars(story.url.trim(), 160);
        out.push_str(&format!(
            "{n}. {title} ({points} points, {comments} comments) by {author}\n    {url}\n",
            n = n,
            points = story.points,
            comments = story.comments,
        ));
    }

    out.push_str("\nOutput (markdown)\n");
    out.push_str("-----------------\n");
    out.push_str(summary.output_markdown.trim());

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

fn render_story_line(n: usize, story: &FormattedStory) -> String {
    let title = story.title.trim();
    let url = story.url.trim();

    format!(
        "{n}. {} — {}",
        truncate_chars(title, 140),
        truncate_chars(url, 200)
    )
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
