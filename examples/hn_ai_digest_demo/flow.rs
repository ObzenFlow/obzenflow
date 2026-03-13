// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::decoder::HnStoryDecoder;
use super::domain::{FormattedStory, HnStory};
use super::mock_server::spawn_mock_hn_server;
use super::util::{env_bool, env_usize, truncate_chars};
use anyhow::{anyhow, Result};
use obzenflow::ai::{AiChatTask, EstimateSource, ModelConfig, TokenCount};
use obzenflow::sources::{HeaderMap, HttpPullConfig, HttpPullSource, Url};
use obzenflow::typed::{sinks, stateful as typed_stateful, transforms as typed_transforms};
use obzenflow_adapters::middleware::control::ai_circuit_breaker;
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_core::ai::ChatResponse;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{ai_map_reduce, async_source, flow, sink, stateful, transform};
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HnTopStories {
    stories: Vec<NumberedStory>,
}

impl TypedPayload for HnTopStories {
    const EVENT_TYPE: &'static str = "hn.top_stories";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NumberedStory {
    n: usize,
    story: FormattedStory,
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
    chat_prompt_system: String,
    chat_prompt_user: String,
    input: HnTopStories,
    group_summaries: Vec<HnDigestGroupSummary>,
    output_markdown: String,
}

impl TypedPayload for HnDigestSummary {
    const EVENT_TYPE: &'static str = "hn.digest_summary";
}

#[derive(Clone)]
struct MapDigestTask {
    interests: Option<String>,
}

impl AiChatTask for MapDigestTask {
    type Input = Vec<NumberedStory>;
    type Output = HnDigestGroupSummary;

    fn prompt(&self, stories: &Self::Input) -> Result<String, HandlerError> {
        Ok(build_group_prompt(stories, self.interests.as_deref()))
    }

    fn parse(
        &self,
        _stories: Self::Input,
        response: ChatResponse,
    ) -> Result<Self::Output, HandlerError> {
        Ok(HnDigestGroupSummary {
            output_markdown: strip_accidental_story_echo(&response.text),
        })
    }
}

#[derive(Clone)]
struct ReduceDigestTask {
    interests: Option<String>,
    mode_label: String,
    base_url: String,
    ai_provider: String,
    ai_model: String,
    token_estimator: EstimateSource,
    budget_per_group: TokenCount,
    chat_prompt_system: String,
}

impl AiChatTask for ReduceDigestTask {
    type Input = (HnTopStories, Vec<HnDigestGroupSummary>);
    type Output = HnDigestSummary;

    fn prompt(&self, (_, summaries): &Self::Input) -> Result<String, HandlerError> {
        Ok(build_reduce_prompt(summaries, self.interests.as_deref()))
    }

    fn parse(
        &self,
        (seed, summaries): Self::Input,
        response: ChatResponse,
    ) -> Result<Self::Output, HandlerError> {
        let groups = summaries.len();
        let stories_fetched = seed.stories.len();
        let user_prompt = build_reduce_prompt(&summaries, self.interests.as_deref());

        Ok(HnDigestSummary {
            mode: self.mode_label.clone(),
            base_url: self.base_url.clone(),
            ai_provider: self.ai_provider.clone(),
            ai_model: self.ai_model.clone(),
            token_estimator: self.token_estimator,
            stories_fetched,
            budget_per_group: self.budget_per_group,
            groups,
            interests: self.interests.clone(),
            chat_prompt_system: self.chat_prompt_system.clone(),
            chat_prompt_user: user_prompt,
            input: seed,
            group_summaries: summaries,
            output_markdown: response.text,
        })
    }
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

    let ai = ModelConfig::from_env_with_prefix("HN_AI_")?;

    let budget_per_group_tokens =
        env_usize("HN_AI_GROUP_BUDGET_TOKENS").unwrap_or(match ai.provider_label() {
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

    let estimator = ai.estimator();

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
    for line in ai.to_string().lines() {
        println!("  {line}");
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
            let next_n = acc.stories.len() + 1;
            acc.stories.push(NumberedStory {
                n: next_n,
                story: story.clone(),
            })
        })
        .emit_on_eof();

    let interests = std::env::var("HN_AI_INTERESTS").ok();

    let system_prompt = "You write concise, skimmable Hacker News digests from a list of headlines + URLs. Be neutral, avoid hype, and do not invent facts beyond what the titles imply."
        .to_string();
    let system_prompt_for_summary = system_prompt.clone();

    let map_llm_handler = ai
        .chat()
        .system(system_prompt.clone())
        .temperature(0.2)
        .max_tokens(800)
        .build_task(MapDigestTask {
            interests: interests.clone(),
        })?;

    let digest_llm_handler = ai
        .chat()
        .system(system_prompt.clone())
        .temperature(0.2)
        .max_tokens(800)
        .build_task(ReduceDigestTask {
            interests: interests.clone(),
            mode_label: mode_label_for_summary.clone(),
            base_url: base_url_for_summary.clone(),
            ai_provider: ai.provider_label().to_string(),
            ai_model: ai.model_label().to_string(),
            token_estimator: ai.resolved_estimator().source(),
            budget_per_group,
            chat_prompt_system: system_prompt_for_summary.clone(),
        })?;

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
                digest = ai_map_reduce!(
                    HnTopStories -> HnDigestSummary => {
                        map: [NumberedStory] -> HnDigestGroupSummary => map_llm_handler,
                        reduce: (HnTopStories, [HnDigestGroupSummary]) -> HnDigestSummary => digest_llm_handler,
                    },
                    chunking: by_budget {
                        estimator: estimator.clone(),
                        items: |seed: &HnTopStories| seed.stories.clone(),
                        render: |story: &NumberedStory, _ctx| render_story_line(story.n, &story.story),
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
    for numbered in &summary.input.stories {
        let story = &numbered.story;
        let title = truncate_chars(story.title.trim(), 140);
        let author = truncate_chars(story.author.trim(), 60);
        let url = truncate_chars(story.url.trim(), 160);
        out.push_str(&format!(
            "{n}. {title} ({points} points, {comments} comments) by {author}\n    {url}\n",
            n = numbered.n,
            points = story.points,
            comments = story.comments,
        ));
    }

    out.push_str("\nOutput (markdown)\n");
    out.push_str("-----------------\n");
    out.push_str(summary.output_markdown.trim());

    out
}

fn build_group_prompt(stories: &[NumberedStory], interests: Option<&str>) -> String {
    let mut out = String::new();

    if let Some(interests) = interests {
        out.push_str("My interests: ");
        out.push_str(interests.trim());
        out.push_str("\n\n");
    }

    let min_citations = stories.len().min(6);
    out.push_str("Summarise these Hacker News stories (titles + URLs are provided as input).\n");
    out.push_str("- Do not invent facts that are not implied by the titles.\n");
    out.push_str("- Use a neutral, specific tone.\n");
    out.push_str("- IMPORTANT: Do not repeat the input story list.\n");
    out.push_str("- Cite stories only by number, like (12); do not paste URLs.\n");
    out.push_str(&format!(
        "- Reference at least {min_citations} distinct story numbers across Themes + Notable stories.\n\n",
    ));

    out.push_str("Output format (follow exactly):\n");
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
    for numbered in stories {
        out.push_str(&render_story_line(numbered.n, &numbered.story));
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

fn build_reduce_prompt(summaries: &[HnDigestGroupSummary], interests: Option<&str>) -> String {
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

    out.push_str("Chunk summaries:\n");
    for (idx, summary) in summaries.iter().enumerate() {
        out.push_str(&format!("\n### Group {}\n", idx + 1));
        out.push_str(summary.output_markdown.trim());
        out.push('\n');
    }

    out
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
