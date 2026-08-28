// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::config::{HnDigestPostgresConfig, HnRunInputs, PreparedHnRun};
use super::decoder::hn_story_decoder;
use super::domain::{FormattedStory, HnStory};
use super::util::truncate_chars;
use anyhow::Result;
use obzenflow::ai::{
    ChatBindingMetadata, ChatCompletion, ChatEffectBinding, ChunkInfo, EstimateSource, Prompt,
    SystemPrompt, TokenCount, UserPrompt,
};
use obzenflow::sinks::postgres::{
    PostgresBind, PostgresBindings, PostgresSink, PostgresSinkConfig,
};
use obzenflow::sources::{http_pull_config, HttpPullSource};
use obzenflow::{sinks, stateful, transforms};
use obzenflow_adapters::middleware::control::ai_resilience;
use obzenflow_adapters::middleware::{CircuitBreaker, RateLimiterBuilder};
use obzenflow_core::ai::{
    AiFinaliseRole, AiMapRole, AiRoleLogicFailure, ChatCompletionReply, ChatMessage, ChatParams,
    ChatRequestSpec, ChatResponse, ChatTarget, Many,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::dsl::error::FlowBuildError;
use obzenflow_dsl::{ai_map_reduce, async_source, flow, sink, stateful, transform, FlowDefinition};
use obzenflow_infra::application::{Banner, FlowApplication, Presentation, RunPresentationOutcome};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{EffectBinding, SinkRedeliverySafety};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

const HN_SOURCE_BREAKER_FAILURES: u32 = 3;
const HN_SOURCE_BREAKER_COOLDOWN_SECS: u64 = 2;
const HN_DIGEST_TABLE: &str = "hn_digest_summaries";
const HN_DIGEST_INSERT: &str = "(source_mode, source_identity, ai_provider, ai_model, \
token_estimator, interests, story_ids, output_markdown) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";

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

#[derive(Clone, Debug)]
struct HnDigestPostgresBinder;

impl PostgresBind for HnDigestPostgresBinder {
    type Input = HnDigestSummary;

    fn bind(&self, bindings: &mut PostgresBindings, digest: &Self::Input) {
        let token_estimator = match digest.token_estimator {
            EstimateSource::Heuristic => "heuristic",
            EstimateSource::Tokenizer => "tokenizer",
        };
        let story_ids = digest
            .input
            .stories
            .iter()
            .map(|story| story.id.to_string())
            .collect::<Vec<_>>();

        bindings
            .bind(&digest.mode)
            .bind(&digest.base_url)
            .bind(&digest.ai_provider)
            .bind(&digest.ai_model)
            .bind(token_estimator)
            .bind(digest.interests.as_deref())
            .bind(story_ids)
            .bind(&digest.output_markdown);
    }
}

fn build_digest_postgres_config(
    config: HnDigestPostgresConfig,
) -> Result<PostgresSinkConfig<HnDigestPostgresBinder>> {
    Ok(PostgresSink::builder(HnDigestPostgresBinder)
        .connection(config.connection)
        .insert_into(config.schema, HN_DIGEST_TABLE, HN_DIGEST_INSERT)?
        .batch_size(1)?
        .redelivery_safety(SinkRedeliverySafety::DuplicateSensitive)
        .build_config()?)
}

#[cfg(test)]
pub(crate) fn describe_digest_postgres_sink(
    config: HnDigestPostgresConfig,
) -> Result<obzenflow_runtime::stages::sink::SinkDescription> {
    let sink = sinks::postgres(build_digest_postgres_config(config)?);
    Ok(obzenflow_runtime::stages::sink::SinkConnector::describe(
        &sink,
    ))
}

struct DigestMapCtx {
    interests: Option<String>,
}

struct HnMapRole {
    system_prompt: SystemPrompt,
    context: DigestMapCtx,
}

impl HnMapRole {
    fn new(system_prompt: SystemPrompt, context: DigestMapCtx) -> Self {
        Self {
            system_prompt,
            context,
        }
    }
}

impl AiMapRole<FormattedStory, HnDigestGroupSummary> for HnMapRole {
    fn prepare(
        &self,
        items: &[FormattedStory],
        chunk: &ChunkInfo,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        let user_prompt = digest_map_prompt(&self.context, items, chunk).map_err(|error| {
            AiRoleLogicFailure::Prompt {
                message: error.to_string(),
            }
        })?;
        Ok(ChatRequestSpec {
            messages: vec![
                ChatMessage::system(self.system_prompt.as_str()),
                ChatMessage::user(user_prompt.as_str()),
            ],
            params: ChatParams {
                temperature: Some(0.2),
                max_tokens: Some(800),
                ..ChatParams::default()
            },
            tools: Vec::new(),
            response_format: None,
        })
    }

    fn interpret(
        &self,
        _items: Vec<FormattedStory>,
        _chunk: ChunkInfo,
        _request: ChatRequestSpec,
        reply: ChatCompletionReply,
    ) -> Result<HnDigestGroupSummary, AiRoleLogicFailure> {
        digest_map_parse(&self.context, reply.response).map_err(|error| AiRoleLogicFailure::Parse {
            message: error.to_string(),
        })
    }
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
        "Cite stories only with Markdown footnote markers like [^12]; do not use parenthesized numbers, paste URLs, or add footnote definitions.".to_string(),
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
- <theme> [^n] [^n] [^n]: 1 sentence\n\
- <theme> [^n] [^n] [^n]: 1 sentence\n\
- <theme> [^n] [^n] [^n]: 1 sentence\n\
Notable stories:\n\
- Title: 1 sentence [^n]\n\
- Title: 1 sentence [^n]\n\
- Title: 1 sentence [^n]\n\
- Title: 1 sentence [^n]",
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

struct HnFinaliseRole {
    context: DigestReduceCtx,
}

impl HnFinaliseRole {
    fn new(context: DigestReduceCtx) -> Self {
        Self { context }
    }
}

impl AiFinaliseRole<HnTopStories, Many<HnDigestGroupSummary>, HnDigestSummary> for HnFinaliseRole {
    fn prepare(
        &self,
        seed: &HnTopStories,
        collected: &Many<HnDigestGroupSummary>,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        let user_prompt =
            digest_reduce_prompt(&self.context, seed, &collected.items).map_err(|error| {
                AiRoleLogicFailure::Prompt {
                    message: error.to_string(),
                }
            })?;
        Ok(ChatRequestSpec {
            messages: vec![
                ChatMessage::system(self.context.chat_prompt_system.as_str()),
                ChatMessage::user(user_prompt.as_str()),
            ],
            params: ChatParams {
                temperature: Some(0.2),
                max_tokens: Some(800),
                ..ChatParams::default()
            },
            tools: Vec::new(),
            response_format: None,
        })
    }

    fn interpret(
        &self,
        seed: HnTopStories,
        collected: Many<HnDigestGroupSummary>,
        request: ChatRequestSpec,
        reply: ChatCompletionReply,
    ) -> Result<HnDigestSummary, AiRoleLogicFailure> {
        let user_prompt = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role.as_str() == "user")
            .map(|message| UserPrompt::raw(message.content.clone()))
            .ok_or_else(|| AiRoleLogicFailure::Prompt {
                message: "retained request has no user message".to_string(),
            })?;
        digest_reduce_parse(
            &self.context,
            seed,
            collected.items,
            user_prompt,
            reply.response,
        )
        .map_err(|error| AiRoleLogicFailure::Parse {
            message: error.to_string(),
        })
    }
}

fn digest_reduce_prompt(
    ctx: &DigestReduceCtx,
    _seed: &HnTopStories,
    summaries: &[HnDigestGroupSummary],
) -> Result<UserPrompt, HandlerError> {
    let rules: Vec<String> = vec![
        "Do not invent facts that are not implied by the titles.".to_string(),
        "Start the response immediately with \"## What's topical today\" (no intro).".to_string(),
        "Include: Thesis, Themes, Notable stories, Watch.".to_string(),
        "Cite stories only with Markdown footnote markers like [^12]; preserve the story numbers from the chunk summaries and do not use parenthesized citations.".to_string(),
        "Do not add URLs, footnote definitions, or a Links section; the application appends verified links.".to_string(),
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
    let output_markdown = render_digest_links(&response.text, &seed.stories)?;

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
        output_markdown,
    })
}

fn resolve_hn_group_budget(budget_override: Option<TokenCount>, target: &ChatTarget) -> TokenCount {
    budget_override.unwrap_or_else(|| {
        TokenCount::new(if target.provider.as_str() == "ollama" {
            2_500
        } else {
            6_000
        })
    })
}

pub(crate) struct HnFlowOptions {
    pub journal_base: std::path::PathBuf,
    pub chat_binding_override: Option<EffectBinding<ChatCompletion>>,
}

impl Default for HnFlowOptions {
    fn default() -> Self {
        Self {
            journal_base: std::path::PathBuf::from("target/hn-ai-digest-logs"),
            chat_binding_override: None,
        }
    }
}

pub(crate) fn build_flow_definition(inputs: HnRunInputs, options: HnFlowOptions) -> FlowDefinition {
    FlowDefinition::materialize(move |runtime_config| {
        let HnRunInputs {
            max_stories,
            poll_timeout_secs,
            source_rate_limit,
            budget_per_group_override,
            max_stories_per_group,
            interests,
            mode_label,
            base_url,
        } = inputs;
        let HnFlowOptions {
            journal_base,
            chat_binding_override,
        } = options;

        // The mock server binds an ephemeral loopback port for each process.
        // Its guard stays in the host; only this owned endpoint enters the
        // source. Durable output names the logical mock source.
        let base_url_for_summary = if mode_label == "mock" {
            "mock://hacker-news/".to_string()
        } else {
            base_url.to_string()
        };

        let chat = match chat_binding_override {
            Some(binding) => binding,
            None => ChatEffectBinding::from_config(&runtime_config.ai_models())?,
        };
        let chat_target = chat.target().clone();
        let budget_per_group = resolve_hn_group_budget(budget_per_group_override, &chat_target);
        let system_prompt: SystemPrompt = "You write concise, skimmable Hacker News digests from a list of headlines + URLs. Be neutral, avoid hype, and do not invent facts beyond what the titles imply."
            .into();
        let map_role = HnMapRole::new(
            system_prompt.clone(),
            DigestMapCtx {
                interests: interests.clone(),
            },
        );
        let finalise_role = HnFinaliseRole::new(DigestReduceCtx {
            mode_label,
            base_url: base_url_for_summary,
            ai_provider: chat_target.provider.to_string(),
            ai_model: chat_target.model.clone(),
            token_estimator: chat.estimator().source(),
            budget_per_group,
            interests,
            chat_prompt_system: system_prompt,
        });
        let decoder = hn_story_decoder(base_url, max_stories);
        let http_source_config = http_pull_config()
            .map_err(|error| {
                FlowBuildError::StageResourcesFailed(format!(
                    "HN HTTP source client unavailable: {error}"
                ))
            })?
            .max_batch_size(10)
            .poll_timeout(Duration::from_secs(poll_timeout_secs as u64))
            .build()
            .map_err(|error| {
                FlowBuildError::StageResourcesFailed(format!(
                    "HN HTTP source configuration failed: {error}"
                ))
            })?;
        let hn_source = HttpPullSource::new(decoder, http_source_config);
        let formatter = transforms::map(|story: HnStory| format_story(story));
        let digest_seed =
            stateful::reduce(HnTopStories::default(), |acc, story: &FormattedStory| {
                acc.stories.push(story.clone());
            })
            .emit_on_eof();
        let source_breaker = CircuitBreaker::builder()
            .consecutive_failures(HN_SOURCE_BREAKER_FAILURES)
            .open_for(Duration::from_secs(HN_SOURCE_BREAKER_COOLDOWN_SECS))
            .build()
            .map_err(|error| {
                FlowBuildError::StageResourcesFailed(format!(
                    "HN source circuit-breaker configuration failed: {error}"
                ))
            })?;
        let source_limiter = RateLimiterBuilder::new(source_rate_limit).build();
        let console_sink = sinks::console(format_digest_summary_for_console);
        let postgres_config = HnDigestPostgresConfig::from_env()
            .and_then(build_digest_postgres_config)
            .map_err(|error| {
                FlowBuildError::StageResourcesFailed(format!(
                    "HN digest PostgreSQL sink configuration failed: {error}"
                ))
            })?;
        let postgres_sink = sinks::postgres(postgres_config);

        Ok(flow! {
            name: "hn_ai_digest_demo",
            journals: disk_journals(journal_base),

            stages: {
                // Source-boundary policies (FLOWIP-115a): the breaker protects
                // the external HN HTTP dependency; the limiter paces API reads.
                // Replay reconstructs archived stories and suppresses both.
                hn_stories = async_source!(HnStory => hn_source with [
                    source_breaker,
                    source_limiter
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
                        map: [FormattedStory] -> HnDigestGroupSummary
                        uses at_least_once(ChatCompletion)
                            via chat
                            with ai_resilience()
                        => map_role,

                        reduce: (HnTopStories, [HnDigestGroupSummary]) -> HnDigestSummary
                        uses at_least_once(ChatCompletion)
                            via chat
                            with ai_resilience()
                        => finalise_role,
                    },
                    chunking: by_budget {
                        items: |seed: &HnTopStories| seed.stories.clone(),
                        render: |story: &FormattedStory, ctx| {
                            render_story_line(ctx.item_ordinal + 1, story)
                        },
                        budget: budget_per_group,
                        max_items: max_stories_per_group,
                        oversize: decompose {
                            max_depth: 5,
                            exhaustion: fail,
                        },
                        snapshot_excluded_items_limit: 25,
                    }
                );
                digest_summary = sink!(
                    HnDigestSummary => handler_set!(console_sink, postgres_sink)
                )?;
            },

            topology: {
                hn_stories |> formatter;
                formatter |> batch;
                batch |> digest;
                digest |> digest_summary;
            }
        })
    })
}

pub async fn run_example(prepared: PreparedHnRun, presentation: Presentation) -> Result<()> {
    let PreparedHnRun {
        inputs,
        mock_server,
    } = prepared;
    let outcome = FlowApplication::builder()
        .with_presentation(presentation)
        .run_async(build_flow_definition(inputs, HnFlowOptions::default()))
        .await;
    drop(mock_server);
    outcome?;
    Ok(())
}

pub fn run_demo_blocking() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async {
        let prepared = PreparedHnRun::from_env().await?;
        let presentation = build_presentation(&prepared.inputs);
        run_example(prepared, presentation).await
    })
}

pub(crate) fn build_presentation(config: &HnRunInputs) -> Presentation {
    Presentation::new(
        Banner::new("HN AI Digest Demo")
            .description(
                "Fetch top HN stories, then generate a markdown digest via Rig-backed LLM transforms.",
            )
            .config("mode", &config.mode_label)
            .config("base_url", config.base_url.to_string())
            .config("max_stories", config.max_stories)
            .config("poll_timeout", format!("{}s", config.poll_timeout_secs))
            .config("group_max_stories", config.group_max_stories_label())
            .config("source_rate_limit", format!("{} events/sec", config.source_rate_limit))
            .config(
                "source_breaker",
                format!(
                    "{} failures, {}s cooldown",
                    HN_SOURCE_BREAKER_FAILURES, HN_SOURCE_BREAKER_COOLDOWN_SECS
                ),
            ),
    )
    .with_footer(|outcome| {
        let is_success = matches!(&outcome, RunPresentationOutcome::Completed { .. });
        let footer = outcome.into_footer();
        if is_success {
            footer.paragraph(
                "The generated digest was delivered by the configured sink handler.\n\
                 Re-run with HN_LIVE=1 to fetch from the real Hacker News API."
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

fn render_digest_links(markdown: &str, stories: &[FormattedStory]) -> Result<String, HandlerError> {
    let authored = markdown
        .lines()
        .take_while(|line| line.trim() != "### Links")
        .collect::<Vec<_>>()
        .join("\n");
    let (body, citations) = canonicalize_story_citations(authored.trim(), stories.len())?;
    if citations.is_empty() {
        return Err(HandlerError::Validation(
            "HN digest contains no story footnote citations".to_string(),
        ));
    }

    let mut output = body.trim_end().to_string();
    output.push_str("\n\n### Links\n\n");
    for (index, ordinal) in citations.into_iter().enumerate() {
        if index > 0 {
            output.push('\n');
        }
        output.push_str(&render_story_footnote(ordinal, &stories[ordinal - 1]));
    }
    Ok(output)
}

fn canonicalize_story_citations(
    markdown: &str,
    story_count: usize,
) -> Result<(String, BTreeSet<usize>), HandlerError> {
    let bytes = markdown.as_bytes();
    let mut output = String::with_capacity(markdown.len());
    let mut citations = BTreeSet::new();
    let mut copied_through = 0;
    let mut cursor = 0;

    while cursor < bytes.len() {
        let explicit = bytes[cursor] == b'[' && bytes.get(cursor + 1) == Some(&b'^');
        let parenthesized = bytes[cursor] == b'(';
        let digits_start = cursor + if explicit { 2 } else { 1 };
        let terminator = if explicit { b']' } else { b')' };

        if explicit || parenthesized {
            if let Some((end, ordinal)) = decimal_marker(bytes, digits_start, terminator) {
                if explicit && !(1..=story_count).contains(&ordinal) {
                    return Err(HandlerError::Validation(format!(
                        "HN digest cites story {ordinal}, but the input contains {story_count} stories"
                    )));
                }
                if (1..=story_count).contains(&ordinal) {
                    output.push_str(&markdown[copied_through..cursor]);
                    output.push_str(&format!("[^{ordinal}]"));
                    citations.insert(ordinal);
                    copied_through = end;
                    cursor = end;
                    continue;
                }
            }
        }
        cursor += 1;
    }

    output.push_str(&markdown[copied_through..]);
    Ok((output, citations))
}

fn decimal_marker(bytes: &[u8], start: usize, terminator: u8) -> Option<(usize, usize)> {
    let mut end = start;
    while bytes.get(end).is_some_and(u8::is_ascii_digit) {
        end += 1;
    }
    if end == start || bytes.get(end) != Some(&terminator) {
        return None;
    }
    let ordinal = std::str::from_utf8(&bytes[start..end]).ok()?.parse().ok()?;
    Some((end + 1, ordinal))
}

fn render_story_footnote(ordinal: usize, story: &FormattedStory) -> String {
    let title = markdown_link_label(story.title.trim());
    let article_url = markdown_link_destination(story.url.trim());
    let discussion_url = format!("https://news.ycombinator.com/item?id={}", story.id);

    if story.url.trim() == discussion_url {
        format!("[^{ordinal}]: [{title}](<{article_url}>)")
    } else {
        format!("[^{ordinal}]: [{title}](<{article_url}>) · [HN discussion](<{discussion_url}>)")
    }
}

fn markdown_link_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('[', "\\[")
        .replace(']', "\\]")
}

fn markdown_link_destination(value: &str) -> String {
    value
        .replace(' ', "%20")
        .replace('<', "%3C")
        .replace('>', "%3E")
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

#[cfg(test)]
mod tests {
    use super::super::domain::HnStoryId;
    use super::*;

    fn story(id: u64, title: &str, url: &str) -> FormattedStory {
        FormattedStory {
            id: HnStoryId(id),
            title: title.to_string(),
            url: url.to_string(),
            author: "author".to_string(),
            points: 1,
            comments: 1,
        }
    }

    #[test]
    fn digest_links_are_verified_deduplicated_and_derived_from_story_inputs() {
        let stories = vec![
            story(100, "First", "https://news.ycombinator.com/item?id=100"),
            story(101, "Second [story]", "https://example.com/second story"),
        ];
        let markdown = "## Digest\n\nSecond (2), first [^1], second again [^2].\n\n\
                        ### Links\n\n[^2]: model-authored link";

        assert_eq!(
            render_digest_links(markdown, &stories).expect("render verified links"),
            "## Digest\n\nSecond [^2], first [^1], second again [^2].\n\n\
             ### Links\n\n\
             [^1]: [First](<https://news.ycombinator.com/item?id=100>)\n\
             [^2]: [Second \\[story\\]](<https://example.com/second%20story>) · [HN discussion](<https://news.ycombinator.com/item?id=101>)"
        );
    }

    #[test]
    fn digest_links_reject_missing_and_out_of_range_citations() {
        let stories = vec![story(100, "First", "https://example.com/first")];
        assert!(matches!(
            render_digest_links("## Digest\n\nNo citations.", &stories),
            Err(HandlerError::Validation(message)) if message.contains("no story footnote")
        ));
        assert!(matches!(
            render_digest_links("## Digest\n\nInvalid [^2].", &stories),
            Err(HandlerError::Validation(message)) if message.contains("cites story 2")
        ));
    }
}
