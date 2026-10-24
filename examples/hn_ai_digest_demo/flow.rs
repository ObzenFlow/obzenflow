// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::config::{HnDigestPostgresConfig, HnRunInputs, HnSettings};
use super::decoder::hn_story_decoder;
use super::domain::{FormattedStory, HnStory};
use super::util::truncate_chars;
use anyhow::Result;
use obzenflow::ai::{
    self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec, Chunk,
    EstimateSource, Partials, Prompt, SystemPrompt, UserPrompt,
};
use obzenflow::application::{Banner, FlowApplication, Presentation, RunPresentationOutcome};
use obzenflow::error::HandlerError;
use obzenflow::flow::FlowBuildError;
use obzenflow::flow::{ai, async_source, flow, sink, transform, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::{circuit_breaker, rate_limit};
use obzenflow::schema::TypedPayload;
use obzenflow::stages::sinks::postgres::{PostgresBind, PostgresBindings, PostgresSink};
use obzenflow::stages::sinks::SinkRedeliverySafety;
use obzenflow::stages::sources::{http_pull_config, HttpPullSource};
use obzenflow::stages::{sinks, transforms};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

const HN_SOURCE_BREAKER_FAILURES: u32 = 3;
const HN_SOURCE_BREAKER_COOLDOWN_SECS: u64 = 2;
const HN_AI_BREAKER_FAILURES: u32 = 5;
const HN_DIGEST_TABLE: &str = "hn_digest_summaries";
const HN_DIGEST_INSERT: &str = "(source_mode, source_identity, ai_provider, ai_model, \
token_estimator, interests, story_ids, output_markdown) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
const SYSTEM_PROMPT: &str = "You write concise, skimmable Hacker News digests from a list of headlines + URLs. Be neutral, avoid hype, and do not invent facts beyond what the titles imply.";

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
    groups: usize,
    interests: Option<String>,
    chat_prompt_system: SystemPrompt,
    chat_prompt_user: UserPrompt,
    stories: Vec<FormattedStory>,
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

fn build_digest_postgres_sink(
    config: HnDigestPostgresConfig,
) -> Result<PostgresSink<HnDigestPostgresBinder>> {
    Ok(PostgresSink::builder(HnDigestPostgresBinder)
        .connection(config.connection)
        .insert_into(config.schema, HN_DIGEST_TABLE, HN_DIGEST_INSERT)?
        .batch_size(1)?
        .redelivery_safety(SinkRedeliverySafety::DuplicateSensitive)
        .build()?)
}

#[derive(Clone)]
struct HnDigest {
    interests: Option<String>,
    mode: String,
    base_url: String,
}

impl AiRole<Chunk<FormattedStory>> for HnDigest {
    type Effect = ChatCompletion;
    type Out = HnDigestGroupSummary;

    fn prepare(&self, chunk: &Chunk<FormattedStory>) -> Result<ChatRequestSpec, HandlerError> {
        let lines = chunk
            .numbered()
            .map(|(n, story)| render_story_line(n, story));
        let user = digest_map_prompt(self.interests.as_deref(), chunk.len(), lines);
        Ok(ChatRequestSpec::new(SYSTEM_PROMPT, user.as_str())
            .temperature(0.2)
            .max_tokens(320))
    }

    fn interpret(
        &self,
        _chunk: Chunk<FormattedStory>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<HnDigestGroupSummary, HandlerError> {
        let reply = outcome.replied()?;
        Ok(HnDigestGroupSummary {
            output_markdown: strip_accidental_story_echo(reply.text()),
        })
    }
}

impl AiRole<Partials<FormattedStory, HnDigestGroupSummary>> for HnDigest {
    type Effect = ChatCompletion;
    type Out = HnDigestSummary;

    fn prepare(
        &self,
        input: &Partials<FormattedStory, HnDigestGroupSummary>,
    ) -> Result<ChatRequestSpec, HandlerError> {
        let user = digest_reduce_prompt(self.interests.as_deref(), input.partials());
        Ok(ChatRequestSpec::new(SYSTEM_PROMPT, user.as_str())
            .temperature(0.2)
            .max_tokens(800))
    }

    fn interpret(
        &self,
        input: Partials<FormattedStory, HnDigestGroupSummary>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<HnDigestSummary, HandlerError> {
        let reply = outcome.replied()?;
        let markdown = render_digest_links(reply.text(), input.items())?;
        Ok(self.summary(input, &reply, markdown))
    }
}

impl HnDigest {
    fn summary(
        &self,
        input: Partials<FormattedStory, HnDigestGroupSummary>,
        reply: &ChatCompletionReply,
        output_markdown: String,
    ) -> HnDigestSummary {
        let (stories, group_summaries) = input.into_parts();
        HnDigestSummary {
            mode: self.mode.clone(),
            base_url: self.base_url.clone(),
            ai_provider: reply.observability.provider.to_string(),
            ai_model: reply.observability.model.clone(),
            token_estimator: reply
                .observability
                .estimated_input_resolution
                .as_ref()
                .map_or(EstimateSource::Heuristic, |resolution| resolution.source),
            stories_fetched: stories.len(),
            groups: group_summaries.len(),
            interests: self.interests.clone(),
            chat_prompt_system: SYSTEM_PROMPT.into(),
            chat_prompt_user: UserPrompt::raw(reply.request().user_prompt().unwrap_or_default()),
            stories,
            group_summaries,
            output_markdown,
        }
    }
}

fn digest_map_prompt(
    interests: Option<&str>,
    story_count: usize,
    lines: impl IntoIterator<Item = String>,
) -> UserPrompt {
    let min_citations = story_count.min(6);

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
    p.text_if(interests, |i| format!("My interests: {i}"))
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
        .fenced_lines("Input stories (numbered; do not repeat)", lines);

    p.finish()
}

fn digest_reduce_prompt(
    interests: Option<&str>,
    summaries: &[HnDigestGroupSummary],
) -> UserPrompt {
    let rules: Vec<String> = vec![
        "Do not invent facts that are not implied by the titles.".to_string(),
        "Start the response immediately with \"## What's topical today\" (no intro).".to_string(),
        "Include: Thesis, Themes, Notable stories, Watch.".to_string(),
        "Cite stories only with Markdown footnote markers like [^12]; preserve the story numbers from the chunk summaries and do not use parenthesized citations.".to_string(),
        "Do not add URLs, footnote definitions, or a Links section; the application appends verified links.".to_string(),
        "Avoid generic wrap-ups.".to_string(),
    ];

    let mut p = Prompt::new();
    p.text_if(interests, |i| format!("My interests: {i}"))
        .text("Write a concise Markdown digest of the following Hacker News chunk summaries.")
        .rules(rules)
        .indexed_sections("Chunk summaries", summaries, |idx, summary| {
            (
                format!("Group {idx}"),
                summary.output_markdown.trim().to_string(),
            )
        });

    p.finish()
}

pub(crate) struct HnFlowOptions {
    pub journal_base: std::path::PathBuf,
}

impl Default for HnFlowOptions {
    fn default() -> Self {
        Self {
            journal_base: std::path::PathBuf::from("target/hn-ai-digest-logs"),
        }
    }
}

pub(crate) fn settings_flow_definition() -> FlowDefinition {
    FlowDefinition::materialize_with(|_runtime_config, settings: HnSettings| {
        let inputs = settings
            .into_inputs()
            .map_err(|error| FlowBuildError::StageResourcesFailed(format!("{error:#}")))?;
        Ok(build_flow_definition(inputs, HnFlowOptions::default()))
    })
}

pub(crate) fn build_flow_definition(inputs: HnRunInputs, options: HnFlowOptions) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let HnRunInputs {
            max_stories,
            poll_timeout_secs,
            source_rate_limit,
            max_stories_per_group,
            interests,
            mode_label,
            base_url,
        } = inputs;
        let HnFlowOptions { journal_base } = options;

        // The mock server binds an ephemeral loopback port for each process.
        // Its guard stays in the host; only this owned endpoint enters the
        // source. Durable output names the logical mock source.
        let base_url_for_summary = if mode_label == "mock" {
            "mock://hacker-news/".to_string()
        } else {
            base_url.to_string()
        };

        let roles = HnDigest {
            interests,
            mode: mode_label,
            base_url: base_url_for_summary,
        };
        let digest = ai::map(roles.clone())
            .chunks(max_stories_per_group)
            .reduce(roles)
            .on_eof();
        let ai_breaker = circuit_breaker().consecutive_failures(HN_AI_BREAKER_FAILURES);
        let decoder = hn_story_decoder(base_url, max_stories);
        let http_source_config = http_pull_config()
            .map_err(|error| {
                FlowBuildError::StageResourcesFailed(format!(
                    "HN HTTP source client unavailable: {error}"
                ))
            })?
            .max_batch_size(10)
            .poll_timeout(Duration::from_secs(poll_timeout_secs))
            .build()
            .map_err(|error| {
                FlowBuildError::StageResourcesFailed(format!(
                    "HN HTTP source configuration failed: {error}"
                ))
            })?;
        let hn_source = HttpPullSource::new(decoder, http_source_config);
        let formatter = transforms::map(|story: HnStory| format_story(story));
        let source_breaker = circuit_breaker()
            .consecutive_failures(HN_SOURCE_BREAKER_FAILURES)
            .open_for(Duration::from_secs(HN_SOURCE_BREAKER_COOLDOWN_SECS));
        let source_limiter = rate_limit(source_rate_limit);
        let console_sink = sinks::ConsoleSink::new(format_digest_summary_for_console);
        let postgres_sink = HnDigestPostgresConfig::from_env()
            .and_then(build_digest_postgres_sink)
            .map_err(|error| {
                FlowBuildError::StageResourcesFailed(format!(
                    "HN digest PostgreSQL sink configuration failed: {error}"
                ))
            })?;

        Ok(flow! {
            name: "hn_ai_digest_demo",
            journals: disk_journals(journal_base),

            stages: {
                // Source-boundary policies (FLOWIP-115a): the breaker protects
                // the external HN HTTP dependency; the limiter paces API reads.
                // Replay reconstructs archived stories and suppresses both.
                hn_stories = async_source!(HnStory with {
                    source_breaker,
                    source_limiter
                } => hn_source);
                formatter = transform!(HnStory -> FormattedStory => formatter);
                digest = ai!(FormattedStory -> HnDigestSummary with ai_breaker => digest);
                digest_summary = sink!(
                    HnDigestSummary => handler_set!(console_sink, postgres_sink)
                )?;
            },

            topology: {
                hn_stories |> formatter;
                formatter |> digest;
                digest |> digest_summary;
            }
        })
    })
}

pub async fn run_example(presentation: Presentation) -> Result<()> {
    FlowApplication::builder()
        .with_presentation(presentation)
        .run_async(settings_flow_definition())
        .await?;
    Ok(())
}

pub fn run_demo_blocking() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(run_example(build_presentation()))
}

pub(crate) fn build_presentation() -> Presentation {
    Presentation::new(
        Banner::new("HN AI Digest Demo")
            .description(
                "Fetch top HN stories, then generate a markdown digest via Rig-backed LLM transforms.",
            )
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
                 Set source = \"live\" under [app] to fetch from the real Hacker News API."
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
    out.push_str(&format!("groups: {}\n", summary.groups));

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
    for (n, story) in summary.stories.iter().enumerate() {
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

fn render_story_line(n: u64, story: &FormattedStory) -> String {
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
    fn postgres_sink_description_is_truthful_without_selection_instrumentation() {
        let connection = obzenflow::stages::sinks::postgres::PostgresConnection::from_url(
            "postgres://obzenflow:sentinel@localhost/obzenflow?sslmode=verify-full",
            obzenflow::stages::sinks::postgres::PostgresTransport::VerifiedTls,
        )
        .expect("cold verified-TLS fixture URL is valid without I/O");
        let sink = build_digest_postgres_sink(HnDigestPostgresConfig {
            connection,
            schema: "hn_digest_fixture".to_string(),
        })
        .expect("the locked PostgreSQL sink operation builds");
        let description = obzenflow::stages::sinks::SinkConnector::describe(&sink);
        assert_eq!(
            description.destination_name(),
            Some("postgres.hn_digest_fixture.hn_digest_summaries")
        );
        assert_eq!(
            description.default_method(),
            &obzenflow::stages::sinks::DeliveryMethod::DatabaseInsert {
                table: "hn_digest_fixture.hn_digest_summaries".to_string(),
            }
        );
        assert_eq!(
            description.redelivery_safety(),
            Some(SinkRedeliverySafety::DuplicateSensitive)
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
