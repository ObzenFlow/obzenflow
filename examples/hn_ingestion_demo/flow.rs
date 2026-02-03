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
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::TypedPayload;
use obzenflow_dsl_infra::{async_source, flow, sink, transform};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::http_client::default_http_client;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::transform::TryMapWith;
use std::time::Duration;

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

    println!("HN Ingestion Demo (FLOWIP-084f)");
    println!("================================");
    println!("Validates HTTP pull source (084e) with an API pull pattern");
    println!();
    println!("  mode: {mode_label}");
    println!("  base_url: {base_url}");
    println!("  max_stories: {max_stories}");
    println!("  poll_timeout: {poll_timeout_secs}s");
    println!("  pattern: source -> transform -> sink");
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

    FlowApplication::builder()
        .with_console_subscriber()
        .with_log_level(LogLevel::Info)
        .run_async(flow! {
            name: "hn_ingestion_demo",
            journals: disk_journals(std::path::PathBuf::from("target/hn-ingestion-logs")),
            middleware: [],

            stages: {
                source = async_source!("hn_stories" => (
                    HttpPullSource::new(decoder, config),
                    Some(Duration::from_secs(poll_timeout_secs as u64))
                ));
                formatter = transform!("formatter" => formatter);
                output = sink!("console" => ConsoleSink::<FormattedStory>::table(
                    &["#", "Title", "Author", "Pts", "URL"],
                    |story| vec![
                        story.id.to_string(),
                        truncate_chars(&story.title, 60),
                        story.author.clone(),
                        story.points.to_string(),
                        truncate_chars(&story.url, 40),
                    ]
                ));
            },

            topology: {
                source |> formatter;
                formatter |> output;
            }
        })
        .await?;

    println!();
    println!("Demo completed!");
    println!("Journal written to: target/hn-ingestion-logs/");

    Ok(())
}

fn format_story_event(
    event: obzenflow_core::ChainEvent,
) -> std::result::Result<obzenflow_core::ChainEvent, String> {
    if !HnStory::event_type_matches(&event.event_type()) {
        return Ok(event);
    }

    let story = HnStory::try_from_event(&event).map_err(|e| e.to_string())?;
    let formatted = FormattedStory {
        id: story.id,
        title: story
            .title
            .unwrap_or_else(|| "(untitled)".to_string())
            .to_uppercase(),
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
