// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Example entrypoint and application hosting for the HN AI digest demo.

use super::config::HnSettings;
use super::flow::{
    build_flow_definition, HnFlowOptions, HN_SOURCE_BREAKER_COOLDOWN_SECS,
    HN_SOURCE_BREAKER_FAILURES,
};
use anyhow::Result;
use obzenflow::application::{Banner, FlowApplication, Presentation, RunPresentationOutcome};
use obzenflow::flow::{FlowBuildError, FlowDefinition};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) fn settings_flow_definition(reads_mock: Arc<AtomicBool>) -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, settings: HnSettings| {
        let inputs = settings
            .into_inputs()
            .map_err(|error| FlowBuildError::StageResourcesFailed(format!("{error:#}")))?;
        reads_mock.store(inputs.mode_label == "mock", Ordering::SeqCst);
        Ok(build_flow_definition(inputs, HnFlowOptions::default()))
    })
}

pub async fn run_example(presentation: Presentation, reads_mock: Arc<AtomicBool>) -> Result<()> {
    FlowApplication::builder()
        .with_presentation(presentation)
        .run_async(settings_flow_definition(reads_mock))
        .await?;
    Ok(())
}

pub fn run_demo_blocking() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let reads_mock = Arc::new(AtomicBool::new(false));
    runtime.block_on(run_example(
        build_presentation(reads_mock.clone()),
        reads_mock,
    ))
}

pub(crate) fn build_presentation(reads_mock: Arc<AtomicBool>) -> Presentation {
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
    .with_footer(move |outcome| {
        let is_success = matches!(&outcome, RunPresentationOutcome::Completed { .. });
        let footer = outcome.into_footer();
        if !is_success {
            footer
        } else if reads_mock.load(Ordering::SeqCst) {
            footer.paragraph(
                "The generated digest was delivered by the configured sink handler.\n\
                 Set source = \"live\" under [app] to fetch from the real Hacker News API.",
            )
        } else {
            footer.paragraph("The generated digest was delivered by the configured sink handler.")
        }
    })
}
