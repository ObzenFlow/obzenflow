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
use obzenflow::application::{Banner, FlowApplication, Presentation};
use obzenflow::flow::FlowDefinition;
use tokio::runtime::Builder;

pub(crate) fn settings_flow_definition() -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, settings: HnSettings| {
        let inputs = settings.into_inputs();
        Ok(build_flow_definition(inputs, HnFlowOptions::default()))
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
    let runtime = Builder::new_multi_thread().enable_all().build()?;

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
}
