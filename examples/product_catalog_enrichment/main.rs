// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod presentation;
mod support;

use obzenflow_infra::application::{FlowApplication, LogLevel, Presentation};

fn main() -> anyhow::Result<()> {
    let presentation =
        Presentation::for_mode(presentation::banner_for).with_footer(presentation::footer_for);

    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation)
        .with_config_file("examples/product_catalog_enrichment/obzenflow.toml")
        .run_blocking(support::flow::build_flow("target/catalog-logs".into()))?;

    Ok(())
}
