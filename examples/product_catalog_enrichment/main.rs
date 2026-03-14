// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod support;

use obzenflow_infra::application::{Banner, Presentation};

#[cfg(test)]
fn main() -> anyhow::Result<()> {
    support::flow::run_example_in_tests()
}

#[cfg(not(test))]
fn main() -> anyhow::Result<()> {
    let mut banner = Banner::new("Product Catalog Enrichment")
        .description("Demonstrates inner, left, and strict join strategies.")
        .config_block("Join strategies:\n- InnerJoin: Core dimension enrichment (Category->Product->SKU)\n- LeftJoin: Optional promotion enrichment\n- StrictJoin: Critical payment validation (Jonestown Protocol)\n\nBased on industrial-scale product catalog patterns");

    if std::env::var("INJECT_BAD_PAYMENT").is_ok() {
        banner = banner.config_block("INJECT_BAD_PAYMENT is set!\nPipeline will trigger Jonestown Protocol on invalid payment.\nStrictJoin will emit poison EOF and cascade shutdown.");
    }

    let presentation = Presentation::new(banner).with_footer(|outcome| {
        let mut out = outcome.default_footer();
        out.push_str(
            "\n\nTry setting INJECT_BAD_PAYMENT=1 to see StrictJoin trigger Jonestown Protocol!",
        );
        out
    });

    support::flow::run_example(presentation)
}
