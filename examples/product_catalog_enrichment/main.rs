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
    let inject_bad_payment = std::env::var("INJECT_BAD_PAYMENT").is_ok();

    let banner = Banner::new("Product Catalog Enrichment")
        .description("Demonstrates inner, left, and strict join strategies.")
        .bullets(
            "Join strategies",
            [
                "InnerJoin: Core dimension enrichment (Category->Product->SKU)",
                "LeftJoin: Optional promotion enrichment",
                "StrictJoin: Critical payment validation (Jonestown Protocol)",
            ],
        )
        .section("Background", "Based on industrial-scale product catalog patterns")
        .config_block_if(
            inject_bad_payment,
            "INJECT_BAD_PAYMENT is set!\nPipeline will trigger Jonestown Protocol on invalid payment.\nStrictJoin will emit poison EOF and cascade shutdown.",
        );

    let presentation = Presentation::new(banner).with_footer(|outcome| {
        outcome.into_footer().paragraph(
            "Try setting INJECT_BAD_PAYMENT=1 to see StrictJoin trigger Jonestown Protocol!",
        )
    });

    support::flow::run_example(presentation)
}
