// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::application::{Banner, Footer, RunMode, RunPresentationOutcome};

fn live_banner() -> Banner {
    let inject_bad_payment = std::env::var("INJECT_BAD_PAYMENT").is_ok();

    Banner::new("Product Catalog Enrichment")
        .description("Demonstrates inner, left, and strict join strategies.")
        .bullets(
            "Join strategies",
            [
                "InnerJoin: Core dimension enrichment (Category->Product->SKU)",
                "LeftJoin: Optional promotion enrichment",
                "StrictJoin: Critical payment validation (Jonestown Protocol)",
            ],
        )
        .section(
            "Background",
            "Based on industrial-scale product catalog patterns",
        )
        .config_block_if(
            inject_bad_payment,
            "INJECT_BAD_PAYMENT is set!\nStrictJoin will trigger the Jonestown Protocol on the invalid payment.\nIt preserves the valid committed prefix, then emits a sealed Poison EOF.",
        )
}

pub fn banner_for(mode: &RunMode) -> Banner {
    match mode {
        RunMode::Replay(ctx) => Banner::new("Product Catalog Enrichment (strict replay)")
            .description(
                "Reconstructing archived catalog inputs and deterministic join outputs.",
            )
            .bullets(
                "What this replay does",
                [
                    format!("Source archive: {}", ctx.source_label()),
                    "Source configuration and environment variables are ignored; recorded catalog and order facts are re-admitted".to_string(),
                    "Inner, left, and strict joins are recomputed from the recorded inputs".to_string(),
                    "The analytics summary is rebuilt and idempotent console deliveries run again".to_string(),
                ],
            )
            .config("journal_dir", "target/catalog-logs"),
        _ => live_banner(),
    }
}

pub fn footer_for(outcome: RunPresentationOutcome) -> Footer {
    let next_step = match outcome.run_mode() {
        RunMode::Replay(_) => {
            "Next: inspect the replay journal or replay this replay; use a separate live run to exercise the strict-join poison path."
        }
        _ => {
            "Try setting INJECT_BAD_PAYMENT=1 to see StrictJoin trigger the Jonestown Protocol!"
        }
    };

    outcome.into_footer().paragraph(next_step)
}
