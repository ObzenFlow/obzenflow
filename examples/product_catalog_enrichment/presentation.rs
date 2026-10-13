// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::application::{Banner, Footer, RunMode, RunPresentationOutcome};

const BAD_PAYMENT_CONFIG: &str = "examples/product_catalog_enrichment/obzenflow.bad_payment.toml";

fn live_banner() -> Banner {
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
        .section(
            "Final summary",
            "An application-owned control permits one live dashboard delivery attempt per summary sink; repeated attempts fail before printing.",
        )
        .section(
            "Fault injection",
            "With [app] inject_bad_payment = true, one order names an unknown payment method.\nStrictJoin triggers the Jonestown Protocol on it: it preserves the valid committed prefix, then emits a sealed Poison EOF.",
        )
}

pub fn banner_for(mode: &RunMode) -> Banner {
    match mode {
        RunMode::Replay(ctx) => Banner::new("Product Catalog Enrichment (strict replay)")
            .description("Reconstructing archived catalog inputs and deterministic join outputs.")
            .bullets(
                "What this replay does",
                [
                    format!("Source archive: {}", ctx.source_label()),
                    "Source settings are ignored; recorded catalog and order facts are re-admitted"
                        .to_string(),
                    "Inner, left, and strict joins are recomputed from the recorded inputs"
                        .to_string(),
                    "The analytics summary is rebuilt and idempotent console deliveries run again"
                        .to_string(),
                ],
            ),
        _ => live_banner(),
    }
}

pub fn footer_for(outcome: RunPresentationOutcome) -> Footer {
    let next_step = match outcome.run_mode() {
        RunMode::Replay(_) => {
            "Next: inspect the replay journal or replay this replay; use a separate live run to exercise the strict-join poison path.".to_string()
        }
        _ => format!(
            "Try --config {BAD_PAYMENT_CONFIG} to see StrictJoin trigger the Jonestown Protocol!"
        ),
    };

    outcome.into_footer().paragraph(next_step)
}
