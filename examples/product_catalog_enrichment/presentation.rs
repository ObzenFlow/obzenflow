// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::application::{Banner, RunMode};

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
            "Run with --config examples/product_catalog_enrichment/obzenflow.bad_payment.toml to set [app] inject_bad_payment = true. One order then names an unknown payment method.\nStrictJoin triggers the Jonestown Protocol on it: it preserves the valid committed prefix, then emits a sealed Poison EOF.",
        )
}

pub fn banner_for(mode: &RunMode) -> Banner {
    match mode {
        RunMode::Replay(_) => Banner::new("Product Catalog Enrichment (strict replay)")
            .description("Reconstructing archived catalog inputs and deterministic join outputs.")
            .bullets(
                "What this replay does",
                [
                    "Inner, left, and strict joins are recomputed from the recorded inputs",
                    "The replay rebuilds the analytics summary and does not call the console sinks \
                     again",
                ],
            ),
        _ => live_banner(),
    }
}
