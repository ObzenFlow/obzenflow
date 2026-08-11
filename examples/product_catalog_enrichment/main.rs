// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod support;

#[cfg(not(test))]
use obzenflow_infra::application::Presentation;
use obzenflow_infra::application::{Banner, Footer, RunMode, RunPresentationOutcome};

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

fn banner_for(mode: &RunMode) -> Banner {
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

fn footer_for(outcome: RunPresentationOutcome) -> Footer {
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

#[cfg(test)]
fn main() -> anyhow::Result<()> {
    support::flow::run_example_in_tests()
}

#[cfg(not(test))]
fn main() -> anyhow::Result<()> {
    let presentation = Presentation::for_mode(banner_for).with_footer(footer_for);

    support::flow::run_example(presentation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_infra::application::ReplayRunContext;
    use std::path::PathBuf;

    fn replay_mode() -> RunMode {
        RunMode::Replay(ReplayRunContext {
            archive_path: PathBuf::from("target/catalog-logs/flows/flow_01SOURCE"),
            archive_flow_id: Some("flow_01SOURCE".to_string()),
        })
    }

    #[test]
    fn replay_banner_names_the_archive_and_drops_live_only_guidance() {
        let replay = banner_for(&replay_mode()).render_for_stdout().text;

        assert!(replay.contains("strict replay"));
        assert!(replay.contains("flow_01SOURCE"));
        assert!(replay.contains("environment variables are ignored"));
        assert!(!replay.contains("INJECT_BAD_PAYMENT is set"));
    }

    #[test]
    fn replay_footer_does_not_recommend_an_ignored_environment_variable() {
        let replay = footer_for(RunPresentationOutcome::Completed {
            flow_name: "product_catalog_enrichment".to_string(),
            location: None,
            run_mode: replay_mode(),
        })
        .finish();

        assert!(replay.contains("separate live run"));
        assert!(!replay.contains("INJECT_BAD_PAYMENT"));
    }

    #[test]
    fn live_footer_keeps_the_strict_join_experiment() {
        let live = footer_for(RunPresentationOutcome::Completed {
            flow_name: "product_catalog_enrichment".to_string(),
            location: None,
            run_mode: RunMode::Live,
        })
        .finish();

        assert!(live.contains("INJECT_BAD_PAYMENT=1"));
    }
}
