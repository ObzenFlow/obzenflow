// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../../../../examples/product_catalog_enrichment"]
mod example {
    pub mod summary_delivery;
}

mod summary_delivery;

#[path = "../../../../examples/product_catalog_enrichment/presentation.rs"]
mod catalog_presentation;

mod presentation_tests {
    use super::catalog_presentation::*;
    use obzenflow_infra::application::ReplayRunContext;
    use obzenflow_infra::application::{RunMode, RunPresentationOutcome};
    use std::path::PathBuf;

    fn replay_mode() -> RunMode {
        RunMode::Replay(ReplayRunContext {
            archive_path: PathBuf::from("target/catalog-logs/flows/flow_01SOURCE"),
            archive_flow_id: Some("flow_01SOURCE".to_string()),
            verify: false,
        })
    }

    #[test]
    fn replay_banner_names_the_archive_and_drops_live_only_guidance() {
        let replay = banner_for(&replay_mode()).render_for_stdout().text;

        assert!(replay.contains("strict replay"));
        assert!(replay.contains("flow_01SOURCE"));
        assert!(replay.contains("Source settings are ignored"));
        assert!(!replay.contains("inject_bad_payment"));
    }

    #[test]
    fn replay_footer_does_not_recommend_an_ignored_source_setting() {
        let replay = footer_for(RunPresentationOutcome::Completed {
            flow_name: "product_catalog_enrichment".to_string(),
            location: None,
            run_mode: replay_mode(),
            offers: Default::default(),
        })
        .finish();

        assert!(replay.contains("separate live run"));
        assert!(!replay.contains("bad_payment"));
    }

    #[test]
    fn live_footer_keeps_the_strict_join_experiment() {
        let live = footer_for(RunPresentationOutcome::Completed {
            flow_name: "product_catalog_enrichment".to_string(),
            location: None,
            run_mode: RunMode::Live,
            offers: Default::default(),
        })
        .finish();

        assert!(live
            .contains("--config examples/product_catalog_enrichment/obzenflow.bad_payment.toml"));
    }
}
