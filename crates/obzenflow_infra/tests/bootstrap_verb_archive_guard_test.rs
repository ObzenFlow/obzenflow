// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120u guard: the replay/resume verb and the opened input archive
//! travel as separate bootstrap fields, so an installed verb without an
//! archive is representable and must fail the build loudly rather than
//! silently selecting a live run.
//!
//! One test per binary: the bootstrap install is process-global, so a sibling
//! test building concurrently would see this test's verb.

use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::{
    install_bootstrap_config, BootstrapConfig, ReplayBootstrap, ReplayVerb,
};
use obzenflow_runtime::stages::sink::SinkTyped;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GuardEvent;
impl TypedPayload for GuardEvent {
    const EVENT_TYPE: &'static str = "test.verb_archive_guard";
    const SCHEMA_VERSION: u32 = 1;
}

#[tokio::test]
async fn installed_replay_verb_without_opened_archive_fails_the_build() {
    let _bootstrap = install_bootstrap_config(BootstrapConfig {
        replay: Some(ReplayBootstrap {
            archive_path: std::path::PathBuf::from("/nonexistent/archive"),
            allow_incomplete_archive: true,
            allow_duplicate_sink_delivery: false,
            verb: ReplayVerb::Resume,
        }),
        replay_archive: None,
        ..Default::default()
    });

    // Disk journals pass the F13 ephemeral-resume refusal and the declared
    // sink passes the 120n F16 gate, so the build reaches the
    // strategy-selection point this guard sits at.
    let base = tempfile::tempdir().expect("tempdir");
    let journals = disk_journals(base.path().to_path_buf());
    let failure = flow! {
        name: "verb_without_archive",
        journals: journals,
        middleware: [],

        stages: {
            src = source!(GuardEvent => placeholder!());
            snk = sink!(
                GuardEvent => SinkTyped::new(|_value: GuardEvent| async move {}),
                delivery: idempotent
            );
        },

        topology: {
            src |> snk;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .err()
    .expect("an installed verb without an opened archive must fail the build");

    assert!(
        failure.error.to_string().contains("no opened archive"),
        "the failure must name the missing archive: {failure}"
    );
}
