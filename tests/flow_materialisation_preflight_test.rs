// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133a host-preflight witnesses for deferred handler construction.

use obzenflow::sources;
use obzenflow::typed::sinks;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::{ApplicationError, FlowApplication};
use obzenflow_infra::journal::disk_journals;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ProbeEvent;

impl TypedPayload for ProbeEvent {
    const EVENT_TYPE: &'static str = "test.flowip_133a.preflight.probe";
}

fn workspace_tempdir(prefix: &str) -> tempfile::TempDir {
    let target = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target");
    std::fs::create_dir_all(&target).expect("create workspace target directory");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(target)
        .expect("create workspace-local preflight tempdir")
}

fn materialised_probe(
    journal_root: PathBuf,
    constructor_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        constructor_calls.fetch_add(1, Ordering::SeqCst);

        let input = sources::finite(Vec::<ProbeEvent>::new());
        let output = sinks::debug::<ProbeEvent>();

        Ok(flow! {
            name: "flowip_133a_preflight_probe",
            journals: disk_journals(journal_root),

            stages: {
                input = source!(ProbeEvent => input);
                output = sink!(ProbeEvent => output);
            },

            topology: {
                input |> output;
            }
        })
    })
}

#[tokio::test]
async fn invalid_configuration_does_not_construct_handlers_or_select_a_run_substrate() {
    let tempdir = workspace_tempdir("flowip-133a-invalid-config-");
    let config_path = tempdir.path().join("invalid.toml");
    std::fs::write(&config_path, "[runtime\nshutdown_timeout_secs = 1")
        .expect("write invalid config fixture");
    let journal_root = tempdir.path().join("journals-must-not-exist");
    let constructor_calls = Arc::new(AtomicUsize::new(0));
    let flow = materialised_probe(journal_root.clone(), Arc::clone(&constructor_calls));

    let result = FlowApplication::builder()
        .with_config_file(config_path)
        .with_cli_args(["flowip-133a-invalid-config"])
        .run_async(flow)
        .await;

    assert!(
        matches!(&result, Err(ApplicationError::InvalidConfiguration(_))),
        "invalid configuration returned the wrong application result: {result:?}"
    );
    assert_eq!(constructor_calls.load(Ordering::SeqCst), 0);
    assert!(!journal_root.exists());
}

#[tokio::test]
async fn invalid_replay_input_does_not_construct_handlers_or_select_a_run_substrate() {
    let tempdir = workspace_tempdir("flowip-133a-invalid-replay-");
    let config_path = tempdir.path().join("obzenflow.toml");
    std::fs::write(&config_path, "").expect("write replay preflight config fixture");
    let missing_archive = tempdir.path().join("missing-replay-archive");
    let journal_root = tempdir.path().join("journals-must-not-exist");
    let constructor_calls = Arc::new(AtomicUsize::new(0));
    let flow = materialised_probe(journal_root.clone(), Arc::clone(&constructor_calls));

    let result = FlowApplication::builder()
        .with_config_file(config_path)
        .with_cli_args([
            "flowip-133a-invalid-replay",
            "--replay-from",
            missing_archive
                .to_str()
                .expect("temporary archive path must be UTF-8"),
        ])
        .run_async(flow)
        .await;

    assert!(
        matches!(&result, Err(ApplicationError::InvalidConfiguration(_))),
        "invalid replay input returned the wrong application result: {result:?}"
    );
    assert_eq!(constructor_calls.load(Ordering::SeqCst), 0);
    assert!(!journal_root.exists());
}
