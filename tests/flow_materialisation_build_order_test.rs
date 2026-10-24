// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133b application-level ordering witnesses for materialised flows.

#![cfg(feature = "ai")]

use obzenflow::ai::{self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec};
use obzenflow::stages::sinks;
use obzenflow::stages::sources;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{ai, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::{ApplicationError, FlowApplication};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::bootstrap_config;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const UNSET_SECRET_ENV: &str = "FLOWIP_133B_BUILD_ORDER_MUST_NOT_RESOLVE_THIS_SECRET";

#[derive(Clone, Copy, Debug)]
enum FailureKind {
    MissingModel,
    UnknownEndpoint,
    FailureAfterBinding,
}

impl FailureKind {
    fn slug(self) -> &'static str {
        match self {
            Self::MissingModel => "missing-model",
            Self::UnknownEndpoint => "unknown-endpoint",
            Self::FailureAfterBinding => "failure-after-binding",
        }
    }

    fn ai_config(self) -> String {
        match self {
            Self::MissingModel => "[ai.models]\nendpoint = \"ollama\"\n".to_string(),
            Self::UnknownEndpoint => {
                "[ai.models]\nendpoint = \"grok\"\nmodel = \"fixture-model\"\n".to_string()
            }
            Self::FailureAfterBinding => format!(
                "[ai.endpoints.compat]\nprovider = \"openai_compatible\"\n\
                 base_url = \"http://127.0.0.1:9/v1\"\napi_key_env = \"{UNSET_SECRET_ENV}\"\n\n\
                 [ai.models]\nendpoint = \"compat\"\nmodel = \"fixture-model\"\n"
            ),
        }
    }

    fn expected_error(self) -> &'static str {
        match self {
            Self::MissingModel => {
                "stage `brief`: no model configured for obzenflow.ai.chat_completion; set ai.models.model, or model under [ai.models.stages.brief]"
            }
            Self::UnknownEndpoint => {
                "config: stage `brief` names endpoint `grok`, which is not defined"
            }
            Self::FailureAfterBinding => "topology references unknown binding 'unbound'",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ProbeEvent;

impl TypedPayload for ProbeEvent {
    const EVENT_TYPE: &'static str = "test.flowip_133b.build_order.probe";
}

#[derive(Clone, Copy)]
struct Echo;

impl AiRole<ProbeEvent> for Echo {
    type Effect = ChatCompletion;
    type Out = ProbeEvent;

    fn prepare(&self, _input: &ProbeEvent) -> Result<ChatRequestSpec, HandlerError> {
        Ok(ChatRequestSpec::new("Echo.", "probe"))
    }

    fn interpret(
        &self,
        input: ProbeEvent,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<ProbeEvent, HandlerError> {
        outcome.replied()?;
        Ok(input)
    }
}

fn materialised_failure_probe(
    kind: FailureKind,
    marker: u64,
    journal_base: PathBuf,
    factory_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        factory_calls.fetch_add(1, Ordering::SeqCst);

        // The host bootstrap must already be installed when materialisation starts.
        let bootstrap = bootstrap_config();
        assert_eq!(bootstrap.shutdown_timeout, Duration::from_secs(marker));

        let input = sources::ValuesSource::new(Vec::<ProbeEvent>::new());
        let brief = ai::infer(Echo);
        let output = sinks::ConsoleSink::<ProbeEvent, _>::new(sinks::DebugFormatter);

        if matches!(kind, FailureKind::FailureAfterBinding) {
            // The AI stage binds before topology validation runs, so reaching
            // this topology error proves that binding neither resolved the
            // unset secret nor constructed a client.
            return Ok(flow! {
                name: "flowip_133b_build_order_probe",
                journals: disk_journals(journal_base),
                stages: {
                    input = source!(ProbeEvent => input);
                    brief = ai!(ProbeEvent -> ProbeEvent => brief);
                    output = sink!(ProbeEvent => output);
                },

                topology: {
                    input |> brief;
                    brief |> unbound;
                }
            });
        }

        Ok(flow! {
            name: "flowip_133b_build_order_probe",
            journals: disk_journals(journal_base),
            stages: {
                input = source!(ProbeEvent => input);
                brief = ai!(ProbeEvent -> ProbeEvent => brief);
                output = sink!(ProbeEvent => output);
            },

            topology: {
                input |> brief;
                brief |> output;
            }
        })
    })
}

fn write_config(path: &Path, marker: u64, ai_config: &str) {
    std::fs::write(
        path,
        format!(
            r#"[runtime]
shutdown_timeout_secs = {marker}

[metrics]
enabled = false

{ai_config}"#
        ),
    )
    .expect("write build-order test config");
}

#[tokio::test]
async fn materialisation_runs_after_bootstrap_and_before_run_substrate_creation() {
    assert!(
        std::env::var_os(UNSET_SECRET_ENV).is_none(),
        "the deferred-resolution witness requires {UNSET_SECRET_ENV} to be unset"
    );

    let baseline_bootstrap = bootstrap_config();
    let tempdir = tempfile::tempdir().expect("build-order tempdir");
    let cases = [
        (FailureKind::MissingModel, 41_u64),
        (FailureKind::UnknownEndpoint, 42_u64),
        (FailureKind::FailureAfterBinding, 43_u64),
    ];

    // Keep the three host runs sequential because bootstrap installation is
    // intentionally process-global and guarded against overlapping runs.
    for (kind, marker) in cases {
        let case_root = tempdir.path().join(kind.slug());
        std::fs::create_dir_all(&case_root).expect("create build-order case directory");
        let config_path = case_root.join("obzenflow.toml");
        let journal_base = case_root.join("journals-must-not-exist");
        write_config(&config_path, marker, &kind.ai_config());

        let factory_calls = Arc::new(AtomicUsize::new(0));
        let flow = materialised_failure_probe(
            kind,
            marker,
            journal_base.clone(),
            Arc::clone(&factory_calls),
        );

        let result = FlowApplication::builder()
            .with_config_file(config_path)
            .with_cli_args(["flowip-133b-build-order-test"])
            .run_async(flow)
            .await;

        let detail = match result {
            Err(ApplicationError::FlowBuildFailed(detail)) => detail,
            other => panic!("{kind:?} returned the wrong application result: {other:?}"),
        };
        assert!(
            detail.contains(kind.expected_error()),
            "{kind:?} returned an unexpected build error: {detail}"
        );
        assert_eq!(
            factory_calls.load(Ordering::SeqCst),
            1,
            "{kind:?} factory must run exactly once"
        );
        assert!(
            !journal_base.exists(),
            "{kind:?} selected a run substrate before materialisation failed"
        );
        assert_eq!(
            bootstrap_config(),
            baseline_bootstrap,
            "{kind:?} did not restore the host bootstrap after failure"
        );
    }
}
