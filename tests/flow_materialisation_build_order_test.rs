// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133b application-level ordering witnesses for materialised flows.

#![cfg(feature = "ai")]

use obzenflow::typed::{sinks, sources};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::ai::ChatEffectBinding;
use obzenflow_infra::application::{ApplicationError, FlowApplication};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::bootstrap_config;
use obzenflow_runtime::effects::EffectPortRegistry;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const GENERIC_FAILURE_SENTINEL: &str = "FLOWIP-133b materializer sentinel";
const UNSET_SECRET_ENV: &str = "FLOWIP_133B_BUILD_ORDER_MUST_NOT_RESOLVE_THIS_SECRET";

#[derive(Clone, Copy, Debug)]
enum FailureKind {
    MissingModel,
    InvalidEndpoint,
    GenericAfterRegistration,
}

impl FailureKind {
    fn slug(self) -> &'static str {
        match self {
            Self::MissingModel => "missing-model",
            Self::InvalidEndpoint => "invalid-endpoint",
            Self::GenericAfterRegistration => "generic-after-registration",
        }
    }

    fn ai_config(self) -> &'static str {
        match self {
            Self::MissingModel => r#"provider = "ollama""#,
            Self::InvalidEndpoint => {
                r#"provider = "openai_compatible"
model = "fixture-model"
base_url = "not a valid URL""#
            }
            Self::GenericAfterRegistration => {
                r#"provider = "openai"
model = "fixture-model"
api_key_env = "FLOWIP_133B_BUILD_ORDER_MUST_NOT_RESOLVE_THIS_SECRET""#
            }
        }
    }

    fn expected_error(self) -> &'static str {
        match self {
            Self::MissingModel => {
                "ai.models.model is required for the single-target ChatEffectBinding"
            }
            Self::InvalidEndpoint => "invalid ai.models.base_url",
            Self::GenericAfterRegistration => GENERIC_FAILURE_SENTINEL,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ProbeEvent;

impl TypedPayload for ProbeEvent {
    const EVENT_TYPE: &'static str = "test.flowip_133b.build_order.probe";
}

fn materialised_failure_probe(
    kind: FailureKind,
    marker: u64,
    journal_base: PathBuf,
    factory_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |runtime_config| {
        factory_calls.fetch_add(1, Ordering::SeqCst);

        // The host bootstrap must already be installed when materialisation starts.
        let bootstrap = bootstrap_config();
        assert_eq!(bootstrap.shutdown_timeout, Duration::from_secs(marker));
        assert!(!bootstrap.metrics.enabled);

        let binding =
            ChatEffectBinding::from_config(&runtime_config.ai_models()).map_err(|error| {
                FlowBuildError::BindingConfiguration {
                    binding: "chat".to_string(),
                    detail: error.to_string(),
                }
            })?;
        let (_chat, registration) = binding.into_parts();
        let effect_ports = registration
            .install_into(EffectPortRegistry::new())
            .map_err(|error| FlowBuildError::BindingConfiguration {
                binding: "chat".to_string(),
                detail: error.to_string(),
            })?;

        if matches!(kind, FailureKind::GenericAfterRegistration) {
            // Registration is deliberately deferred: reaching this sentinel proves
            // that neither secret lookup nor client construction ran eagerly.
            return Err(FlowBuildError::StageResourcesFailed(
                GENERIC_FAILURE_SENTINEL.to_string(),
            ));
        }

        let input = sources::finite(Vec::<ProbeEvent>::new());
        let output = sinks::debug::<ProbeEvent>();

        Ok(flow! {
            name: "flowip_133b_build_order_probe",
            journals: disk_journals(journal_base),
            effect_ports,

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

fn write_config(path: &Path, marker: u64, ai_config: &str) {
    std::fs::write(
        path,
        format!(
            r#"[runtime]
shutdown_timeout_secs = {marker}

[metrics]
enabled = false

[ai.models]
{ai_config}
"#
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
        (FailureKind::InvalidEndpoint, 42_u64),
        (FailureKind::GenericAfterRegistration, 43_u64),
    ];

    // Keep the three host runs sequential because bootstrap installation is
    // intentionally process-global and guarded against overlapping runs.
    for (kind, marker) in cases {
        let case_root = tempdir.path().join(kind.slug());
        std::fs::create_dir_all(&case_root).expect("create build-order case directory");
        let config_path = case_root.join("obzenflow.toml");
        let journal_base = case_root.join("journals-must-not-exist");
        write_config(&config_path, marker, kind.ai_config());

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
