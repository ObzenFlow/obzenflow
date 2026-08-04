// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115g drift guards for the destructive handler-shell contraction.

use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources_under(path: &Path, output: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(path).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            rust_sources_under(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}

#[test]
fn generic_handler_shell_and_standalone_retry_vocabulary_stay_absent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![root.join("src/lib.rs")];
    for crate_entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        let source_root = crate_entry.expect("read crate entry").path().join("src");
        if source_root.is_dir() {
            rust_sources_under(&source_root, &mut sources);
        }
    }

    let forbidden = [
        "MiddlewareTransform",
        "UnifiedMiddlewareTransform",
        "AsyncMiddlewareTransform",
        "AsyncTransformHandlerExt",
        "AsyncTransformMiddlewareBuilder",
        "MiddlewareStateful",
        "TransformHandlerExt",
        "StatefulHandlerMiddlewareExt",
        "TransformMiddlewareBuilder",
        "StatefulMiddlewareBuilder",
        "MiddlewareAsyncFiniteSource",
        "MiddlewareAsyncInfiniteSource",
        "MiddlewareFiniteSource",
        "MiddlewareInfiniteSource",
        "AsyncFiniteSourceHandlerExt",
        "AsyncInfiniteSourceHandlerExt",
        "FiniteSourceHandlerExt",
        "InfiniteSourceHandlerExt",
        "AsyncFiniteSourceMiddlewareBuilder",
        "AsyncInfiniteSourceMiddlewareBuilder",
        "FiniteSourceMiddlewareBuilder",
        "InfiniteSourceMiddlewareBuilder",
        "MiddlewareJoin",
        "JoinHandlerMiddlewareExt",
        "JoinMiddlewareBuilder",
        "MiddlewareSink",
        "SinkHandlerExt",
        "SinkMiddlewareBuilder",
        "MiddlewareAction",
        "ErrorAction",
        "legacy_shell",
        "create_for_effect",
        "TopologyMiddlewareConfigSlot::Retry",
        "MiddlewareKind",
        "MiddlewareLifecycle::Retry",
        "RetryEvent",
        "lifecycle.middleware.retry",
        "MiddlewarePlanContribution",
        "AiMapReduceChunkContext",
        "AiMapReduceChunkContextKey",
        "UnifiedMiddlewareStateful",
        "CircuitBreakerShouldRetry",
        "CircuitBreakerRetryDelayMs",
        "CircuitBreakerTotalRetryWallMs",
        "ephemeral_events",
        "emit_ephemeral_event",
    ];

    for source_path in sources {
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        for token in forbidden {
            assert!(
                !source.contains(token),
                "retired token {token:?} resurfaced in {}",
                source_path.display()
            );
        }
    }
}

#[test]
fn standalone_retry_discovery_and_topology_production_stay_absent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cases = [
        (
            "crates/obzenflow_adapters/src/middleware/hints.rs",
            vec![
                "RetryHint",
                "Attempts",
                "BackoffKind",
                "pub retry:",
                "retry: Option",
            ],
        ),
        (
            "crates/obzenflow_adapters/src/middleware/validation/safety.rs",
            vec![
                "Infinite retry on source",
                "test_infinite_retry_on_source_is_error",
                "MockInfiniteRetryFactory",
            ],
        ),
        (
            "crates/obzenflow_dsl/src/middleware_resolution.rs",
            vec![
                "validate_middleware_combination",
                "hints().retry",
                "retry behaviour without a circuit breaker",
            ],
        ),
        (
            "crates/obzenflow_dsl/src/dsl/dsl.rs",
            vec![
                "let mut retry_config",
                "retry_config.map",
                "TopologyMiddlewareConfigSlot::Retry",
            ],
        ),
        (
            "crates/obzenflow_runtime/src/pipeline/handle.rs",
            vec!["pub retry: Option<serde_json::Value>"],
        ),
        (
            "crates/obzenflow_core/src/event/chain_event/factory/middleware.rs",
            vec![
                "pub fn retry_exhausted(",
                "pub fn retry_attempt_failed(",
                "pub fn retry_succeeded_after_retry(",
            ],
        ),
        (
            "crates/obzenflow_core/src/event/chain_event/model.rs",
            vec!["lifecycle.middleware.retry"],
        ),
        (
            "crates/obzenflow_core/src/event/payloads/observability_payload.rs",
            vec!["Retry(RetryEvent)", "enum RetryEvent"],
        ),
    ];

    for (relative, forbidden) in cases {
        let source = fs::read_to_string(root.join(relative)).expect("read guarded source");
        for token in forbidden {
            assert!(
                !source.contains(token),
                "retired standalone-retry token {token:?} resurfaced in {relative}"
            );
        }
    }
}

#[test]
fn retry_contracts_with_live_non_middleware_owners_stay_present() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cases = [
        (
            "crates/obzenflow_adapters/src/middleware/control/resilience.rs",
            vec![
                "pub struct EffectResilienceBuilder",
                "pub fn retry(mut self, retry: Retry) -> Self",
                "BackoffStrategy",
            ],
        ),
        (
            "crates/obzenflow_adapters/src/middleware/control/circuit_breaker/retry.rs",
            vec!["pub struct Retry"],
        ),
        (
            "crates/obzenflow_core/src/event/payloads/effect_payload.rs",
            vec!["RetryDisposition"],
        ),
        (
            "crates/obzenflow_adapters/src/sources/http_pull.rs",
            vec!["HttpRetryConfig"],
        ),
        (
            "crates/obzenflow_core/src/event/payloads/observability_payload.rs",
            vec![
                "RetryScheduled",
                "RetrySucceeded",
                "RetryExhausted",
                "RetryStoppedNonRetryable",
                "RecoveryCompleted",
            ],
        ),
    ];

    for (relative, required) in cases {
        let source = fs::read_to_string(root.join(relative)).expect("read guarded source");
        for token in required {
            assert!(
                source.contains(token),
                "live retry contract {token:?} disappeared from {relative}"
            );
        }
    }
}

#[test]
fn payment_tutorial_is_proof_free_and_uses_configured_retry() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let example_root = root.join("examples/payment_gateway_resilience");

    assert!(
        !example_root.join("proof.rs").exists(),
        "FLOWIP release witnesses must not live in the user-facing example"
    );

    for relative in [
        "console.rs",
        "deliveries.rs",
        "domain.rs",
        "main.rs",
        "flow.rs",
        "gateway.rs",
        "fixtures.rs",
        "support.rs",
        "validation.rs",
        "obzenflow.toml",
        "README.md",
    ] {
        let source = fs::read_to_string(example_root.join(relative))
            .unwrap_or_else(|error| panic!("read payment tutorial {relative}: {error}"));
        for forbidden in [
            "PAYMENT_DEMO_RETRY_PROOF",
            "GatewayRetryProof",
            "RetryProofProfile",
            "assemble_flow_with_resilience",
            "build_gateway_resilience",
            "with_retry_proof",
            "retry_proof",
        ] {
            assert!(
                !source.contains(forbidden),
                "payment tutorial {relative} must not contain release-witness machinery; found {forbidden:?}"
            );
        }
    }

    let flow =
        fs::read_to_string(example_root.join("flow.rs")).expect("read payment tutorial flow");
    for forbidden in [
        "Option<Retry>",
        ".retry(None)",
        ".retry(Some(",
        "MiddlewareFactory",
        "FnOnce(",
    ] {
        assert!(
            !flow.contains(forbidden),
            "payment tutorial flow must use direct configured retry with no proof injection seam; found {forbidden:?}"
        );
    }
    assert_eq!(
        flow.matches("EffectResilience::with_breaker(gateway_breaker)")
            .count(),
        1,
        "the tutorial must have one concrete resilience construction path"
    );
    for required in [
        "let gateway_retry = Retry::fixed(Duration::from_millis(250))",
        ".max_attempts(3)",
        ".attempt_start_window(Duration::from_secs(30))",
        "let gateway_resilience = EffectResilience::with_breaker(gateway_breaker)",
        ".retry(gateway_retry)",
        ".rate_limit_each_attempt(gateway_limiter)",
    ] {
        assert!(
            flow.contains(required),
            "payment tutorial must show the configured retry golden path; missing {required:?}"
        );
    }
    assert_eq!(
        flow.matches(".retry(").count(),
        1,
        "the tutorial must configure retry exactly once on its direct resilience path"
    );

    let main = fs::read_to_string(example_root.join("main.rs"))
        .expect("read payment tutorial entry point");
    assert!(main.contains(".run_blocking(flow::build_flow())"));

    let witness =
        fs::read_to_string(root.join("tests/test_support/payment_gateway_retry_fixture.rs"))
            .expect("read test-only payment resilience witness");
    for required in [
        "Retry::fixed(",
        "ReleasePolicy::BreakerRecovery => resilience.retry(canonical_recovery())",
    ] {
        assert!(
            witness.contains(required),
            "configured test witness must retain positive retry authoring {required:?}"
        );
    }
}

#[test]
fn middleware_context_is_confined_to_the_typed_policy_substrate() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![root.join("src/lib.rs")];
    for crate_entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        let source_root = crate_entry.expect("read crate entry").path().join("src");
        if source_root.is_dir() {
            rust_sources_under(&source_root, &mut sources);
        }
    }

    let allowed = [
        "crates/obzenflow_adapters/src/middleware/",
        "crates/obzenflow_core/src/event/schema/middleware_context_key.rs",
        "crates/obzenflow_core/src/event/schema/mod.rs",
        "crates/obzenflow_core/src/lib.rs",
    ];

    for source_path in sources {
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("split always yields one segment");
        if !production_source.contains("MiddlewareContext") {
            continue;
        }
        let relative = source_path
            .strip_prefix(&root)
            .expect("source lives under workspace")
            .to_string_lossy();
        assert!(
            allowed.iter().any(|prefix| relative.starts_with(prefix)),
            "MiddlewareContext escaped the adapter-owned typed policy substrate into {relative}"
        );
    }

    let keys =
        fs::read_to_string(root.join("crates/obzenflow_adapters/src/middleware/context_keys.rs"))
            .expect("read typed context keys");
    for retired in [
        "struct CircuitBreakerAttempt;",
        "struct CircuitBreakerShouldRetry;",
        "struct CircuitBreakerRetryDelayMs;",
        "struct CircuitBreakerTotalRetryWallMs;",
    ] {
        assert!(
            !keys.contains(retired),
            "shell-only typed context key {retired:?} resurfaced"
        );
    }
    for key in [
        "EffectCallDurationNanos",
        "CircuitBreakerIsProbe",
        "CircuitBreakerProbeGeneration",
        "CircuitBreakerProbeSlot",
        "CircuitBreakerRetryAfterMs",
    ] {
        assert!(
            keys.contains(&format!("struct {key}")),
            "typed-policy context key {key} disappeared"
        );
    }
}

#[test]
fn resume_live_handoff_clears_idle_state_in_every_source_supervisor() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "crates/obzenflow_runtime/src/stages/source/finite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/finite/async_supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/async_supervisor.rs",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("read source supervisor");
        let handoff_start = source
            .find("SourceReplayExhaustion::ContinueLive =>")
            .unwrap_or_else(|| panic!("{relative} must handle resume-live exhaustion"));
        let handoff = &source[handoff_start..];
        let reset = handoff
            .find("self.idle_backoff.reset();")
            .unwrap_or_else(|| panic!("{relative} must reset resume-live backoff"));
        let clear_delay = handoff
            .find("self.pending_idle_delay = None;")
            .unwrap_or_else(|| panic!("{relative} must clear its pending resume-live delay"));
        let drop_replay = handoff
            .find("self.replay_driver = None;")
            .unwrap_or_else(|| panic!("{relative} must leave replay before the next live poll"));
        let continue_live = handoff
            .find("Ok(EventLoopDirective::Continue)")
            .unwrap_or_else(|| panic!("{relative} must re-enter the live event loop"));
        assert!(
            reset < continue_live && clear_delay < continue_live && drop_replay < continue_live,
            "{relative} must clear stale idle state before its first resumed live poll"
        );
    }
}

#[test]
fn every_source_supervisor_closes_poll_timing_before_error_normalisation() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "crates/obzenflow_runtime/src/stages/source/finite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/finite/async_supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/async_supervisor.rs",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("read source supervisor");
        let poll_start = source
            .rfind("let poll_started_at = tokio::time::Instant::now();")
            .unwrap_or_else(|| panic!("{relative} must use the paused-clock-testable poll timer"));
        let live_poll = &source[poll_start..];
        let duration_capture = live_poll
            .find("poll_started_at.elapsed()")
            .unwrap_or_else(|| panic!("{relative} must capture raw poll duration"));
        let normalisation = live_poll
            .find("normalise_source_poll_error(")
            .unwrap_or_else(|| panic!("{relative} must retain source-error normalisation"));
        assert!(
            duration_capture < normalisation,
            "{relative} must end poll_duration before normalising the raw result"
        );
    }
}

#[test]
fn eof_and_boundary_rejection_cannot_schedule_idle_delay() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "crates/obzenflow_runtime/src/stages/source/finite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/finite/async_supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/async_supervisor.rs",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("read source supervisor");

        let rejection_start = source
            .find("SourceBoundaryOutcome::Rejected { reason } =>")
            .unwrap_or_else(|| panic!("{relative} must handle boundary rejection"));
        let rejection_end = source[rejection_start..]
            .find("SourceBoundaryOutcome::Polled(poll) =>")
            .map(|offset| rejection_start + offset)
            .unwrap_or_else(|| panic!("{relative} must handle a successful boundary poll"));
        let rejection_arm = &source[rejection_start..rejection_end];
        assert!(
            !rejection_arm.contains("pending_idle_delay = Some"),
            "{relative} must not schedule idle delay after boundary rejection"
        );

        let eof_start = source
            .find("Ok(SourcePollCompletion::Eof) =>")
            .unwrap_or_else(|| panic!("{relative} must handle live EOF"));
        let eof_end = source[eof_start..]
            .find("Err(e) =>")
            .map(|offset| eof_start + offset)
            .unwrap_or_else(|| panic!("{relative} must handle source poll errors after EOF"));
        let eof_arm = &source[eof_start..eof_end];
        assert!(
            !eof_arm.contains("pending_idle_delay = Some"),
            "{relative} must not schedule idle delay after EOF"
        );
    }
}

#[test]
fn effectful_stateful_keeps_only_its_existing_typed_lowerer() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let descriptor =
        fs::read_to_string(root.join("crates/obzenflow_dsl/src/dsl/stage_descriptor.rs"))
            .expect("read stage descriptor");
    let stateful_config =
        fs::read_to_string(root.join("crates/obzenflow_runtime/src/stages/stateful/config.rs"))
            .expect("read stateful config");

    assert!(descriptor.contains("EffectfulStatefulHandlerAdapter(self.handler)"));
    assert!(descriptor.contains("EffectfulStatefulPendingBoundary"));
    assert!(
        !stateful_config.contains("effect_boundary"),
        "FLOWIP-120l must not be pre-implemented as a StatefulConfig carrier"
    );

    let mut stateful_runtime_sources = Vec::new();
    rust_sources_under(
        &root.join("crates/obzenflow_runtime/src/stages/stateful"),
        &mut stateful_runtime_sources,
    );
    for source_path in stateful_runtime_sources {
        let source = fs::read_to_string(&source_path).expect("read stateful runtime source");
        for token in [
            "UnifiedMiddlewareStateful",
            "StatefulHandlerMiddlewareExt",
            "pre_write",
        ] {
            assert!(
                !source.contains(token),
                "retired stateful shell carrier {token:?} resurfaced in {}",
                source_path.display()
            );
        }
    }
}
