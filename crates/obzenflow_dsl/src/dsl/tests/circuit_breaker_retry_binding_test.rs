// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::dsl::binder::{
    bind_effect_policy, materialize_sink_delivery, materialize_source_poll,
    register_retry_enabled_breaker, reject_handler_shell_retry, SourcePollRetryEvidence,
};
use obzenflow_adapters::middleware::control::circuit_breaker::CircuitBreakerBuilder;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    MiddlewareDeclarationIndex, MiddlewareFactory, MiddlewareOrigin,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::StageId;
use obzenflow_runtime::effects::{
    EffectDeclaration, EffectSafety, IdempotencyKeyPolicy, SinkDeliverySafety,
};
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::handlers::{
    SourcePollRetryOwnership, SourcePollRetrySafety,
};
use std::sync::Arc;
use std::time::Duration;

fn stage_config(name: &str) -> StageConfig {
    StageConfig {
        stage_id: StageId::new(),
        name: name.to_string(),
        flow_name: "retry_binding_test".to_string(),
        cycle_guard: None,
        lineage: obzenflow_core::config::LineagePolicy::default(),
        resolved_policies: Default::default(),
    }
}

fn retry_breaker() -> Box<dyn MiddlewareFactory> {
    CircuitBreakerBuilder::new(3)
        .with_retry_fixed(Duration::from_millis(10), 3)
        .build()
}

fn health_only_breaker() -> Box<dyn MiddlewareFactory> {
    CircuitBreakerBuilder::new(3).build()
}

fn error_of<T>(result: Result<T, String>) -> String {
    match result {
        Ok(_) => panic!("expected materialisation to fail"),
        Err(error) => error,
    }
}

fn effect_declaration(effect_type: &'static str, safety: EffectSafety) -> EffectDeclaration {
    EffectDeclaration {
        effect_type,
        safety,
        idempotency_key_policy: match safety {
            EffectSafety::NonIdempotentRequiresKey => IdempotencyKeyPolicy::Required,
            EffectSafety::Idempotent | EffectSafety::Transactional => {
                IdempotencyKeyPolicy::NotRequired
            }
        },
        required_ports: Vec::new(),
        transactional_executor: (safety == EffectSafety::Transactional).then_some("tx"),
        outcome_fact_types: Vec::new(),
    }
}

#[test]
fn synchronous_source_retry_is_rejected_before_materialisation() {
    let config = stage_config("orders");
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let breaker = retry_breaker();

    let error = error_of(materialize_source_poll(
        breaker.as_ref(),
        &config,
        StageType::FiniteSource,
        SourcePollRetryEvidence::synchronous(),
        &control,
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    ));

    assert_eq!(
        error,
        "circuit_breaker retry rejected for synchronous source 'orders': max_total_wall_time and force-abort cannot pre-empt FiniteSourceHandler::next(); use an async source with a poll timeout, or keep retry inside the handler"
    );
}

#[test]
fn async_source_retry_fails_closed_on_missing_or_negative_safety() {
    let config = stage_config("stories");

    let undeclared = error_of(materialize_source_poll(
        retry_breaker().as_ref(),
        &config,
        StageType::FiniteSource,
        SourcePollRetryEvidence::asynchronous(None, SourcePollRetryOwnership::NoNestedRetry),
        &Arc::new(ControlMiddlewareAggregator::new()),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    ));
    assert_eq!(
        undeclared,
        "circuit_breaker retry rejected for source-poll 'stories': retry safety is undeclared; implement AsyncFiniteSourceHandler::poll_retry_safety() returning Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation), or remove breaker retry"
    );

    let non_retry_safe = error_of(materialize_source_poll(
        retry_breaker().as_ref(),
        &config,
        StageType::FiniteSource,
        SourcePollRetryEvidence::asynchronous(
            Some(SourcePollRetrySafety::NonRetrySafe),
            SourcePollRetryOwnership::NoNestedRetry,
        ),
        &Arc::new(ControlMiddlewareAggregator::new()),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    ));
    assert_eq!(
        non_retry_safe,
        "circuit_breaker retry rejected for source-poll 'stories': retry safety is SourcePollRetrySafety::NonRetrySafe; remove breaker retry or provide a different source whose poll is repeatable after error and cancellation"
    );
}

#[test]
fn async_source_handler_owned_retry_is_rejected_before_safety() {
    let config = stage_config("hn_stories");
    let error = error_of(materialize_source_poll(
        retry_breaker().as_ref(),
        &config,
        StageType::FiniteSource,
        SourcePollRetryEvidence::asynchronous(
            None,
            SourcePollRetryOwnership::HandlerOwned {
                owner: "HttpRetryConfig",
            },
        ),
        &Arc::new(ControlMiddlewareAggregator::new()),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    ));

    assert_eq!(
        error,
        "circuit_breaker retry rejected for source-poll 'hn_stories': observed SourcePollRetryOwnership::HandlerOwned { owner: \"HttpRetryConfig\" }; use HttpRetryConfig::disabled() so the circuit-breaker boundary is the single retry owner"
    );
}

#[test]
fn http_decoder_missing_request_proof_uses_locked_diagnostic() {
    let config = stage_config("hn_stories");
    let error = error_of(materialize_source_poll(
        retry_breaker().as_ref(),
        &config,
        StageType::FiniteSource,
        SourcePollRetryEvidence::asynchronous_request_level(
            None,
            SourcePollRetryOwnership::NoNestedRetry,
        ),
        &Arc::new(ControlMiddlewareAggregator::new()),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    ));

    assert_eq!(
        error,
        "circuit_breaker retry rejected for source-poll 'hn_stories': observed retry safety 'undeclared' because PullDecoder does not declare every generated request repeatable; call retry_safe_requests() after verifying the request contract, implement PullDecoder::request_retry_safety(), or remove retry"
    );
}

#[test]
fn retry_safe_async_source_and_health_only_sync_source_remain_supported() {
    let safe_config = stage_config("safe_feed");
    let safe_binding = materialize_source_poll(
        retry_breaker().as_ref(),
        &safe_config,
        StageType::InfiniteSource,
        SourcePollRetryEvidence::asynchronous(
            Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation),
            SourcePollRetryOwnership::NoNestedRetry,
        ),
        &Arc::new(ControlMiddlewareAggregator::new()),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    )
    .expect("declared-safe async source should bind");
    let owner = safe_binding
        .policy
        .retry_owner()
        .expect("retry-enabled breaker exposes one owner");
    assert_eq!(owner.protected_unit_label, "safe_feed");

    materialize_source_poll(
        health_only_breaker().as_ref(),
        &stage_config("blocking_feed"),
        StageType::FiniteSource,
        SourcePollRetryEvidence::synchronous(),
        &Arc::new(ControlMiddlewareAggregator::new()),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    )
    .expect("health-only breaker should preserve sync-source compatibility");
}

#[test]
fn transactional_effect_retry_is_rejected_with_the_complete_declaration() {
    let config = stage_config("payments");
    let declaration = effect_declaration("AuthorizePayment", EffectSafety::Transactional);
    let error = error_of(bind_effect_policy(
        retry_breaker().as_ref(),
        &config,
        StageType::Transform,
        &Arc::new(ControlMiddlewareAggregator::new()),
        &declaration,
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::effect_policy(0),
    ));

    assert_eq!(
        error,
        "circuit_breaker retry rejected for effect 'AuthorizePayment': EffectSafety::Transactional commits inside the protected attempt and has no rollback-proof retry contract; remove retry or use an idempotent/keyed effect"
    );

    bind_effect_policy(
        retry_breaker().as_ref(),
        &config,
        StageType::Transform,
        &Arc::new(ControlMiddlewareAggregator::new()),
        &effect_declaration("AuthorizePayment", EffectSafety::Idempotent),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::effect_policy(0),
    )
    .expect("idempotent effect should bind retry");
}

#[test]
fn retry_enabled_breaker_is_rejected_from_the_generic_handler_shell_lane() {
    assert_eq!(
        reject_handler_shell_retry(retry_breaker().as_ref()).expect_err(
            "retry must attach through `Effect with [policies]`, not generic middleware"
        ),
        "circuit_breaker retry requires a source-poll, declared-effect, or sink-delivery attachment; handler-shell retry is not supported"
    );
    reject_handler_shell_retry(health_only_breaker().as_ref())
        .expect("health-only transitional policy remains supported");
}

#[test]
fn sink_retry_requires_declared_idempotent_delivery() {
    let config = stage_config("payments");

    let undeclared = error_of(materialize_sink_delivery(
        retry_breaker().as_ref(),
        &config,
        StageType::Sink,
        None,
        &Arc::new(ControlMiddlewareAggregator::new()),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    ));
    assert_eq!(
        undeclared,
        "circuit_breaker retry rejected for sink-delivery 'payments': delivery safety is undeclared; add delivery: idempotent, implement SinkHandler::delivery_safety(), or remove breaker retry"
    );

    let unsafe_delivery = error_of(materialize_sink_delivery(
        retry_breaker().as_ref(),
        &config,
        StageType::Sink,
        Some(SinkDeliverySafety::NonIdempotentExternal),
        &Arc::new(ControlMiddlewareAggregator::new()),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    ));
    assert_eq!(
        unsafe_delivery,
        "circuit_breaker retry rejected for sink-delivery 'payments': delivery safety is SinkDeliverySafety::NonIdempotentExternal; declare idempotent delivery only if repeated delivery is absorbed, or remove breaker retry"
    );

    let safe_policy = materialize_sink_delivery(
        retry_breaker().as_ref(),
        &config,
        StageType::Sink,
        Some(SinkDeliverySafety::IdempotentProjection),
        &Arc::new(ControlMiddlewareAggregator::new()),
        &MiddlewareOrigin::Stage,
        MiddlewareDeclarationIndex::resolved(0),
    )
    .expect("idempotent sink should bind retry");
    assert_eq!(
        safe_policy
            .retry_owner()
            .expect("retry-enabled breaker exposes one owner")
            .protected_unit_label,
        "payments"
    );
}

#[test]
fn a_protected_unit_accepts_only_one_retry_enabled_breaker() {
    let mut seen = false;
    register_retry_enabled_breaker(
        health_only_breaker().as_ref(),
        &mut seen,
        "effect",
        "AuthorizePayment",
    )
    .expect("health-only breaker is not a retry owner");
    assert!(!seen);

    register_retry_enabled_breaker(
        retry_breaker().as_ref(),
        &mut seen,
        "effect",
        "AuthorizePayment",
    )
    .expect("first retry owner is accepted");
    let error = register_retry_enabled_breaker(
        retry_breaker().as_ref(),
        &mut seen,
        "effect",
        "AuthorizePayment",
    )
    .expect_err("second retry owner must fail");
    assert_eq!(
        error,
        "circuit_breaker retry rejected for effect 'AuthorizePayment': multiple retry-enabled circuit breakers resolve to this protected unit; keep exactly one recovery owner"
    );
}
