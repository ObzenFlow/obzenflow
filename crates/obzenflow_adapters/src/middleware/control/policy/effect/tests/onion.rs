// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Onion order, structural finalization, and per-effect isolation.

use super::support::*;

struct RecordingPolicy {
    label: &'static str,
    observed: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EffectPolicy for RecordingPolicy {
    fn label(&self) -> &'static str {
        self.label
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        PolicyAdmission::Admit
    }

    fn observe(&self, attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {
        let kind = match attempt {
            EffectAttemptOutcome::Executed(Ok(_)) => "executed_ok".to_string(),
            EffectAttemptOutcome::Executed(Err(_)) => "executed_err".to_string(),
            EffectAttemptOutcome::SkippedBy(label) => format!("skipped_by:{label}"),
            EffectAttemptOutcome::RejectedBy(cause) => {
                format!("rejected_by:{}", cause.source)
            }
        };
        self.observed.lock().unwrap().push(kind);
    }
}

struct RejectingPolicy;

#[async_trait]
impl EffectPolicy for RejectingPolicy {
    fn label(&self) -> &'static str {
        "test.rejecting"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        PolicyAdmission::Reject(MiddlewareAbortCause {
            source: EffectFailureSource::new("test.rejecting"),
            code: EffectFailureCode::new("rejected"),
            message: "test rejection".to_string(),
            retry: RetryDisposition::NotRetryable,
            event: None,
        })
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
}

/// FLOWIP-120c gap G8: finalization is structural. A policy that
/// admitted observes the attempt even when a later policy rejects it.
#[tokio::test]
async fn admitted_policies_observe_rejection_by_later_policy() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let chain: Arc<Vec<EffectPolicyAttachment>> = Arc::new(vec![
        EffectPolicyAttachment::neutral(Arc::new(RecordingPolicy {
            label: "test.recording",
            observed: observed.clone(),
        })),
        EffectPolicyAttachment::neutral(Arc::new(RejectingPolicy)),
    ]);
    let mut chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>> = HashMap::new();
    chains.insert("effect.a", chain);
    let boundary = PerEffectPolicyBoundary::new(chains);

    let report = boundary
        .around_repeatable_effect(&identity_for("effect.a"), &data_event(), ok_execute())
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Aborted(ref reason)
            if reason.cause.source == "test.rejecting"
    ));
    assert_eq!(
        observed.lock().unwrap().as_slice(),
        ["rejected_by:test.rejecting"],
        "the admitted policy observes how the attempt ended"
    );
}

/// FLOWIP-120c gap G3: one policy instance per protected dependency.
/// Failures of effect A open A's breaker and leave B's untouched.
#[tokio::test]
async fn per_effect_breakers_do_not_cross_trip() {
    let breaker_a = Arc::new(CircuitBreakerMiddleware::new(1));
    let breaker_b = Arc::new(CircuitBreakerMiddleware::new(1));
    let mut chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>> = HashMap::new();
    chains.insert(
        "effect.a",
        Arc::new(vec![EffectPolicyAttachment::event_aware(breaker_a)]),
    );
    chains.insert(
        "effect.b",
        Arc::new(vec![EffectPolicyAttachment::event_aware(breaker_b)]),
    );
    let boundary = PerEffectPolicyBoundary::new(chains);

    // One failure opens effect A's breaker (threshold 1).
    let report = boundary
        .around_repeatable_effect(&identity_for("effect.a"), &data_event(), failing_execute())
        .await;
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(_))
    ));

    // Effect A is now rejected at admission.
    let report = boundary
        .around_repeatable_effect(&identity_for("effect.a"), &data_event(), ok_execute())
        .await;
    assert!(
        matches!(report.outcome, EffectBoundaryOutcome::Aborted(_)),
        "effect A's breaker must reject after its failure"
    );

    // Effect B's breaker never saw A's failure and still admits.
    let report = boundary
        .around_repeatable_effect(&identity_for("effect.b"), &data_event(), ok_execute())
        .await;
    assert!(
        matches!(report.outcome, EffectBoundaryOutcome::Executed(Ok(_))),
        "effect B must stay admitted; breakers are per effect"
    );

    // An effect with no declared policies executes unguarded.
    let report = boundary
        .around_repeatable_effect(&identity_for("effect.c"), &data_event(), ok_execute())
        .await;
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
}

#[tokio::test]
async fn policy_position_controls_logical_vs_physical_admission_count() {
    let inner_calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(3))
            .build(),
    );
    let (inner_limiter, inner_control, inner_stage) = rate_limiter_fixture();
    let report = boundary_with_chain(vec![breaker, inner_limiter])
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(inner_calls.clone(), |call| {
                if call < 3 {
                    Err(EffectError::Timeout("again".to_string()))
                } else {
                    Ok(Vec::new())
                }
            }),
        )
        .await;
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(effect_limiter_events(&inner_control, inner_stage), 3);

    let outer_calls = Arc::new(AtomicUsize::new(0));
    let (outer_limiter, outer_control, outer_stage) = rate_limiter_fixture();
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(3))
            .build(),
    );
    let report = boundary_with_chain(vec![outer_limiter, breaker])
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(outer_calls.clone(), |call| {
                if call < 3 {
                    Err(EffectError::Timeout("again".to_string()))
                } else {
                    Ok(Vec::new())
                }
            }),
        )
        .await;
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(effect_limiter_events(&outer_control, outer_stage), 1);
}

#[tokio::test]
async fn health_only_breaker_calls_a_failing_effect_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(CircuitBreaker::opens_after(3).build());
    let report = boundary_with_chain(vec![breaker])
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |_| {
                Err(EffectError::Timeout("no recovery configured".to_string()))
            }),
        )
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(retry_events(&report).is_empty());
}

#[tokio::test]
async fn open_circuit_makes_no_call_and_half_open_probe_never_retries() {
    let (open_breaker, _, _) = retrying_breaker_fixture(
        CircuitBreaker::opens_after(1)
            .cooldown(Duration::from_secs(60))
            .retry(Retry::fixed(Duration::ZERO).attempts(3))
            .build(),
    );
    let open_boundary = boundary_with_chain(vec![open_breaker]);
    let first_calls = Arc::new(AtomicUsize::new(0));
    open_boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(first_calls, |_| {
                Err(EffectError::Permanent("open".to_string()))
            }),
        )
        .await;
    let rejected_calls = Arc::new(AtomicUsize::new(0));
    let report = open_boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(rejected_calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    assert!(matches!(report.outcome, EffectBoundaryOutcome::Aborted(_)));
    assert_eq!(rejected_calls.load(Ordering::SeqCst), 0);

    let (half_open_breaker, _, _) = retrying_breaker_fixture(
        CircuitBreaker::opens_after(1)
            .cooldown(Duration::ZERO)
            .retry(Retry::fixed(Duration::ZERO).attempts(3))
            .build(),
    );
    let half_open_boundary = boundary_with_chain(vec![half_open_breaker]);
    half_open_boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(Arc::new(AtomicUsize::new(0)), |_| {
                Err(EffectError::Permanent("open".to_string()))
            }),
        )
        .await;
    let probe_calls = Arc::new(AtomicUsize::new(0));
    let report = half_open_boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(probe_calls.clone(), |_| {
                Err(EffectError::Timeout("probe failed".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));
    assert_eq!(probe_calls.load(Ordering::SeqCst), 1);
    assert!(retry_events(&report).is_empty());
}
