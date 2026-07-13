// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

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
        .around_effect(&identity_for("effect.a"), &data_event(), ok_execute())
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
        .around_effect(&identity_for("effect.a"), &data_event(), failing_execute())
        .await;
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(_))
    ));

    // Effect A is now rejected at admission.
    let report = boundary
        .around_effect(&identity_for("effect.a"), &data_event(), ok_execute())
        .await;
    assert!(
        matches!(report.outcome, EffectBoundaryOutcome::Aborted(_)),
        "effect A's breaker must reject after its failure"
    );

    // Effect B's breaker never saw A's failure and still admits.
    let report = boundary
        .around_effect(&identity_for("effect.b"), &data_event(), ok_execute())
        .await;
    assert!(
        matches!(report.outcome, EffectBoundaryOutcome::Executed(Ok(_))),
        "effect B must stay admitted; breakers are per effect"
    );

    // An effect with no declared policies executes unguarded.
    let report = boundary
        .around_effect(&identity_for("effect.c"), &data_event(), ok_execute())
        .await;
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
}
