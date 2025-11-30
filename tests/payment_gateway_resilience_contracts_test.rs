//! Regression tests for the payment_gateway_resilience example.
//!
//! These tests exercise the strict vs breaker-aware configurations:
//! - Strict flow (no circuit breaker) is expected to fail with a
//!   transport SeqDivergence contract violation.
//! - Breaker-aware flow (CB + typed fallback + policies) is expected
//!   to complete successfully under the same scripted outage pattern.

#[path = "../examples/payment_gateway_resilience/lib.rs"]
mod payment_gateway_resilience;

#[test]
#[ignore = "full payment_gateway_resilience flows are relatively slow; run manually when validating CB + contract alignment end-to-end"]
fn payment_gateway_strict_flow_fails() {
    let result = payment_gateway_resilience::flow::run_strict_example_in_tests();
    assert!(
        result.is_err(),
        "strict payment gateway flow should fail with contract violation"
    );
}

#[test]
#[ignore = "full payment_gateway_resilience flows are relatively slow; run manually when validating CB + contract alignment end-to-end"]
fn payment_gateway_breaker_aware_flow_succeeds() {
    let result = payment_gateway_resilience::flow::run_example_in_tests();
    assert!(
        result.is_ok(),
        "breaker-aware payment gateway flow should succeed with degraded responses"
    );
}
