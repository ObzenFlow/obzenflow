use obzenflow_core::control_middleware::{CircuitBreakerContractMode, ControlMiddlewareProvider};
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::{ContractResult, StageId, ViolationCause};

use obzenflow_core::event::types::ViolationCause as EventViolationCause;
use std::sync::Arc;

/// Final decision for a single edge after applying policies to raw contract results.
#[derive(Clone)]
pub enum EdgeContractDecision {
    Pass,
    Fail(EventViolationCause),
}

/// Minimal edge context needed by policies.
pub struct EdgeContext {
    pub upstream_stage: StageId,
    pub downstream_stage: StageId,
    pub advertised_writer_seq: Option<SeqNo>,
    pub reader_seq: SeqNo,
}

/// Hints supplied to policies, typically derived from middleware registries.
pub struct PolicyHints {
    pub breaker_mode: Option<CircuitBreakerContractMode>,
    pub has_opened_since_registration: bool,
    pub has_fallback_configured: bool,
}

pub trait ContractPolicy: Send + Sync {
    fn apply(
        &self,
        results: &[ContractResult],
        edge: &EdgeContext,
        hints: &PolicyHints,
        prior: EdgeContractDecision,
    ) -> EdgeContractDecision;
}

pub struct ContractPolicyStack {
    policies: Vec<Box<dyn ContractPolicy>>,
}

impl ContractPolicyStack {
    pub fn new(policies: Vec<Box<dyn ContractPolicy>>) -> Self {
        Self { policies }
    }

    pub fn decide(
        &self,
        results: &[ContractResult],
        edge: &EdgeContext,
        hints: &PolicyHints,
    ) -> EdgeContractDecision {
        // Apply policies in order; each sees the prior decision and may respect
        // or override it. We start with a conservative default of Pass.
        let mut decision = EdgeContractDecision::Pass;
        for policy in &self.policies {
            decision = policy.apply(results, edge, hints, decision);
        }
        decision
    }
}

/// TransportStrictPolicy reproduces today's behaviour: any SeqDivergence is a failure.
pub struct TransportStrictPolicy;

impl ContractPolicy for TransportStrictPolicy {
    fn apply(
        &self,
        results: &[ContractResult],
        edge: &EdgeContext,
        _hints: &PolicyHints,
        _prior: EdgeContractDecision,
    ) -> EdgeContractDecision {
        for result in results {
            if let ContractResult::Failed(violation) = result {
                let event_cause = match &violation.cause {
                    ViolationCause::SeqDivergence { advertised, reader } => {
                        EventViolationCause::SeqDivergence {
                            advertised: *advertised,
                            reader: *reader,
                        }
                    }
                    ViolationCause::ContentMismatch { .. } => {
                        EventViolationCause::Other("content_mismatch".into())
                    }
                    ViolationCause::DeliveryMismatch { .. } => {
                        EventViolationCause::Other("delivery_mismatch".into())
                    }
                    ViolationCause::AccountingMismatch { .. } => {
                        EventViolationCause::Other("accounting_mismatch".into())
                    }
                    ViolationCause::Other(msg) => EventViolationCause::Other(msg.clone()),
                };
                return EdgeContractDecision::Fail(event_cause);
            }
        }

        // No failures observed by underlying contracts.
        EdgeContractDecision::Pass
    }
}

/// BreakerAwarePolicy can override pure SeqDivergence failures for CB-protected edges.
pub struct BreakerAwarePolicy;

impl ContractPolicy for BreakerAwarePolicy {
    fn apply(
        &self,
        results: &[ContractResult],
        edge: &EdgeContext,
        hints: &PolicyHints,
        prior: EdgeContractDecision,
    ) -> EdgeContractDecision {
        // Only consider overriding when:
        // - BreakerAware mode is explicitly configured,
        // - The breaker has actually opened at least once, and
        // - A fallback is configured for this stage.
        if hints.breaker_mode != Some(CircuitBreakerContractMode::BreakerAware)
            || !hints.has_opened_since_registration
            || !hints.has_fallback_configured
        {
            return prior;
        }

        // Minimal 051b-part-2 behaviour: if the strict decision is a SeqDivergence
        // failure, treat it as pass for CB-protected edges. More precise correlation
        // with CB-open windows can be added later.
        match prior {
            EdgeContractDecision::Fail(EventViolationCause::SeqDivergence { .. }) => {
                EdgeContractDecision::Pass
            }
            _ => prior,
        }
    }
}

/// Helper to build a default policy stack for an upstream stage.
pub fn build_policy_stack_for_upstream(
    upstream_stage: StageId,
    control_middleware: &Arc<dyn ControlMiddlewareProvider>,
) -> ContractPolicyStack {
    let mut policies: Vec<Box<dyn ContractPolicy>> = Vec::new();

    // Always include strict transport semantics.
    policies.push(Box::new(TransportStrictPolicy));

    // If the upstream is configured as BreakerAware, add the breaker-aware override.
    if let Some(info) = control_middleware.circuit_breaker_contract_info(&upstream_stage) {
        if info.mode == CircuitBreakerContractMode::BreakerAware {
            policies.push(Box::new(BreakerAwarePolicy));
        }
    }

    ContractPolicyStack::new(policies)
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::types::SeqNo;
    use obzenflow_core::{
        ContractEvidence, ContractResult, ContractViolation, StageId, ViolationCause,
    };
    use serde_json::json;

    fn make_violation(cause: ViolationCause) -> ContractResult {
        ContractResult::Failed(ContractViolation {
            contract_name: "transport".to_string(),
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            detected_at: chrono::Utc::now(),
            cause,
            details: json!({}),
        })
    }

    #[test]
    fn transport_strict_policy_passes_on_no_failures() {
        let policy = TransportStrictPolicy;
        let edge = EdgeContext {
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            advertised_writer_seq: None,
            reader_seq: SeqNo(0),
        };
        let hints = PolicyHints {
            breaker_mode: None,
            has_opened_since_registration: false,
            has_fallback_configured: false,
        };

        let results = vec![ContractResult::Passed(ContractEvidence {
            contract_name: "transport".to_string(),
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            verified_at: chrono::Utc::now(),
            details: json!({}),
        })];

        let decision = policy.apply(&results, &edge, &hints, EdgeContractDecision::Pass);
        assert!(matches!(decision, EdgeContractDecision::Pass));
    }

    #[test]
    fn transport_strict_policy_fails_on_seq_divergence() {
        let policy = TransportStrictPolicy;
        let edge = EdgeContext {
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            advertised_writer_seq: Some(SeqNo(3)),
            reader_seq: SeqNo(1),
        };
        let hints = PolicyHints {
            breaker_mode: None,
            has_opened_since_registration: false,
            has_fallback_configured: false,
        };

        let results = vec![make_violation(ViolationCause::SeqDivergence {
            advertised: Some(SeqNo(3)),
            reader: SeqNo(1),
        })];

        let decision = policy.apply(&results, &edge, &hints, EdgeContractDecision::Pass);
        match decision {
            EdgeContractDecision::Fail(EventViolationCause::SeqDivergence {
                advertised,
                reader,
            }) => {
                assert_eq!(advertised, Some(SeqNo(3)));
                assert_eq!(reader, SeqNo(1));
            }
            _ => panic!("expected SeqDivergence failure, got different decision"),
        }
    }

    #[test]
    fn breaker_aware_policy_respects_prior_when_not_configured() {
        let policy = BreakerAwarePolicy;
        let edge = EdgeContext {
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            advertised_writer_seq: Some(SeqNo(3)),
            reader_seq: SeqNo(1),
        };
        let hints = PolicyHints {
            breaker_mode: Some(CircuitBreakerContractMode::Strict),
            has_opened_since_registration: false,
            has_fallback_configured: false,
        };

        let prior = EdgeContractDecision::Fail(EventViolationCause::SeqDivergence {
            advertised: Some(SeqNo(3)),
            reader: SeqNo(1),
        });

        let decision = policy.apply(&[], &edge, &hints, prior);
        assert!(matches!(
            decision,
            EdgeContractDecision::Fail(EventViolationCause::SeqDivergence { .. })
        ));
    }

    #[test]
    fn breaker_aware_policy_requires_open_and_fallback() {
        let policy = BreakerAwarePolicy;
        let edge = EdgeContext {
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            advertised_writer_seq: Some(SeqNo(3)),
            reader_seq: SeqNo(1),
        };

        let prior = EdgeContractDecision::Fail(EventViolationCause::SeqDivergence {
            advertised: Some(SeqNo(3)),
            reader: SeqNo(1),
        });

        // Case 1: BreakerAware but never opened
        let hints_never_opened = PolicyHints {
            breaker_mode: Some(CircuitBreakerContractMode::BreakerAware),
            has_opened_since_registration: false,
            has_fallback_configured: true,
        };
        let decision = policy.apply(&[], &edge, &hints_never_opened, prior.clone());
        assert!(matches!(
            decision,
            EdgeContractDecision::Fail(EventViolationCause::SeqDivergence { .. })
        ));

        // Case 2: BreakerAware and opened, but no fallback configured
        let hints_no_fallback = PolicyHints {
            breaker_mode: Some(CircuitBreakerContractMode::BreakerAware),
            has_opened_since_registration: true,
            has_fallback_configured: false,
        };
        let decision = policy.apply(&[], &edge, &hints_no_fallback, prior.clone());
        assert!(matches!(
            decision,
            EdgeContractDecision::Fail(EventViolationCause::SeqDivergence { .. })
        ));
    }

    #[test]
    fn breaker_aware_policy_overrides_seq_divergence_when_safe() {
        let policy = BreakerAwarePolicy;
        let edge = EdgeContext {
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            advertised_writer_seq: Some(SeqNo(3)),
            reader_seq: SeqNo(1),
        };
        let hints = PolicyHints {
            breaker_mode: Some(CircuitBreakerContractMode::BreakerAware),
            has_opened_since_registration: true,
            has_fallback_configured: true,
        };

        let prior = EdgeContractDecision::Fail(EventViolationCause::SeqDivergence {
            advertised: Some(SeqNo(3)),
            reader: SeqNo(1),
        });

        let decision = policy.apply(&[], &edge, &hints, prior);
        assert!(matches!(decision, EdgeContractDecision::Pass));
    }

    #[test]
    fn breaker_aware_policy_does_not_override_other_failures() {
        let policy = BreakerAwarePolicy;
        let edge = EdgeContext {
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            advertised_writer_seq: Some(SeqNo(3)),
            reader_seq: SeqNo(1),
        };
        let hints = PolicyHints {
            breaker_mode: Some(CircuitBreakerContractMode::BreakerAware),
            has_opened_since_registration: true,
            has_fallback_configured: true,
        };

        let prior =
            EdgeContractDecision::Fail(EventViolationCause::Other("content_mismatch".into()));

        let decision = policy.apply(&[], &edge, &hints, prior);
        match decision {
            EdgeContractDecision::Fail(EventViolationCause::Other(reason)) => {
                assert_eq!(reason, "content_mismatch");
            }
            _ => panic!("expected Other failure to be preserved by BreakerAwarePolicy"),
        }
    }
}
