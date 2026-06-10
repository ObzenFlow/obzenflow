// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::types::SeqNo;
use obzenflow_core::{ContractResult, StageId, ViolationCause};

use obzenflow_core::event::types::ViolationCause as EventViolationCause;

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

pub trait ContractPolicy: Send + Sync {
    fn apply(
        &self,
        results: &[ContractResult],
        edge: &EdgeContext,
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

    pub fn decide(&self, results: &[ContractResult], edge: &EdgeContext) -> EdgeContractDecision {
        // Apply policies in order; each sees the prior decision and may respect
        // or override it. We start with a conservative default of Pass.
        let mut decision = EdgeContractDecision::Pass;
        for policy in &self.policies {
            decision = policy.apply(results, edge, decision);
        }
        decision
    }
}

/// TransportStrictPolicy reproduces today's behaviour: any SeqDivergence is a failure.
///
/// FLOWIP-120h removed the `BreakerAware` override that used to sit behind this
/// policy. A breaker fallback rides the effect cursor as a recorded outcome
/// fact and a breaker rejection records a structured failure, so contract
/// reconciliation needs no breaker-keyed compensation: every stage runs strict
/// contracts.
pub struct TransportStrictPolicy;

impl ContractPolicy for TransportStrictPolicy {
    fn apply(
        &self,
        results: &[ContractResult],
        _edge: &EdgeContext,
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
                    ViolationCause::Divergence {
                        predicate,
                        observed,
                        threshold,
                        window_seconds,
                    } => EventViolationCause::Divergence {
                        predicate: predicate.clone(),
                        observed: *observed,
                        threshold: *threshold,
                        window_seconds: *window_seconds,
                    },
                    ViolationCause::Other(msg) => EventViolationCause::Other(msg.clone()),
                };
                return EdgeContractDecision::Fail(event_cause);
            }
        }

        // No failures observed by underlying contracts.
        EdgeContractDecision::Pass
    }
}

/// Helper to build the default policy stack for an upstream stage.
pub fn build_policy_stack_for_upstream(_upstream_stage: StageId) -> ContractPolicyStack {
    ContractPolicyStack::new(vec![Box::new(TransportStrictPolicy)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::system_event::ContractName;
    use obzenflow_core::event::types::SeqNo;
    use obzenflow_core::{
        ContractEvidence, ContractResult, ContractViolation, StageId, ViolationCause,
    };
    use serde_json::json;

    fn make_violation(cause: ViolationCause) -> ContractResult {
        ContractResult::Failed(ContractViolation {
            contract_name: ContractName::from("transport"),
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

        let results = vec![ContractResult::Passed(ContractEvidence {
            contract_name: ContractName::from("transport"),
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            verified_at: chrono::Utc::now(),
            details: json!({}),
        })];

        let decision = policy.apply(&results, &edge, EdgeContractDecision::Pass);
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

        let results = vec![make_violation(ViolationCause::SeqDivergence {
            advertised: Some(SeqNo(3)),
            reader: SeqNo(1),
        })];

        let decision = policy.apply(&results, &edge, EdgeContractDecision::Pass);
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
}
