// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::types::SeqNo;
use obzenflow_core::{ContractResult, StageId, ViolationCause};

use obzenflow_core::event::types::ViolationCause as EventViolationCause;

/// Strictness mode for source at-least-once contracts.
///
/// This is a minimal, flow-wide toggle for how contract failures on
/// *source* edges influence pipeline behaviour:
/// - `Abort` (default): any failed source contract aborts the pipeline.
/// - `Warn`: failures are logged and surfaced via contract events, but
///   do not cause a pipeline abort. This is intended as a transitional
///   mode until full contract strictness plumbing lands in 090d.
///
/// FLOWIP-010: build-resolved from `contracts.source_contract_strict_mode`
/// and applied by the consuming child before aggregating edge outcomes; the registry rejects unknown tokens at
/// startup (the old env coercion is gone).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SourceContractStrictMode {
    #[default]
    Abort,
    Warn,
}

impl SourceContractStrictMode {
    /// Parse the registry-validated token (`abort` or `warn`).
    pub fn from_token(token: &str) -> Self {
        match token {
            "warn" => SourceContractStrictMode::Warn,
            _ => SourceContractStrictMode::Abort,
        }
    }
}

/// Final decision for a single edge after applying policies to raw contract results.
#[derive(Clone)]
pub enum EdgeContractDecision {
    Pass,
    Fail(EventViolationCause),
}

/// Minimal edge context needed by policies.
pub struct EdgeContext {
    /// Selected-feed policies already allow unavailable per-type advertisements.
    pub selected_population: bool,
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
        edge: &EdgeContext,
        _prior: EdgeContractDecision,
    ) -> EdgeContractDecision {
        for result in results {
            if let ContractResult::Pending {
                reason: obzenflow_core::contracts::PendingReason::WriterAdvertisementUnavailable,
                ..
            } = result
            {
                if !edge.selected_population && edge.reader_seq.0 > 0 {
                    return EdgeContractDecision::Fail(EventViolationCause::Other(
                        "writer_advertisement_unavailable".into(),
                    ));
                }
            }
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
    use obzenflow_core::event::payloads::system_payload::ContractName;
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
            details: json!({}).into(),
        })
    }

    #[test]
    fn pending_transport_preserves_existing_empty_and_selected_acceptance() {
        use obzenflow_core::contracts::{ContractEvidenceDetails, PendingReason};
        for selected_population in [false, true] {
            for consumed in [0, 10] {
                let upstream = StageId::new();
                let reader = StageId::new();
                let result = ContractResult::Pending {
                    reason: PendingReason::WriterAdvertisementUnavailable,
                    evidence: ContractEvidence {
                        contract_name: ContractName::from("TransportContract"),
                        upstream_stage: upstream,
                        downstream_stage: reader,
                        evaluated_at: chrono::Utc::now(),
                        details: ContractEvidenceDetails::Transport {
                            advertised_writer_seq: None,
                            consumed_count: obzenflow_core::event::types::Count(consumed),
                        },
                    },
                };
                let edge = EdgeContext {
                    selected_population,
                    upstream_stage: upstream,
                    downstream_stage: reader,
                    advertised_writer_seq: None,
                    reader_seq: SeqNo(consumed),
                };
                let decision =
                    TransportStrictPolicy.apply(&[result], &edge, EdgeContractDecision::Pass);
                assert_eq!(
                    matches!(decision, EdgeContractDecision::Pass),
                    selected_population || consumed == 0
                );
            }
        }
    }

    #[test]
    fn transport_strict_policy_passes_on_no_failures() {
        let policy = TransportStrictPolicy;
        let edge = EdgeContext {
            selected_population: false,
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            advertised_writer_seq: None,
            reader_seq: SeqNo(0),
        };

        let results = vec![ContractResult::Passed(ContractEvidence {
            contract_name: ContractName::from("transport"),
            upstream_stage: StageId::new(),
            downstream_stage: StageId::new(),
            evaluated_at: chrono::Utc::now(),
            details: json!({}).into(),
        })];

        let decision = policy.apply(&results, &edge, EdgeContractDecision::Pass);
        assert!(matches!(decision, EdgeContractDecision::Pass));
    }

    #[test]
    fn transport_strict_policy_fails_on_seq_divergence() {
        let policy = TransportStrictPolicy;
        let edge = EdgeContext {
            selected_population: false,
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
