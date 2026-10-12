// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084l: waiting for each member's commit and recording its evidence.
//!
//! The wait runs in its own task, so a request's deadline or disconnect never
//! drops the duplicate or conflict evidence its answer depends on. Members are
//! reported as duplicates or conflicts only once that evidence is recorded.

use super::contract::{conflict_member, unknown_member, SubmissionMemberOutcome};
use super::shared::{EvidenceRecorder, RefusalEvidence};
use futures::stream::{FuturesUnordered, StreamExt};
use obzenflow_core::ingress::{
    IngressAttemptContext, IngressOriginal, IngressReceipt, IngressRefusalReason, IngressRequestId,
};
use obzenflow_core::EventId;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

/// One queued member and the receipt its commit resolves.
pub(crate) struct SettlementMember {
    /// Position within the request.
    pub position: usize,
    pub batch_index: Option<usize>,
    pub answer: oneshot::Receiver<IngressReceipt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberSettlement {
    Pending,
    Committed(EventId),
    Duplicate(EventId),
    Conflict,
    /// The receipt was dropped before resolving.
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvidenceState {
    Pending,
    Recorded,
    Unavailable,
}

#[derive(Debug, Clone)]
pub(crate) struct SettlementSnapshot {
    /// `(position, settlement)` per queued member.
    pub members: Vec<(usize, MemberSettlement)>,
    pub evidence: EvidenceState,
    done: bool,
}

impl SettlementSnapshot {
    /// The member's answer under the contract; duplicates and conflicts wait
    /// for their recorded evidence.
    pub(crate) fn outcome(
        &self,
        settlement: MemberSettlement,
        request_id: Option<&IngressRequestId>,
    ) -> SubmissionMemberOutcome {
        let recorded = self.evidence == EvidenceState::Recorded;
        match (settlement, request_id) {
            (MemberSettlement::Committed(event_id), _) => {
                SubmissionMemberOutcome::Committed { event_id }
            }
            (MemberSettlement::Duplicate(event_id), Some(_)) if recorded => {
                SubmissionMemberOutcome::Duplicate { event_id }
            }
            (MemberSettlement::Conflict, Some(id)) if recorded => conflict_member(id),
            _ => unknown_member(request_id.is_some()),
        }
    }
}

/// A request's members, settling in their own task.
pub(crate) struct Settlement {
    rx: watch::Receiver<SettlementSnapshot>,
}

impl Settlement {
    pub(crate) fn spawn(
        members: Vec<SettlementMember>,
        evidence: EvidenceRecorder,
        attempt: IngressAttemptContext,
        request_id: Option<IngressRequestId>,
    ) -> Self {
        let (tx, rx) = watch::channel(SettlementSnapshot {
            members: members
                .iter()
                .map(|member| (member.position, MemberSettlement::Pending))
                .collect(),
            evidence: EvidenceState::Pending,
            done: false,
        });
        tokio::spawn(settle(members, tx, evidence, attempt, request_id));
        Self { rx }
    }

    /// Wait for every member and its evidence, or until `deadline`; either
    /// way the snapshot shows what has settled.
    pub(crate) async fn wait_until(mut self, deadline: Instant) -> SettlementSnapshot {
        if tokio::time::timeout_at(deadline, self.rx.wait_for(|snapshot| snapshot.done))
            .await
            .is_err()
        {
            tracing::debug!("acknowledgement deadline passed before every member settled");
        }
        self.rx.borrow().clone()
    }
}

async fn settle(
    members: Vec<SettlementMember>,
    tx: watch::Sender<SettlementSnapshot>,
    evidence: EvidenceRecorder,
    attempt: IngressAttemptContext,
    request_id: Option<IngressRequestId>,
) {
    let batch_indices: Vec<Option<usize>> =
        members.iter().map(|member| member.batch_index).collect();
    let mut answers: FuturesUnordered<_> = members
        .into_iter()
        .enumerate()
        .map(|(slot, member)| async move { (slot, member.answer.await) })
        .collect();
    while let Some((slot, answer)) = answers.next().await {
        let settled = match answer {
            Ok(IngressReceipt::Committed { event_id }) => MemberSettlement::Committed(event_id),
            Ok(IngressReceipt::Duplicate { event_id }) => MemberSettlement::Duplicate(event_id),
            Ok(IngressReceipt::Conflict) => MemberSettlement::Conflict,
            Err(_) => MemberSettlement::Lost,
        };
        tx.send_modify(|snapshot| snapshot.members[slot].1 = settled);
    }

    let settled: Vec<MemberSettlement> = tx
        .borrow()
        .members
        .iter()
        .map(|(_, settlement)| *settlement)
        .collect();
    let mut state = EvidenceState::Recorded;
    if let Some(id) = request_id.as_ref() {
        let originals: Vec<IngressOriginal> = settled
            .iter()
            .zip(&batch_indices)
            .filter_map(|(settlement, batch_index)| match settlement {
                MemberSettlement::Duplicate(event_id) => Some(IngressOriginal {
                    batch_index: *batch_index,
                    event_id: *event_id,
                }),
                _ => None,
            })
            .collect();
        if !originals.is_empty()
            && evidence
                .duplicate(attempt.attempt_seq, id, originals)
                .await
                .is_err()
        {
            state = EvidenceState::Unavailable;
        }
        let conflicts = settled
            .iter()
            .filter(|settlement| **settlement == MemberSettlement::Conflict)
            .count() as u64;
        if conflicts > 0 {
            let conflicted = IngressAttemptContext {
                event_count: conflicts,
                ..attempt
            };
            let recorded = evidence
                .refusal(RefusalEvidence {
                    reason: IngressRefusalReason::IdempotencyKeyConflict,
                    attempt: &conflicted,
                    http_status: 422,
                    retry_after: None,
                    diagnostics: Vec::new(),
                    request_id: Some(id),
                })
                .await;
            if recorded.is_err() {
                state = EvidenceState::Unavailable;
            }
        }
    }
    tx.send_modify(|snapshot| {
        snapshot.evidence = state;
        snapshot.done = true;
    });
}
