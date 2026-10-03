// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Evidence owned by the existing contract evaluator, independent of policy.

use crate::event::types::{Count, SeqNo};
use crate::{EventDescriptor, EventId, SccId, StageId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractPhase {
    Progress,
    Final,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingReason {
    ProgressOnly,
    ProducerDeclarationUnavailable,
    ProductionCountUnavailable,
    WriterAdvertisementUnavailable,
    EvaluationUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkippedReason {
    NoProductionExpectation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DivergencePredicate {
    SignalsWhenNoData,
    SignalToDataRatio,
    CycleDepth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "evidence_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContractEvidenceDetails {
    Transport {
        advertised_writer_seq: Option<SeqNo>,
        consumed_count: Count,
    },
    Source {
        declaration_received: bool,
        expected_count: Option<Count>,
        produced_count: Option<Count>,
    },
    Delivery {
        consumed_total: u64,
        receipted_total: u64,
        pending_peak: usize,
        buffered_count: u64,
        success_count: u64,
        partial_count: u64,
        failed_count: u64,
        missing_count: usize,
        orphan_count: u64,
        missing_event_ids: Vec<EventId>,
    },
    Divergence {
        scc_id: SccId,
        window_seconds: u64,
        elapsed_ms: Option<u64>,
        evaluated_predicates: Vec<DivergencePredicate>,
        data_events: u64,
        flow_control_signals: u64,
        signal_to_data_ratio_threshold: f64,
        max_signals_when_no_data: u64,
        max_cycle_depth: u16,
        max_cycle_depth_observed: u16,
    },
    Progress,
    Custom {
        details: serde_json::Value,
    },
}

impl From<serde_json::Value> for ContractEvidenceDetails {
    fn from(details: serde_json::Value) -> Self {
        Self::Custom { details }
    }
}

/// The upstream-authored transport population observed by one subscription.
/// Selection applies within that population; forwarded rows retain their authors
/// and do not become production or receipts of this immediate upstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionScope {
    pub upstream: StageId,
    pub reader: StageId,
    pub selection: SubscriptionSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "selection", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubscriptionSelection {
    All,
    Selected { feeds: Vec<SubscriptionFeed> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionFeed {
    pub descriptor: EventDescriptor,
    pub role: Option<crate::event::payloads::system_payload::SystemFeedRole>,
}

/// Contiguous terminal-receipt frontier for the subscription's receipt population.
/// Absence of this object means receipt tracking is unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionReceipts {
    pub terminal_frontier: SeqNo,
    pub last_event_id: Option<EventId>,
}
