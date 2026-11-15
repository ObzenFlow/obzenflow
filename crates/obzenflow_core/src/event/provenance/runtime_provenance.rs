// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Protected execution accounting. These counts survive observation omission.

use crate::event::payloads::execution_payload::{RecoveryFact, RetryStopReason};
use crate::event::status::processing_status::ErrorKind;
use crate::{EventType, StageId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProvenance {
    pub accounting: ExecutionAccounting,
}

/// Counts retain their existing physical input/output populations. In particular,
/// composite protocol and framework effect rows are not erased by the `fact` kind.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAccounting {
    pub events_processed_total: u64,
    pub events_accumulated_total: u64,
    pub events_emitted_total: u64,
    pub errors_total: u64,
    pub failures_total: u64,
    pub errors_by_kind: HashMap<ErrorKind, u64>,
    pub data_outputs_by_event_type: Vec<EventTypeCountContext>,
    pub data_inputs_by_upstream_event_type: Vec<UpstreamEventTypeCountContext>,
    pub retry: RetryAccounting,
}

/// Counts of the retry facts a stage committed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryAccounting {
    pub scheduled_total: u64,
    pub calls_by_outcome: BTreeMap<RetryCallOutcome, u64>,
    pub backoff_ms_total: u64,
}

impl RetryAccounting {
    /// Adds one committed recovery fact to the counts.
    pub fn record(&mut self, fact: &RecoveryFact) {
        match fact {
            RecoveryFact::RetryScheduled { .. } => {
                self.scheduled_total = self.scheduled_total.saturating_add(1);
            }
            RecoveryFact::RetrySucceeded { .. } => self.count_call(RetryCallOutcome::Succeeded),
            RecoveryFact::RetryStoppedNonRetryable { .. } => {
                self.count_call(RetryCallOutcome::NotRetryable)
            }
            RecoveryFact::RetryExhausted { reason, .. } => {
                self.count_call(RetryCallOutcome::GaveUp(*reason))
            }
            RecoveryFact::RecoveryCompleted {
                backoff_elapsed_ms, ..
            } => {
                self.backoff_ms_total = self.backoff_ms_total.saturating_add(*backoff_elapsed_ms);
            }
            RecoveryFact::AttemptCompleted { .. } => {}
        }
    }

    /// Calls that ended in `outcome`.
    pub fn calls(&self, outcome: RetryCallOutcome) -> u64 {
        self.calls_by_outcome.get(&outcome).copied().unwrap_or(0)
    }

    /// True when no retry fact has been counted.
    pub fn is_empty(&self) -> bool {
        self.scheduled_total == 0 && self.calls_by_outcome.is_empty() && self.backoff_ms_total == 0
    }

    /// Adds `other`'s counts to these.
    pub fn add_counts(&mut self, other: &Self) {
        self.scheduled_total = self.scheduled_total.saturating_add(other.scheduled_total);
        self.backoff_ms_total = self.backoff_ms_total.saturating_add(other.backoff_ms_total);
        for (outcome, total) in &other.calls_by_outcome {
            let current = self.calls_by_outcome.entry(*outcome).or_insert(0);
            *current = current.saturating_add(*total);
        }
    }

    fn count_call(&mut self, outcome: RetryCallOutcome) {
        let total = self.calls_by_outcome.entry(outcome).or_insert(0);
        *total = total.saturating_add(1);
    }
}

/// How a call that retried, waited or gave up ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryCallOutcome {
    Succeeded,
    NotRetryable,
    #[serde(untagged)]
    GaveUp(RetryStopReason),
}

impl RetryCallOutcome {
    pub const ALL: [Self; 6] = [
        Self::Succeeded,
        Self::NotRetryable,
        Self::GaveUp(RetryStopReason::AttemptLimit),
        Self::GaveUp(RetryStopReason::AttemptStartWindow),
        Self::GaveUp(RetryStopReason::CircuitNoLongerClosed),
        Self::GaveUp(RetryStopReason::OutageWaitLimit),
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::NotRetryable => "not_retryable",
            Self::GaveUp(reason) => reason.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTypeCountContext {
    pub event_type: EventType,
    pub event_kind: crate::event::EventKind,
    pub payload_schema_version: std::num::NonZeroU32,
    pub total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamEventTypeCountContext {
    pub upstream: StageId,
    pub event_type: EventType,
    pub event_kind: crate::event::EventKind,
    pub payload_schema_version: std::num::NonZeroU32,
    pub total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_outcomes_round_trip_as_labels() {
        let accounting = RetryAccounting {
            scheduled_total: 1,
            calls_by_outcome: RetryCallOutcome::ALL.into_iter().zip(1..).collect(),
            backoff_ms_total: 250,
        };
        let json = serde_json::to_value(&accounting).unwrap();
        let mut keys: Vec<&str> = json["calls_by_outcome"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let mut labels = RetryCallOutcome::ALL.map(RetryCallOutcome::as_str);
        keys.sort_unstable();
        labels.sort_unstable();
        assert_eq!(keys, labels);
        assert_eq!(
            serde_json::from_value::<RetryAccounting>(json).unwrap(),
            accounting
        );
    }
}
