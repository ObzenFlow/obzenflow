// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Helpers for deterministic tests, behind the `test-support` feature.
//!
//! Wait on [`JournalProbe`], [`JournalSnapshot`] or [`MetricsBarrier`] instead of
//! a fixed sleep. For time-driven behaviour, use [`TestClock`] with
//! `#[tokio::test(start_paused = true)]`. A probe sees only committed records, so
//! before asserting that something did not happen, finish the producers involved
//! or read a completed journal prefix.

#[cfg(test)]
pub(crate) mod causal_fixture;
pub mod delivered_order;
pub mod flow_test_harness;
pub mod journal_snapshot;
pub mod metrics;
pub mod metrics_barrier;
pub mod pipeline;
pub mod probe;
pub mod sink;
pub mod stage_journal;
pub mod test_clock;

pub use crate::backpressure::BackpressureAckGate;
pub use delivered_order::{DeliveredOrderProjection, DeliveredOrderRow};
pub use flow_test_harness::FlowTestHarness;
pub use journal_snapshot::{
    assert_concurrent, assert_happens_before, CausalAssertionError, EventShape, FanOutGroup,
    JournalExpectation, JournalExpectationError, JournalOrder, JournalSnapshot, ParentEventId,
    ParentSelector, SequenceMatchMode,
};
pub use metrics_barrier::{MetricsBarrier, MetricsBarrierError};
pub use probe::{JournalProbe, JournalProbeError, JournalProbeEvent};
pub use stage_journal::StageJournalLookupError;
pub use test_clock::{TestClock, TestClockError};
