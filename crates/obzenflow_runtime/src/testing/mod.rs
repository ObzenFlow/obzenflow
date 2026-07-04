// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Test-only helpers for deterministic CI tests (FLOWIP-114h).
//!
//! Gated behind `#[cfg(any(test, feature = "test-support"))]`. The
//! `test-support` feature pulls in `tokio/test-util` so callers can use
//! `#[tokio::test(start_paused = true)]` together with [`TestClock`].
//!
//! The primitives are scoped to FLOWIP-114h's non-cyclic single-writer
//! case. Cycle, fan-in, and concurrent-writer semantics for [`JournalProbe`]
//! are out of scope and move to FLOWIP-114n.
//!
//! See `obzenflow_improvement_proposals/content/P0/open/FLOWIP-114h-...md`
//! for the full contract.

pub mod delivered_order;
pub mod flow_test_harness;
pub mod journal_snapshot;
pub mod memory_journal;
pub mod metrics_barrier;
pub mod probe;
pub mod stage_journal;
pub mod test_clock;

pub use delivered_order::{DeliveredOrderProjection, DeliveredOrderRow};
pub use flow_test_harness::FlowTestHarness;
pub use journal_snapshot::{
    assert_concurrent, assert_happens_before, CausalAssertionError, EventShape, FanOutGroup,
    JournalExpectation, JournalExpectationError, JournalOrder, JournalSnapshot, ParentEventId,
    ParentSelector, SequenceMatchMode,
};
pub use memory_journal::MemoryJournal;
pub use metrics_barrier::{MetricsBarrier, MetricsBarrierError};
pub use probe::{JournalProbe, JournalProbeError, JournalProbeEvent};
pub use stage_journal::StageJournalLookupError;
pub use test_clock::{TestClock, TestClockError};
