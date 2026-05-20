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
//! are out of scope and move to FLOWIP-114k.
//!
//! See `obzenflow_improvement_proposals/content/P0/open/FLOWIP-114h-...md`
//! for the full contract.

pub mod flow_test_harness;
pub mod metrics_barrier;
pub mod probe;
pub mod stage_journal;
pub mod test_clock;

pub use flow_test_harness::FlowTestHarness;
pub use metrics_barrier::{MetricsBarrier, MetricsBarrierError};
pub use probe::{JournalProbe, JournalProbeError, JournalProbeEvent};
pub use stage_journal::StageJournalLookupError;
pub use test_clock::{TestClock, TestClockError};
