// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! [`JournalProbe`] (FLOWIP-114h): assert on per-stage data-event progress
//! through a stage's data journal.
//!
//! Specified for the non-cyclic single-writer-per-stage case. Cycle,
//! fan-in, and concurrent-writer semantics are deliberately out of scope
//! for 114h and are reserved for FLOWIP-114k's audit-driven migrations.

use crate::testing::FlowTestHarness;
use crate::testing::stage_journal::StageJournalLookupError;
use crate::testing::test_clock::SettleSchedulerError;
use crate::testing::TestClock;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::EventId;
use obzenflow_core::event::WriterId;
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Failure modes for [`JournalProbe`].
#[derive(Debug, Error)]
pub enum JournalProbeError {
    /// Stage-journal lookup failed (unknown name, ambiguous, or missing
    /// journal).
    #[error(transparent)]
    StageJournalLookup(#[from] StageJournalLookupError),

    /// Reading the stage journal failed (I/O, serialisation, etc.).
    #[error("failed to read stage journal: {0}")]
    JournalRead(String),

    /// `expect_event(n)` was called but the journal contained fewer than
    /// `n` data envelopes when the wait expired.
    #[error(
        "expected {expected} data event(s) but observed {observed} \
         on stage `{stage}` after waiting"
    )]
    NotEnoughEvents {
        stage: String,
        expected: u64,
        observed: u64,
    },

    /// `expect_no_event_within(...)` observed a data envelope after the
    /// scheduler barrier completed.
    #[error(
        "expected no data event within {window:?} on stage `{stage}`, \
         but observed {observed} envelope(s)"
    )]
    UnexpectedEvent {
        stage: String,
        window: Duration,
        observed: u64,
    },

    /// The observed envelope vector clock did not include the target stage's
    /// writer component.
    #[error(
        "missing stage-writer component `{writer_key}` in vector clock for event `{event_id}` \
         (stage id `{stage_id}`)"
    )]
    MissingStageWriterSeq {
        stage_id: StageId,
        writer_key: String,
        event_id: EventId,
    },

    /// Scheduler barrier failed to reach a stable observation under paused time.
    #[error(transparent)]
    SchedulerBarrier(#[from] SettleSchedulerError),
}

/// One observed data envelope and its derived stage-writer sequence number.
///
/// The producing stage writer's seq is the value the metrics aggregator's
/// exported watermark must cover before `MetricsBarrier::wait_for_stage_seq`
/// resolves. The envelope vector clock keyed by `WriterId::from(stage_id)`
/// carries that seq; this struct exposes it through
/// [`Self::stage_writer_seq`].
pub struct JournalProbeEvent {
    stage_id: StageId,
    envelope: EventEnvelope<ChainEvent>,
}

impl JournalProbeEvent {
    /// The producing stage's seq, derived from the envelope vector clock
    /// keyed by `WriterId::from(stage_id).to_string()`.
    ///
    /// For the non-cyclic single-writer-per-stage case this probe is
    /// specified for, the envelope's vector clock must contain the writer
    /// component keyed by `WriterId::from(stage_id)`. Missing keys are helper
    /// or event-construction bugs, not expected test control flow.
    pub fn stage_writer_seq(&self) -> Result<u64, JournalProbeError> {
        let key = WriterId::from(self.stage_id).to_string();
        self.envelope
            .vector_clock
            .clocks
            .get(&key)
            .copied()
            .ok_or_else(|| JournalProbeError::MissingStageWriterSeq {
                stage_id: self.stage_id,
                writer_key: key,
                event_id: self.envelope.event.id,
            })
    }

    /// The observed envelope.
    pub fn envelope(&self) -> &EventEnvelope<ChainEvent> {
        &self.envelope
    }
}

/// Per-stage data-journal probe.
///
/// Construct with [`JournalProbe::try_on_stage`] before the flow is
/// started or moved into [`FlowHandle::run`]; the probe owns a cloned
/// `Arc<dyn Journal<ChainEvent>>` and remains usable after the handle has
/// been consumed.
pub struct JournalProbe {
    stage_name: String,
    stage_id: StageId,
    journal: Arc<dyn Journal<ChainEvent>>,
}

impl JournalProbe {
    /// Build a probe pointing at the named stage's data journal.
    pub fn try_on_stage(
        handle: &FlowTestHarness,
        stage_name: &str,
    ) -> Result<Self, JournalProbeError> {
        let (stage_id, journal) = handle.stage_journal_for_test(stage_name)?;
        Ok(Self {
            stage_name: stage_name.to_string(),
            stage_id,
            journal,
        })
    }

    /// Wait until the stage has produced `n` data-event envelopes and
    /// return the `n`-th one.
    ///
    /// Counts data envelopes only: observability, lifecycle, and delivery
    /// envelopes routed through the same stage journal are ignored. Under
    /// paused-time runtimes, this loops on the pull-based journal cursor
    /// with `tokio::time::sleep` between EOF reads; under live runtimes the
    /// sleep simply yields wall time.
    pub async fn expect_event(&self, n: u64) -> Result<JournalProbeEvent, JournalProbeError> {
        assert!(n >= 1, "expect_event(n): n must be >= 1");
        loop {
            let envelopes = self.read_all_envelopes().await?;
            let mut data_count: u64 = 0;
            for envelope in envelopes {
                if envelope.event.is_data() {
                    data_count += 1;
                    if data_count == n {
                        return Ok(JournalProbeEvent {
                            stage_id: self.stage_id,
                            envelope,
                        });
                    }
                }
            }
            // Pull-based journal: yield so writer tasks can make progress before
            // the next read. Avoid timers so the probe composes with paused-time
            // runtimes (FLOWIP-114h).
            tokio::task::yield_now().await;
        }
    }

    /// Number of data-event envelopes observed so far, without blocking.
    pub async fn events_received_so_far(&self) -> Result<u64, JournalProbeError> {
        let envelopes = self.read_all_envelopes().await?;
        Ok(envelopes
            .into_iter()
            .filter(|env| env.event.is_data())
            .count() as u64)
    }

    /// Assert no data envelope arrives within `window`.
    ///
    /// Under paused time, this sleeps for `window`, then runs a scheduler
    /// barrier that yields until the runtime reports no scheduled work,
    /// before reading the journal. This pins the boundary-instant case: a
    /// task scheduled exactly at the boundary is fully polled before the
    /// final read.
    pub async fn expect_no_event_within(&self, window: Duration) -> Result<(), JournalProbeError> {
        let baseline = self.events_received_so_far().await?;
        tokio::time::sleep(window).await;
        let observed = TestClock::settle_scheduler(|| self.events_received_so_far()).await?;
        let delta = observed.saturating_sub(baseline);
        if delta == 0 {
            Ok(())
        } else {
            Err(JournalProbeError::UnexpectedEvent {
                stage: self.stage_name.clone(),
                window,
                observed: delta,
            })
        }
    }

    async fn read_all_envelopes(
        &self,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalProbeError> {
        let mut reader = self
            .journal
            .reader()
            .await
            .map_err(|e| JournalProbeError::JournalRead(e.to_string()))?;
        let mut envelopes = Vec::new();
        loop {
            match reader.next().await {
                Ok(Some(env)) => envelopes.push(env),
                Ok(None) => return Ok(envelopes),
                Err(e) => return Err(JournalProbeError::JournalRead(e.to_string())),
            }
        }
    }
}
