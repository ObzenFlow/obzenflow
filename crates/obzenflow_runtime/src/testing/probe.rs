// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! [`JournalProbe`] (FLOWIP-114h, extended by FLOWIP-114n): assert on per-stage
//! data-event progress through a stage's data journal.
//!
//! FLOWIP-114h specified the probe for the non-cyclic single-writer-per-stage
//! case. FLOWIP-114n extends the surface with explicit semantics for:
//! - fan-in (vector-clock component observation vs author attribution),
//! - direct-parent lineage matching,
//! - cycle-depth filtering (`cycle_scc_id` + `cycle_depth`),
//! - paused-time no-event assertions that advance virtual time explicitly.
//!
//! The probe counts *data envelopes* only (by `ChainEventContent::Data`), and it
//! counts them regardless of `processing_info.status`. Error-marked data events
//! are therefore counted by default.

use crate::testing::stage_journal::StageJournalLookupError;
use crate::testing::test_clock::SettleSchedulerError;
use crate::testing::test_clock::TestClockError;
use crate::testing::FlowTestHarness;
use crate::testing::TestClock;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::EventId;
use obzenflow_core::event::WriterId;
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_core::{CycleDepth, SccId};
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

    /// The flow handle was built without a system journal.
    #[error(
        "flow handle has no system journal; \
         cannot capture system events (build the handle with a system journal)"
    )]
    MissingSystemJournal,

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

    /// A paused-time assertion was attempted without a paused Tokio runtime.
    #[error(transparent)]
    Clock(#[from] TestClockError),

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
    /// Construct a probe directly from a stage id and stage journal.
    ///
    /// This is primarily for infra tests that assemble supervisors without a
    /// [`FlowTestHarness`]. `stage_name` is used for error messages only.
    pub fn on_journal(
        stage_name: impl Into<String>,
        stage_id: StageId,
        journal: Arc<dyn Journal<ChainEvent>>,
    ) -> Self {
        Self {
            stage_name: stage_name.into(),
            stage_id,
            journal,
        }
    }

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
            // Pull-based journal: wait briefly between reads.
            // Under paused time this allows the runtime to auto-advance; under
            // live time it avoids a busy poll loop at EOF (FLOWIP-114h).
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait until the stage has produced `n` data-event envelopes whose vector clock
    /// observes the given writer component (non-zero seq) and return the `n`-th one.
    ///
    /// This asserts only "the observed envelope's vector clock contains ancestry from
    /// `writer_id`". It does not assert author attribution, upstream-edge provenance, or
    /// direct lineage.
    pub async fn expect_event_observing_clock_component(
        &self,
        writer_id: WriterId,
        n: u64,
    ) -> Result<JournalProbeEvent, JournalProbeError> {
        assert!(
            n >= 1,
            "expect_event_observing_clock_component(n): n must be >= 1"
        );
        let writer_key = writer_id.to_string();
        loop {
            let envelopes = self.read_all_envelopes().await?;
            let mut count: u64 = 0;
            for envelope in envelopes {
                if !envelope.event.is_data() {
                    continue;
                }
                if envelope.vector_clock.get(&writer_key) == 0 {
                    continue;
                }
                count += 1;
                if count == n {
                    return Ok(JournalProbeEvent {
                        stage_id: self.stage_id,
                        envelope,
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Count the number of data-event envelopes observed so far whose vector clocks
    /// observe the given writer component (non-zero seq).
    pub async fn events_observing_clock_component_so_far(
        &self,
        writer_id: WriterId,
    ) -> Result<u64, JournalProbeError> {
        let writer_key = writer_id.to_string();
        let envelopes = self.read_all_envelopes().await?;
        Ok(envelopes
            .into_iter()
            .filter(|env| env.event.is_data() && env.vector_clock.get(&writer_key) != 0)
            .count() as u64)
    }

    /// Wait until the stage has produced `n` data-event envelopes whose direct parent
    /// is `parent_event_id` (matching `event.causality.parent_ids.first()`).
    pub async fn expect_event_child_of(
        &self,
        parent_event_id: EventId,
        n: u64,
    ) -> Result<JournalProbeEvent, JournalProbeError> {
        assert!(n >= 1, "expect_event_child_of(n): n must be >= 1");
        loop {
            let envelopes = self.read_all_envelopes().await?;
            let mut count: u64 = 0;
            for envelope in envelopes {
                if !envelope.event.is_data() {
                    continue;
                }
                if envelope.event.causality.parent_ids.first() != Some(&parent_event_id) {
                    continue;
                }
                count += 1;
                if count == n {
                    return Ok(JournalProbeEvent {
                        stage_id: self.stage_id,
                        envelope,
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait until the stage has produced `n` data-event envelopes authored by `writer_id`.
    ///
    /// This matches `envelope.event.writer_id` and must not be used as a proxy for
    /// fan-in upstream provenance.
    pub async fn expect_event_authored_by(
        &self,
        writer_id: WriterId,
        n: u64,
    ) -> Result<JournalProbeEvent, JournalProbeError> {
        assert!(n >= 1, "expect_event_authored_by(n): n must be >= 1");
        loop {
            let envelopes = self.read_all_envelopes().await?;
            let mut count: u64 = 0;
            for envelope in envelopes {
                if !envelope.event.is_data() {
                    continue;
                }
                if envelope.event.writer_id != writer_id {
                    continue;
                }
                count += 1;
                if count == n {
                    return Ok(JournalProbeEvent {
                        stage_id: self.stage_id,
                        envelope,
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait until the stage has produced `n` data-event envelopes at the requested cycle depth.
    ///
    /// Matches `ChainEvent::cycle_scc_id == Some(scc_id)` and
    /// `ChainEvent::cycle_depth == Some(depth)`.
    ///
    /// Under fan-out inside an SCC, multiple derived children may share the same
    /// `(cycle_scc_id, cycle_depth)` pair; this method counts matching envelopes, not
    /// logical cycle iterations.
    pub async fn expect_event_at_cycle_depth(
        &self,
        scc_id: SccId,
        depth: CycleDepth,
        n: u64,
    ) -> Result<JournalProbeEvent, JournalProbeError> {
        assert!(n >= 1, "expect_event_at_cycle_depth(n): n must be >= 1");
        loop {
            let envelopes = self.read_all_envelopes().await?;
            let mut count: u64 = 0;
            for envelope in envelopes {
                if !envelope.event.is_data() {
                    continue;
                }
                if envelope.event.cycle_scc_id != Some(scc_id) {
                    continue;
                }
                if envelope.event.cycle_depth != Some(depth) {
                    continue;
                }
                count += 1;
                if count == n {
                    return Ok(JournalProbeEvent {
                        stage_id: self.stage_id,
                        envelope,
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
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

    /// Assert that no additional data events appear after a paused-time scheduler settle.
    ///
    /// This does not advance time; it is intended for cycle-aware tests that control
    /// virtual time advancement explicitly and need a stable post-advance boundary.
    pub async fn expect_no_event_after_settle(&self) -> Result<(), JournalProbeError> {
        let baseline = self.events_received_so_far().await?;
        let observed = TestClock::settle_scheduler(|| self.events_received_so_far()).await?;
        let delta = observed.saturating_sub(baseline);
        if delta == 0 {
            Ok(())
        } else {
            Err(JournalProbeError::UnexpectedEvent {
                stage: self.stage_name.clone(),
                window: Duration::ZERO,
                observed: delta,
            })
        }
    }

    /// Assert that no additional data events appear while advancing paused Tokio time by `window`.
    ///
    /// The caller supplies a [`TestClock`] so the API is explicit about paused-time usage.
    pub async fn expect_no_event_during(
        &self,
        clock: &TestClock,
        window: Duration,
    ) -> Result<(), JournalProbeError> {
        let baseline = self.events_received_so_far().await?;
        clock.advance(window).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id_conversions::StageIdExt;
    use crate::pipeline::handle::FlowHandleExtras;
    use crate::pipeline::{FlowHandle, PipelineEvent, PipelineState};
    use crate::supervised_base::{ChannelBuilder, HandleBuilder, SupervisorTaskBuilder};
    use chrono::Utc;
    use obzenflow_core::event::event_envelope::EventEnvelope;
    use obzenflow_core::event::status::processing_status::ProcessingStatus;
    use obzenflow_core::event::vector_clock::VectorClock;
    use obzenflow_core::event::{ChainEvent, ChainEventFactory, JournalEvent, WriterId};
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::journal::Journal;
    use obzenflow_topology::TopologyBuilder;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn test_scc_id(n: u128) -> SccId {
        SccId::from_ulid(obzenflow_core::Ulid::from(n))
    }

    struct MemoryJournal<T: JournalEvent> {
        id: JournalId,
        owner: Option<JournalOwner>,
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    }

    impl<T: JournalEvent> Default for MemoryJournal<T> {
        fn default() -> Self {
            Self {
                id: JournalId::new(),
                owner: None,
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl<T: JournalEvent> MemoryJournal<T> {
        fn push_envelope(&self, envelope: EventEnvelope<T>) {
            let mut guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            guard.push(envelope);
        }
    }

    struct MemoryJournalReader<T: JournalEvent> {
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
        pos: usize,
    }

    #[async_trait::async_trait]
    impl<T> JournalReader<T> for MemoryJournalReader<T>
    where
        T: JournalEvent,
    {
        async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
            let guard = self
                .events
                .lock()
                .expect("MemoryJournalReader: poisoned lock");
            if self.pos >= guard.len() {
                return Ok(None);
            }
            let envelope = guard[self.pos].clone();
            drop(guard);
            self.pos += 1;
            Ok(Some(envelope))
        }

        fn position(&self) -> u64 {
            self.pos as u64
        }
    }

    #[async_trait::async_trait]
    impl<T> Journal<T> for MemoryJournal<T>
    where
        T: JournalEvent + 'static,
    {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn append(
            &self,
            event: T,
            _parent: Option<&EventEnvelope<T>>,
        ) -> Result<EventEnvelope<T>, JournalError> {
            let envelope =
                EventEnvelope::new(obzenflow_core::event::JournalWriterId::from(self.id), event);
            let mut guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            guard.push(envelope.clone());
            Ok(envelope)
        }

        async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            Ok(guard.clone())
        }

        async fn read_event(
            &self,
            event_id: &obzenflow_core::event::types::EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            Ok(guard.iter().find(|e| e.event.id() == event_id).cloned())
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(MemoryJournalReader {
                events: Arc::clone(&self.events),
                pos: position as usize,
            }))
        }

        async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            let len = guard.len();
            let start = len.saturating_sub(count);
            Ok(guard[start..].iter().rev().cloned().collect())
        }
    }

    fn harness_with_stage_journal(
        stage_name: &str,
        stage_id: StageId,
        stage_journal: Arc<dyn Journal<ChainEvent>>,
        topology: Arc<obzenflow_topology::Topology>,
    ) -> FlowTestHarness {
        let (event_sender, _event_receiver, state_watcher) =
            ChannelBuilder::<PipelineEvent, PipelineState>::new().build(PipelineState::Created);
        let supervisor_task = SupervisorTaskBuilder::<PipelineState>::new("dummy_pipeline")
            .spawn(|| async move { Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()) });
        let standard_handle = HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(supervisor_task)
            .build_standard()
            .expect("dummy handle should build");

        let extras = FlowHandleExtras {
            topology: Some(topology),
            flow_name: "dummy".to_string(),
            contract_attachments: None,
            system_journal: None,
            pipeline_writer_id: obzenflow_core::event::WriterId::from(
                obzenflow_core::id::SystemId::new(),
            ),
            liveness_snapshots: None,
            run_substrate: crate::journal::RunSubstrateState::Ephemeral,
            flow_effective_config: None,
        };

        let handle = FlowHandle::new(standard_handle, None, extras);
        FlowTestHarness::from_parts(handle, vec![(stage_id, stage_journal)])
            .unwrap_or_else(|e| panic!("failed to build FlowTestHarness for `{stage_name}`: {e}"))
    }

    #[tokio::test]
    async fn events_received_so_far_counts_data_envelopes_only() {
        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = topology_builder.build_unchecked().expect("topology");
        let topology = Arc::new(topology);

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        // Non-data envelopes should be ignored by the probe.
        stage_journal
            .append(ChainEventFactory::eof_event(writer_id, true), None)
            .await
            .expect("append eof");
        stage_journal
            .append(
                ChainEventFactory::data_event(writer_id, "data", serde_json::json!({})),
                None,
            )
            .await
            .expect("append data1");
        stage_journal
            .append(ChainEventFactory::drain_event(writer_id), None)
            .await
            .expect("append drain");
        stage_journal
            .append(
                ChainEventFactory::data_event(writer_id, "data", serde_json::json!({})),
                None,
            )
            .await
            .expect("append data2");

        let harness = harness_with_stage_journal("stage", stage_id, stage_journal, topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let observed = probe
            .events_received_so_far()
            .await
            .expect("events_received_so_far");
        assert_eq!(observed, 2, "expected to count data envelopes only");

        let second = probe.expect_event(2).await.expect("expect second data");
        assert!(second.envelope().event.is_data());
    }

    #[tokio::test]
    async fn stage_writer_seq_errors_when_vector_clock_is_missing_writer_component() {
        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = topology_builder.build_unchecked().expect("topology");
        let topology = Arc::new(topology);

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);
        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        let event = ChainEventFactory::data_event(writer_id, "data", serde_json::json!({}));
        let envelope = EventEnvelope {
            journal_writer_id: obzenflow_core::event::JournalWriterId::from(stage_journal_impl.id),
            vector_clock: VectorClock::new(),
            timestamp: Utc::now(),
            event,
        };
        stage_journal_impl.push_envelope(envelope);

        let harness = harness_with_stage_journal("stage", stage_id, stage_journal, topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let observed = probe.expect_event(1).await.expect("expect first data");
        let err = observed
            .stage_writer_seq()
            .expect_err("missing stage writer seq should error");
        assert!(
            matches!(err, JournalProbeError::MissingStageWriterSeq { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn expect_no_event_within_fails_when_event_written_at_boundary_instant() {
        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = topology_builder.build_unchecked().expect("topology");
        let topology = Arc::new(topology);

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        let harness =
            harness_with_stage_journal("stage", stage_id, stage_journal.clone(), topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let window = Duration::from_secs(1);
        tokio::spawn({
            let stage_journal = stage_journal.clone();
            async move {
                tokio::time::sleep(window).await;
                stage_journal
                    .append(
                        ChainEventFactory::data_event(writer_id, "data", serde_json::json!({})),
                        None,
                    )
                    .await
                    .expect("append data at boundary");
            }
        });

        let err = probe
            .expect_no_event_within(window)
            .await
            .expect_err("expected boundary instant event to be observed");
        assert!(
            matches!(err, JournalProbeError::UnexpectedEvent { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn expect_no_event_within_fails_on_chained_wakeup() {
        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = topology_builder.build_unchecked().expect("topology");
        let topology = Arc::new(topology);

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        let harness =
            harness_with_stage_journal("stage", stage_id, stage_journal.clone(), topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let window = Duration::from_secs(1);
        tokio::spawn({
            let stage_journal = stage_journal.clone();
            async move {
                tokio::time::sleep(window).await;
                tokio::spawn(async move {
                    stage_journal
                        .append(
                            ChainEventFactory::data_event(writer_id, "data", serde_json::json!({})),
                            None,
                        )
                        .await
                        .expect("append chained data");
                });
            }
        });

        let err = probe
            .expect_no_event_within(window)
            .await
            .expect_err("expected chained wakeup event to be observed");
        assert!(
            matches!(err, JournalProbeError::UnexpectedEvent { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn expect_event_observing_clock_component_filters_by_writer_component() {
        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let stage_writer_id = WriterId::from(stage_id);

        let upstream_a = WriterId::from(StageId::new());
        let upstream_b = WriterId::from(StageId::new());

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        // Non-data envelopes do not count.
        stage_journal
            .append(ChainEventFactory::eof_event(stage_writer_id, true), None)
            .await
            .expect("append eof");

        let mut clock_a = VectorClock::new();
        clock_a.clocks.insert(upstream_a.to_string(), 1);
        let env_a = EventEnvelope {
            journal_writer_id: obzenflow_core::event::JournalWriterId::from(stage_journal_impl.id),
            vector_clock: clock_a,
            timestamp: Utc::now(),
            event: ChainEventFactory::data_event(stage_writer_id, "data.a", serde_json::json!({})),
        };
        stage_journal_impl.push_envelope(env_a);

        let mut clock_b = VectorClock::new();
        clock_b.clocks.insert(upstream_b.to_string(), 1);
        let env_b = EventEnvelope {
            journal_writer_id: obzenflow_core::event::JournalWriterId::from(stage_journal_impl.id),
            vector_clock: clock_b,
            timestamp: Utc::now(),
            event: ChainEventFactory::data_event(stage_writer_id, "data.b", serde_json::json!({})),
        };
        stage_journal_impl.push_envelope(env_b);

        let harness = harness_with_stage_journal("stage", stage_id, stage_journal, topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let seen_a = probe
            .events_observing_clock_component_so_far(upstream_a)
            .await
            .expect("count upstream_a");
        assert_eq!(seen_a, 1);

        let seen_b = probe
            .events_observing_clock_component_so_far(upstream_b)
            .await
            .expect("count upstream_b");
        assert_eq!(seen_b, 1);

        let observed_b = probe
            .expect_event_observing_clock_component(upstream_b, 1)
            .await
            .expect("expect upstream_b-observing event");
        assert_ne!(
            observed_b
                .envelope()
                .vector_clock
                .get(&upstream_b.to_string()),
            0
        );
    }

    #[tokio::test]
    async fn expect_event_at_cycle_depth_counts_matching_envelopes() {
        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);

        let scc = test_scc_id(1);
        let depth = CycleDepth::new(4);

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        // One non-matching depth.
        let mut other = ChainEventFactory::data_event(writer_id, "other", serde_json::json!({}));
        other.cycle_scc_id = Some(scc);
        other.cycle_depth = Some(CycleDepth::new(3));
        stage_journal
            .append(other, None)
            .await
            .expect("append other");

        // Two matching envelopes at the same (scc, depth).
        for label in ["match.1", "match.2"] {
            let mut ev = ChainEventFactory::data_event(writer_id, label, serde_json::json!({}));
            ev.cycle_scc_id = Some(scc);
            ev.cycle_depth = Some(depth);
            stage_journal.append(ev, None).await.expect("append match");
        }

        let harness = harness_with_stage_journal("stage", stage_id, stage_journal, topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let second = probe
            .expect_event_at_cycle_depth(scc, depth, 2)
            .await
            .expect("expect 2nd match at cycle depth");
        assert_eq!(second.envelope().event.cycle_scc_id, Some(scc), "scc id");
        assert_eq!(
            second.envelope().event.cycle_depth,
            Some(depth),
            "cycle depth"
        );
    }

    #[tokio::test]
    async fn expect_event_at_cycle_depth_counts_fan_out_children_at_same_depth() {
        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);

        let scc = test_scc_id(2);
        let depth = CycleDepth::new(2);

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        let mut parent =
            ChainEventFactory::data_event(writer_id, "parent", serde_json::json!({"k": 1}));
        parent.cycle_scc_id = Some(scc);
        parent.cycle_depth = Some(depth);

        let child_1 = ChainEventFactory::derived_data_event(
            writer_id,
            &parent,
            "child.1",
            serde_json::json!({"k": 2}),
            obzenflow_core::config::LineagePolicy::default(),
        );
        let child_2 = ChainEventFactory::derived_data_event(
            writer_id,
            &parent,
            "child.2",
            serde_json::json!({"k": 3}),
            obzenflow_core::config::LineagePolicy::default(),
        );

        stage_journal
            .append(parent, None)
            .await
            .expect("append parent");
        stage_journal
            .append(child_1.clone(), None)
            .await
            .expect("append child_1");
        stage_journal
            .append(child_2.clone(), None)
            .await
            .expect("append child_2");

        let harness = harness_with_stage_journal("stage", stage_id, stage_journal, topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        // Parent is match #1, then each child is its own distinct match.
        let third = probe
            .expect_event_at_cycle_depth(scc, depth, 3)
            .await
            .expect("expect 3rd match at cycle depth");
        assert_eq!(
            third.envelope().event.causality.parent_ids.first(),
            Some(&child_2.causality.parent_ids[0]),
            "sanity: lineage still present"
        );
        assert_eq!(third.envelope().event.id, child_2.id);
    }

    #[tokio::test]
    async fn data_event_counting_includes_error_marked_data() {
        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        let mut ok = ChainEventFactory::data_event(writer_id, "ok", serde_json::json!({}));
        ok.processing_info.status = ProcessingStatus::Success;
        stage_journal.append(ok, None).await.expect("append ok");

        let mut err = ChainEventFactory::data_event(writer_id, "err", serde_json::json!({}));
        err.processing_info.status = ProcessingStatus::error("boom");
        stage_journal
            .append(err.clone(), None)
            .await
            .expect("append err");

        let harness = harness_with_stage_journal("stage", stage_id, stage_journal, topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let count = probe
            .events_received_so_far()
            .await
            .expect("events_received_so_far");
        assert_eq!(count, 2);

        let observed = probe.expect_event(2).await.expect("expect 2nd data");
        assert!(matches!(
            observed.envelope().event.processing_info.status,
            ProcessingStatus::Error { .. }
        ));
        assert_eq!(observed.envelope().event.id, err.id);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn expect_no_event_during_advances_time_and_observes_boundary_instant_events() {
        let clock = TestClock::new().await.expect("paused runtime");

        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        let harness =
            harness_with_stage_journal("stage", stage_id, stage_journal.clone(), topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let window = Duration::from_secs(1);
        tokio::spawn({
            let stage_journal = stage_journal.clone();
            async move {
                tokio::time::sleep(window).await;
                stage_journal
                    .append(
                        ChainEventFactory::data_event(writer_id, "data", serde_json::json!({})),
                        None,
                    )
                    .await
                    .expect("append data at boundary");
            }
        });

        let err = probe
            .expect_no_event_during(&clock, window)
            .await
            .expect_err("boundary instant event should be observed");
        assert!(
            matches!(err, JournalProbeError::UnexpectedEvent { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn expect_no_event_during_observes_chained_wakeup_events() {
        let clock = TestClock::new().await.expect("paused runtime");

        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        let harness =
            harness_with_stage_journal("stage", stage_id, stage_journal.clone(), topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let window = Duration::from_secs(1);
        tokio::spawn({
            let stage_journal = stage_journal.clone();
            async move {
                tokio::time::sleep(window).await;
                tokio::spawn(async move {
                    stage_journal
                        .append(
                            ChainEventFactory::data_event(writer_id, "data", serde_json::json!({})),
                            None,
                        )
                        .await
                        .expect("append chained data");
                });
            }
        });

        let err = probe
            .expect_no_event_during(&clock, window)
            .await
            .expect_err("chained wakeup event should be observed");
        assert!(
            matches!(err, JournalProbeError::UnexpectedEvent { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn expect_event_returns_append_order_under_concurrent_writes() {
        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_id = WriterId::from(stage_id);

        let stage_journal_impl: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::default());
        let stage_journal: Arc<dyn Journal<ChainEvent>> = stage_journal_impl.clone();

        let harness =
            harness_with_stage_journal("stage", stage_id, stage_journal.clone(), topology);
        let probe = JournalProbe::try_on_stage(&harness, "stage").expect("probe");

        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        for payload in ["a", "b"] {
            let stage_journal = stage_journal.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                stage_journal
                    .append(
                        ChainEventFactory::data_event(
                            writer_id,
                            "data",
                            serde_json::json!({"payload": payload}),
                        ),
                        None,
                    )
                    .await
                    .expect("append");
            });
        }

        // Release both writers.
        barrier.wait().await;

        let first = probe.expect_event(1).await.expect("first");
        let second = probe.expect_event(2).await.expect("second");
        assert_ne!(first.envelope().event.id, second.envelope().event.id);
    }
}
