// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! [`JournalProbe`] (FLOWIP-114h): assert on per-stage data-event progress
//! through a stage's data journal.
//!
//! Specified for the non-cyclic single-writer-per-stage case. Cycle,
//! fan-in, and concurrent-writer semantics are deliberately out of scope
//! for 114h and are reserved for FLOWIP-114k's audit-driven migrations.

use crate::testing::stage_journal::StageJournalLookupError;
use crate::testing::test_clock::SettleSchedulerError;
use crate::testing::FlowTestHarness;
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
            // Pull-based journal: wait briefly between reads.
            // Under paused time this allows the runtime to auto-advance; under
            // live time it avoids a busy poll loop at EOF (FLOWIP-114h).
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

        async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
            let guard = self
                .events
                .lock()
                .expect("MemoryJournalReader: poisoned lock");
            let len = guard.len();
            drop(guard);
            let before = self.pos;
            self.pos = std::cmp::min(len, self.pos.saturating_add(n as usize));
            Ok((self.pos - before) as u64)
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

        async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            Ok(guard.clone())
        }

        async fn read_causally_after(
            &self,
            after_event_id: &obzenflow_core::event::types::EventId,
        ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            let start = guard
                .iter()
                .position(|e| e.event.id() == after_event_id)
                .map(|idx| idx + 1)
                .unwrap_or(guard.len());
            Ok(guard[start..].to_vec())
        }

        async fn read_event(
            &self,
            event_id: &obzenflow_core::event::types::EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            Ok(guard.iter().find(|e| e.event.id() == event_id).cloned())
        }

        async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(MemoryJournalReader {
                events: Arc::clone(&self.events),
                pos: 0,
            }))
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
            liveness_snapshots: None,
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
}
