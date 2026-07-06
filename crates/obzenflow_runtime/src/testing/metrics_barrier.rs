// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! [`MetricsBarrier`] (FLOWIP-114h): wait for the metrics aggregator to
//! have exported a stage's data through the FLOWIP-059c metrics-watermark
//! `SystemEvent` stream, or to have published its drain-complete signal.
//!
//! The barrier consumes [`crate::pipeline::FlowHandle::system_journal`] and
//! filters for `MetricsCoordination::Exported` / `MetricsCoordination::Drained`
//! events. It does not invent a new aggregator surface; the events it relies
//! on are emitted by the production code at
//! `obzenflow_runtime/src/metrics/fsm.rs:1830-1907`.
//!
//! Cursor semantic is catch-up-then-poll: construction records a baseline
//! over the system journal and the wait loops scan from that baseline so a
//! covering watermark appended before the wait begins still resolves the
//! barrier.

use crate::testing::FlowTestHarness;
use obzenflow_core::event::system_event::{MetricsCoordinationEvent, SystemEventType};
use obzenflow_core::event::{SystemEvent, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Failure modes for [`MetricsBarrier`] construction.
#[derive(Debug, Error)]
pub enum MetricsBarrierError {
    /// The handle was built without a system journal. Metrics watermark
    /// events live on the system journal, so a flow without one cannot
    /// produce the signals the barrier waits on.
    #[error(
        "flow handle has no system journal; \
         cannot construct MetricsBarrier on a flow built without one"
    )]
    MissingSystemJournal,

    /// `try_on_stage` could not resolve the named stage in the topology.
    #[error("unknown stage `{0}` for MetricsBarrier::try_on_stage")]
    UnknownStage(String),

    /// `try_on_stage` resolved more than one stage with the same name.
    #[error("ambiguous stage `{0}`: multiple stages share this name")]
    AmbiguousStage(String),

    /// Reading the system journal failed.
    #[error("failed to read system journal: {0}")]
    JournalRead(String),

    /// The handle has no topology, so a stage-name lookup cannot succeed.
    #[error("flow handle has no topology; cannot resolve stage names")]
    MissingTopology,
}

/// A wait surface over the metrics aggregator's coordination stream.
///
/// Construct before [`crate::pipeline::FlowHandle::run`] consumes the handle.
/// The barrier owns a cloned `Arc<dyn Journal<SystemEvent>>` and remains usable
/// after the handle has been moved.
pub struct MetricsBarrier {
    system_journal: Arc<dyn Journal<SystemEvent>>,
    /// Writer key the watermark map uses for this stage, or `None` for the
    /// flow-wide drain barrier. Matches the production export loop at
    /// `metrics/fsm.rs:1846-1848`, which inserts entries with
    /// `WriterId::from(*stage_id).to_string()`.
    stage_writer_key: Option<String>,
    /// Catch-up baseline: total envelopes already present on the system
    /// journal at construction time. The wait loop scans from this offset
    /// before polling for newly appended events.
    baseline_offset: u64,
}

impl MetricsBarrier {
    /// Build a stage-targeted barrier. Resolves the stage name through the
    /// flow's topology to a `StageId`, then derives the watermark key as
    /// `WriterId::from(stage_id).to_string()`.
    pub async fn try_on_stage(
        handle: &FlowTestHarness,
        stage_name: &str,
    ) -> Result<Self, MetricsBarrierError> {
        let system_journal = handle
            .system_journal()
            .ok_or(MetricsBarrierError::MissingSystemJournal)?;

        let stage_id = resolve_stage_id(handle, stage_name)?;
        let stage_writer_key = Some(WriterId::from(stage_id).to_string());

        let baseline_offset = current_journal_offset(&system_journal).await?;

        Ok(Self {
            system_journal,
            stage_writer_key,
            baseline_offset,
        })
    }

    /// Build a flow-wide barrier for waiting on the drain-complete signal.
    pub async fn try_on_flow(handle: &FlowTestHarness) -> Result<Self, MetricsBarrierError> {
        let system_journal = handle
            .system_journal()
            .ok_or(MetricsBarrierError::MissingSystemJournal)?;

        let baseline_offset = current_journal_offset(&system_journal).await?;

        Ok(Self {
            system_journal,
            stage_writer_key: None,
            baseline_offset,
        })
    }

    /// Wait until the metrics aggregator has exported a snapshot covering
    /// this stage's writer up to `target_seq`.
    ///
    /// Catch-up-then-poll: scans the system journal from the construction
    /// baseline first, so a covering `Exported` event appended before this
    /// call resolves the wait immediately. If the catch-up scan fails to
    /// find a covering watermark, the helper polls newly appended events.
    pub async fn wait_for_stage_seq(&self, target_seq: u64) -> Result<(), MetricsBarrierError> {
        let writer_key = self
            .stage_writer_key
            .as_ref()
            .expect("wait_for_stage_seq called on a non-stage barrier");

        let mut scan_from = self.baseline_offset;
        loop {
            let envelopes = read_journal_from(&self.system_journal, scan_from).await?;
            let next_scan_from = scan_from + envelopes.len() as u64;
            for env in envelopes {
                if let SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Exported {
                    watermark,
                }) = &env.event.event
                {
                    if let Some(seq) = watermark.clocks.get(writer_key) {
                        if *seq >= target_seq {
                            return Ok(());
                        }
                    }
                }
            }
            scan_from = next_scan_from;
            // Under paused time, allow the runtime to auto-advance by awaiting a timer.
            // Under live time, this prevents a busy poll loop at EOF.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait until the metrics aggregator has published its drain-complete
    /// signal. Resolves on either `MetricsCoordination::Drained` or
    /// `MetricsCoordination::Shutdown` to match the existing
    /// `FlowHandle::run_with_metrics` polling contract.
    pub async fn wait_for_drained(&self) -> Result<(), MetricsBarrierError> {
        let mut scan_from = self.baseline_offset;
        loop {
            let envelopes = read_journal_from(&self.system_journal, scan_from).await?;
            let next_scan_from = scan_from + envelopes.len() as u64;
            for env in envelopes {
                if let SystemEventType::MetricsCoordination(
                    MetricsCoordinationEvent::Drained | MetricsCoordinationEvent::Shutdown,
                ) = &env.event.event
                {
                    return Ok(());
                }
            }
            scan_from = next_scan_from;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // FLOWIP-114h: scheduler barriers are provided by
    // `TestClock::settle_scheduler`, not by this helper.
}

fn resolve_stage_id(
    handle: &FlowTestHarness,
    stage_name: &str,
) -> Result<StageId, MetricsBarrierError> {
    use crate::id_conversions::StageIdExt;

    let topology = handle
        .topology()
        .ok_or(MetricsBarrierError::MissingTopology)?;

    let mut matches: Vec<StageId> = topology
        .stages()
        .filter(|s| s.name == stage_name)
        .map(|s| StageId::from_topology_id(s.id))
        .collect();

    match matches.len() {
        0 => Err(MetricsBarrierError::UnknownStage(stage_name.to_string())),
        1 => Ok(matches.remove(0)),
        _ => Err(MetricsBarrierError::AmbiguousStage(stage_name.to_string())),
    }
}

async fn current_journal_offset(
    journal: &Arc<dyn Journal<SystemEvent>>,
) -> Result<u64, MetricsBarrierError> {
    let mut reader = journal
        .reader()
        .await
        .map_err(|e| MetricsBarrierError::JournalRead(e.to_string()))?;
    let mut count: u64 = 0;
    loop {
        match reader.next().await {
            Ok(Some(_)) => count += 1,
            Ok(None) => return Ok(count),
            Err(e) => return Err(MetricsBarrierError::JournalRead(e.to_string())),
        }
    }
}

async fn read_journal_from(
    journal: &Arc<dyn Journal<SystemEvent>>,
    from: u64,
) -> Result<
    Vec<obzenflow_core::event::event_envelope::EventEnvelope<SystemEvent>>,
    MetricsBarrierError,
> {
    let mut reader = journal
        .reader_from(from)
        .await
        .map_err(|e| MetricsBarrierError::JournalRead(e.to_string()))?;
    let mut envelopes = Vec::new();
    loop {
        match reader.next().await {
            Ok(Some(env)) => envelopes.push(env),
            Ok(None) => return Ok(envelopes),
            Err(e) => return Err(MetricsBarrierError::JournalRead(e.to_string())),
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
    use obzenflow_core::event::event_envelope::EventEnvelope;
    use obzenflow_core::event::system_event::MetricsCoordinationEvent;
    use obzenflow_core::event::vector_clock::VectorClock;
    use obzenflow_core::event::{JournalEvent, SystemEvent, SystemEventType, WriterId};
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::journal::Journal;
    use obzenflow_core::StageId;
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

    fn harness_with_system_journal(
        system_journal: Arc<dyn Journal<SystemEvent>>,
        topology: Option<Arc<obzenflow_topology::Topology>>,
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
            topology,
            flow_name: "dummy".to_string(),
            contract_attachments: None,
            system_journal: Some(system_journal),
            pipeline_writer_id: obzenflow_core::event::WriterId::from(
                obzenflow_core::id::SystemId::new(),
            ),
            liveness_snapshots: None,
            run_substrate: crate::journal::RunSubstrateState::Ephemeral,
            flow_effective_config: None,
        };

        let handle = FlowHandle::new(standard_handle, None, extras);
        FlowTestHarness::from_parts(handle, Vec::new()).expect("empty stage journals")
    }

    fn harness_without_system_journal(
        topology: Option<Arc<obzenflow_topology::Topology>>,
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
            topology,
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
        FlowTestHarness::from_parts(handle, Vec::new()).expect("empty stage journals")
    }

    #[tokio::test]
    async fn try_on_flow_errors_when_system_journal_is_missing() {
        let harness = harness_without_system_journal(None);

        let err = MetricsBarrier::try_on_flow(&harness)
            .await
            .err()
            .expect("expected MissingSystemJournal");
        assert!(
            matches!(err, MetricsBarrierError::MissingSystemJournal),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn try_on_stage_errors_when_topology_is_missing() {
        let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(MemoryJournal::default());
        let harness = harness_with_system_journal(system_journal, None);

        let err = MetricsBarrier::try_on_stage(&harness, "stage")
            .await
            .err()
            .expect("expected MissingTopology");
        assert!(
            matches!(err, MetricsBarrierError::MissingTopology),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn try_on_stage_errors_on_unknown_stage_name() {
        let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(MemoryJournal::default());

        let mut topology_builder = TopologyBuilder::new();
        topology_builder.add_stage(Some("present".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let harness = harness_with_system_journal(system_journal, Some(topology));
        let err = MetricsBarrier::try_on_stage(&harness, "missing")
            .await
            .err()
            .expect("expected UnknownStage");
        assert!(
            matches!(err, MetricsBarrierError::UnknownStage(ref name) if name == "missing"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn try_on_stage_errors_on_ambiguous_stage_name() {
        let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(MemoryJournal::default());

        let mut topology_builder = TopologyBuilder::new();
        topology_builder.add_stage(Some("dup".to_string()));
        topology_builder.add_stage(Some("dup".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let harness = harness_with_system_journal(system_journal, Some(topology));
        let err = MetricsBarrier::try_on_stage(&harness, "dup")
            .await
            .err()
            .expect("expected AmbiguousStage");
        assert!(
            matches!(err, MetricsBarrierError::AmbiguousStage(ref name) if name == "dup"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn wait_for_drained_resolves_when_event_appended_before_wait_begins() {
        let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(MemoryJournal::default());
        let harness = harness_with_system_journal(system_journal.clone(), None);

        let barrier = MetricsBarrier::try_on_flow(&harness)
            .await
            .expect("construct barrier");

        system_journal
            .append(
                SystemEvent::new(
                    WriterId::from(StageId::new()),
                    SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained),
                ),
                None,
            )
            .await
            .expect("append drained");

        tokio::time::timeout(Duration::from_secs(1), barrier.wait_for_drained())
            .await
            .expect("wait should resolve within timeout")
            .expect("wait should succeed");
    }

    #[tokio::test]
    async fn wait_for_drained_resolves_when_shutdown_appended_before_wait_begins() {
        let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(MemoryJournal::default());
        let harness = harness_with_system_journal(system_journal.clone(), None);

        let barrier = MetricsBarrier::try_on_flow(&harness)
            .await
            .expect("construct barrier");

        system_journal
            .append(
                SystemEvent::new(
                    WriterId::from(StageId::new()),
                    SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Shutdown),
                ),
                None,
            )
            .await
            .expect("append shutdown");

        tokio::time::timeout(Duration::from_secs(1), barrier.wait_for_drained())
            .await
            .expect("wait should resolve within timeout")
            .expect("wait should succeed");
    }

    #[tokio::test]
    async fn wait_for_stage_seq_resolves_when_covering_export_is_already_present() {
        let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(MemoryJournal::default());

        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let writer_key = WriterId::from(stage_id).to_string();

        let harness = harness_with_system_journal(system_journal.clone(), Some(topology));
        let barrier = MetricsBarrier::try_on_stage(&harness, "stage")
            .await
            .expect("construct stage barrier");

        let mut watermark = VectorClock::new();
        watermark.clocks.insert(writer_key, 5);

        system_journal
            .append(
                SystemEvent::new(
                    WriterId::from(stage_id),
                    SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Exported {
                        watermark,
                    }),
                ),
                None,
            )
            .await
            .expect("append exported");

        tokio::time::timeout(Duration::from_secs(1), barrier.wait_for_stage_seq(3))
            .await
            .expect("wait should resolve within timeout")
            .expect("wait should succeed");
    }

    #[tokio::test]
    async fn wait_for_stage_seq_filters_unrelated_writer_progress() {
        let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(MemoryJournal::default());

        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        let other_topo_id = topology_builder.add_stage(Some("other".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let stage_key = WriterId::from(stage_id).to_string();
        let other_id = StageId::from_topology_id(other_topo_id);
        let other_key = WriterId::from(other_id).to_string();

        let harness = harness_with_system_journal(system_journal.clone(), Some(topology));
        let barrier = MetricsBarrier::try_on_stage(&harness, "stage")
            .await
            .expect("construct stage barrier");

        let mut wait = tokio::spawn(async move { barrier.wait_for_stage_seq(5).await });

        let mut watermark = VectorClock::new();
        watermark.clocks.insert(other_key, 5);
        system_journal
            .append(
                SystemEvent::new(
                    WriterId::from(other_id),
                    SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Exported {
                        watermark,
                    }),
                ),
                None,
            )
            .await
            .expect("append unrelated exported");

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !wait.is_finished(),
            "wait should not resolve on unrelated writer watermark"
        );

        let mut watermark = VectorClock::new();
        watermark.clocks.insert(stage_key, 5);
        system_journal
            .append(
                SystemEvent::new(
                    WriterId::from(stage_id),
                    SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Exported {
                        watermark,
                    }),
                ),
                None,
            )
            .await
            .expect("append covering exported");

        tokio::time::timeout(Duration::from_secs(1), &mut wait)
            .await
            .expect("wait should resolve within timeout")
            .expect("task should join")
            .expect("wait should succeed");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn wait_for_stage_seq_completes_under_paused_time_via_timer_polling() {
        let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(MemoryJournal::default());

        let mut topology_builder = TopologyBuilder::new();
        let stage_topo_id = topology_builder.add_stage(Some("stage".to_string()));
        topology_builder.add_stage(Some("sink".to_string()));
        let topology = Arc::new(topology_builder.build_unchecked().expect("topology"));

        let stage_id = StageId::from_topology_id(stage_topo_id);
        let stage_key = WriterId::from(stage_id).to_string();

        let harness = harness_with_system_journal(system_journal.clone(), Some(topology));
        let barrier = MetricsBarrier::try_on_stage(&harness, "stage")
            .await
            .expect("construct stage barrier");

        let system_journal_for_writer = system_journal.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut watermark = VectorClock::new();
            watermark.clocks.insert(stage_key, 2);

            system_journal_for_writer
                .append(
                    SystemEvent::new(
                        WriterId::from(stage_id),
                        SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Exported {
                            watermark,
                        }),
                    ),
                    None,
                )
                .await
                .expect("append exported");
        });

        let mut wait_task = tokio::spawn(async move { barrier.wait_for_stage_seq(2).await });
        let (watchdog_tx, mut watchdog_rx) = tokio::sync::oneshot::channel::<()>();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(2));
            let _ = watchdog_tx.send(());
        });

        tokio::select! {
            res = &mut wait_task => {
                res.expect("task should join").expect("wait should succeed");
            }
            _ = &mut watchdog_rx => {
                wait_task.abort();
                panic!("wait_for_stage_seq did not complete under paused time");
            }
        }
    }
}
