// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared supervision helpers for source supervisors (finite/infinite, sync/async).
//!
//! Reduces duplication across source supervisors while preserving
//! responsiveness and backpressure semantics.

use crate::backpressure::BackpressureWriter;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::supervision::backpressure_drain::{
    drain_one_pending, drain_one_pending_resolve, DrainAttempt, DrainOutcome,
};
use crate::supervised_base::idle_backoff::IdleBackoff;
use crate::supervised_base::{EventLoopDirective, EventReceiver};
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(crate) fn per_data_event_duration_for_batch(
    tick_duration: Duration,
    data_events_in_tick: usize,
) -> Duration {
    if data_events_in_tick == 0 {
        return Duration::from_nanos(0);
    }

    let nanos =
        (tick_duration.as_nanos() / data_events_in_tick as u128).min(u64::MAX as u128) as u64;
    Duration::from_nanos(nanos)
}

pub(crate) fn emit_batch_to_pending_outputs(
    events: impl IntoIterator<Item = ChainEvent>,
    stage_flow_context: &FlowContext,
    instrumentation: &Arc<StageInstrumentation>,
    per_data_event_duration: Duration,
    pending_outputs: &mut VecDeque<ChainEvent>,
) {
    for event in events {
        let staged_event = event.with_flow_context(stage_flow_context.clone());

        // Track error-marked events for lifecycle/flow rollups.
        if let ProcessingStatus::Error { kind, .. } = &staged_event.processing_info.status {
            let k = kind.clone().unwrap_or(ErrorKind::Unknown);
            instrumentation.record_error(k);
        }

        if staged_event.is_data() {
            instrumentation
                .events_processed_total
                .fetch_add(1, Ordering::Relaxed);
            instrumentation.record_processing_time(per_data_event_duration);
        }
        pending_outputs.push_back(staged_event);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_pending_outputs_sync(
    pending_outputs: &mut VecDeque<ChainEvent>,
    stage_flow_context: &FlowContext,
    stage_id: StageId,
    heartbeat_state: Option<Arc<HeartbeatState>>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    error_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_backoff: &mut IdleBackoff,
) -> Result<bool, BoxError> {
    while let Some(pending) = pending_outputs.pop_front() {
        if matches!(
            pending.processing_info.status,
            ProcessingStatus::Error { .. }
        ) {
            let pending = pending.with_runtime_context(instrumentation.snapshot_with_control());
            error_journal
                .append(pending, None)
                .await
                .map_err(|e| format!("Failed to write event: {e}"))?;
            continue;
        }

        match drain_one_pending(
            pending,
            stage_flow_context,
            stage_id,
            heartbeat_state.clone(),
            data_journal,
            system_journal,
            None,
            instrumentation,
            backpressure_writer,
            backpressure_pulse,
            backpressure_backoff,
            pending_outputs,
        )
        .await?
        {
            DrainOutcome::Committed { .. } => {}
            DrainOutcome::BackedOff => return Ok(true),
        }
    }

    Ok(false)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_pending_outputs_async<E>(
    pending_outputs: &mut VecDeque<ChainEvent>,
    stage_flow_context: &FlowContext,
    stage_id: StageId,
    heartbeat_state: Option<Arc<HeartbeatState>>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    error_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_backoff: &mut IdleBackoff,
    external_events: &mut EventReceiver<E>,
    on_channel_closed: impl FnOnce() -> E,
) -> Result<Option<EventLoopDirective<E>>, BoxError>
where
    E: Send,
{
    let mut on_channel_closed = Some(on_channel_closed);

    while let Some(pending) = pending_outputs.pop_front() {
        if matches!(
            pending.processing_info.status,
            ProcessingStatus::Error { .. }
        ) {
            let pending = pending.with_runtime_context(instrumentation.snapshot_with_control());
            error_journal
                .append(pending, None)
                .await
                .map_err(|e| format!("Failed to write event: {e}"))?;
            continue;
        }

        match drain_one_pending_resolve(
            pending,
            stage_flow_context,
            stage_id,
            heartbeat_state.clone(),
            data_journal,
            system_journal,
            None,
            instrumentation,
            backpressure_writer,
            backpressure_pulse,
            backpressure_backoff,
            pending_outputs,
        )
        .await?
        {
            DrainAttempt::Committed { .. } => {}
            DrainAttempt::BackedOff { delay } => {
                tokio::select! {
                    biased;
                    maybe_event = external_events.recv() => {
                        match maybe_event {
                            Some(event) => return Ok(Some(EventLoopDirective::Transition(event))),
                            None => {
                                let closed = on_channel_closed.take().expect("on_channel_closed is available");
                                return Ok(Some(EventLoopDirective::Transition(closed())));
                            }
                        }
                    }
                    _ = tokio::time::sleep(delay) => {}
                }
                return Ok(Some(EventLoopDirective::Continue));
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure::{BackpressurePlan, BackpressureRegistry};
    use crate::id_conversions::StageIdExt;
    use crate::supervised_base::ChannelBuilder;
    use async_trait::async_trait;
    use obzenflow_core::event::types::EventId;
    use obzenflow_core::event::{ChainEventFactory, JournalWriterId, WriterId};
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::{JournalError, JournalReader};
    use obzenflow_core::{ChainEvent, EventEnvelope, Journal};
    use obzenflow_topology::TopologyBuilder;
    use std::marker::PhantomData;
    use std::num::NonZeroU64;

    struct EmptyReader<T> {
        position: u64,
        _phantom: PhantomData<T>,
    }

    #[async_trait]
    impl<T> JournalReader<T> for EmptyReader<T>
    where
        T: obzenflow_core::event::JournalEvent,
    {
        async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
        }

        async fn skip(&mut self, _n: u64) -> Result<u64, JournalError> {
            Ok(0)
        }

        fn position(&self) -> u64 {
            self.position
        }
    }

    struct NoopJournal<T> {
        id: JournalId,
        _phantom: PhantomData<T>,
    }

    impl<T> NoopJournal<T> {
        fn new() -> Self {
            Self {
                id: JournalId::new(),
                _phantom: PhantomData,
            }
        }
    }

    #[async_trait]
    impl<T> Journal<T> for NoopJournal<T>
    where
        T: obzenflow_core::event::JournalEvent + Clone + 'static,
    {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&obzenflow_core::JournalOwner> {
            None
        }

        async fn append(
            &self,
            event: T,
            _parent: Option<&EventEnvelope<T>>,
        ) -> Result<EventEnvelope<T>, JournalError> {
            Ok(EventEnvelope::new(JournalWriterId::new(), event))
        }

        async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_causally_after(
            &self,
            _after_event_id: &EventId,
        ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
        }

        async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(EmptyReader {
                position: 0,
                _phantom: PhantomData,
            }))
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(EmptyReader {
                position,
                _phantom: PhantomData,
            }))
        }

        async fn read_last_n(&self, _count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }
    }

    #[derive(Debug)]
    enum TestEvent {
        BeginDrain,
        ChannelClosed,
    }

    #[tokio::test]
    async fn drain_pending_outputs_async_returns_external_event_while_backed_off() {
        if BackpressureWriter::is_bypass_enabled() {
            // When bypass is enabled, backpressure never blocks, so this test
            // cannot exercise the backoff select path.
            return;
        }

        // Arrange: a writer with credit=0 so drain_one_pending_resolve returns BackedOff.
        let mut builder = TopologyBuilder::new();
        let s_top = builder.add_stage(Some("s".to_string()));
        let d_top = builder.add_stage(Some("d".to_string()));
        let topology = builder.build_unchecked().expect("topology");

        let s = StageId::from_topology_id(s_top);
        let _d = StageId::from_topology_id(d_top);

        let plan =
            BackpressurePlan::disabled().with_stage_window(s, NonZeroU64::new(1).expect("window"));
        let registry = BackpressureRegistry::new(&topology, &plan);
        let writer = registry.writer(s);
        writer.reserve(1).expect("reserve").commit(1);
        assert!(writer.reserve(1).is_none(), "credit should be 0");

        let data_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(NoopJournal::new());
        let error_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(NoopJournal::new());
        let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(NoopJournal::new());
        let instrumentation = Arc::new(StageInstrumentation::new());

        let stage_flow_context = FlowContext {
            flow_name: "flow".to_string(),
            flow_id: "flow_id".to_string(),
            stage_name: "s".to_string(),
            stage_id: s,
            stage_type: obzenflow_core::event::context::StageType::FiniteSource,
        };

        let mut pending_outputs = VecDeque::new();
        pending_outputs.push_back(ChainEventFactory::data_event(
            WriterId::from(s),
            "test.event",
            serde_json::json!({"x": 1}),
        ));

        let mut backpressure_pulse = BackpressureActivityPulse::new();
        let mut backpressure_backoff = IdleBackoff::exponential_with_cap(
            Duration::from_millis(200),
            Duration::from_millis(200),
        );

        let (sender, mut receiver, _watcher) = ChannelBuilder::<TestEvent, ()>::new()
            .with_event_buffer(1)
            .build(());

        // Act: start draining (will back off), then deliver external event during the sleep.
        let drain_task = tokio::spawn(async move {
            drain_pending_outputs_async(
                &mut pending_outputs,
                &stage_flow_context,
                s,
                None,
                &data_journal,
                &error_journal,
                &system_journal,
                &instrumentation,
                &writer,
                &mut backpressure_pulse,
                &mut backpressure_backoff,
                &mut receiver,
                || TestEvent::ChannelClosed,
            )
            .await
        });

        // Give the drain task a moment to enter the backoff select.
        tokio::time::sleep(Duration::from_millis(10)).await;
        sender.send(TestEvent::BeginDrain).await.expect("send");

        let directive = drain_task
            .await
            .expect("join")
            .expect("drain result")
            .expect("expected directive");

        match directive {
            EventLoopDirective::Transition(TestEvent::BeginDrain) => {}
            other => panic!("expected Transition(BeginDrain), got {other:?}"),
        }
    }
}
