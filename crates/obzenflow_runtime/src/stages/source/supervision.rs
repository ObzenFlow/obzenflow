// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared supervision helpers for source supervisors (finite/infinite, sync/async).
//!
//! Reduces duplication across source supervisors while preserving
//! responsiveness and backpressure semantics.

use crate::backpressure::BackpressureWriter;
use crate::feed_plan::StageOutputContract;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::supervision::backpressure_drain::{
    drain_one_pending, drain_one_pending_resolve, DrainAttempt, DrainOutcome,
};
use crate::stages::observer::dispatch::run_source_poll_observers;
use crate::stages::observer::{
    SourcePollObserverContext, SourcePollObserverOutcome, StageObserverBundle,
};
use crate::stages::source::boundary::{
    SourceBoundary, SourceBoundaryOutcome, SourceBoundaryReport, SourcePollExecution,
};
use crate::supervised_base::{EventLoopDirective, EventReceiver};
use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(crate) async fn around_source_boundary<'a>(
    boundary: Option<Arc<dyn SourceBoundary>>,
    execute: SourcePollExecution<'a>,
) -> SourceBoundaryReport {
    match boundary {
        Some(boundary) => boundary.around_poll(execute).await,
        None => SourceBoundaryReport {
            outcome: SourceBoundaryOutcome::Polled(execute.await),
            control_events: Vec::new(),
        },
    }
}

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
    scope: obzenflow_core::MiddlewareExecutionScope,
    pending_outputs: &mut VecDeque<
        crate::stages::common::supervision::backpressure_drain::PendingOutput,
    >,
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
        pending_outputs.push_back(
            crate::stages::common::supervision::backpressure_drain::PendingOutput {
                event: staged_event,
                scope,
            },
        );
    }
}

pub(crate) struct SourcePollObservation<'a> {
    stage_flow_context: &'a FlowContext,
    instrumentation: &'a Arc<StageInstrumentation>,
    observers: &'a StageObserverBundle,
    scope: MiddlewareExecutionScope,
    data_journal: &'a Arc<dyn Journal<ChainEvent>>,
}

impl<'a> SourcePollObservation<'a> {
    pub(crate) fn new(
        stage_flow_context: &'a FlowContext,
        instrumentation: &'a Arc<StageInstrumentation>,
        observers: &'a StageObserverBundle,
        scope: MiddlewareExecutionScope,
        data_journal: &'a Arc<dyn Journal<ChainEvent>>,
    ) -> Self {
        Self {
            stage_flow_context,
            instrumentation,
            observers,
            scope,
            data_journal,
        }
    }

    pub(crate) async fn observe(
        &self,
        outputs: &mut [ChainEvent],
        poll_duration: Duration,
        outcome: SourcePollObserverOutcome,
    ) -> Result<(), BoxError> {
        let observer_ctx = SourcePollObserverContext {
            stage_id: self.stage_flow_context.stage_id,
            stage_name: &self.stage_flow_context.stage_name,
            flow_context: self.stage_flow_context,
            scope: self.scope,
            poll_duration,
            outcome,
        };
        run_source_poll_observers(
            self.observers,
            &observer_ctx,
            outputs,
            self.data_journal,
            self.instrumentation,
        )
        .await
    }

    pub(crate) async fn observe_empty(
        &self,
        poll_duration: Duration,
        outcome: SourcePollObserverOutcome,
    ) -> Result<(), BoxError> {
        let mut outputs = Vec::new();
        self.observe(outputs.as_mut_slice(), poll_duration, outcome)
            .await
    }
}

pub(crate) async fn observe_source_boundary_rejection(
    observation: &SourcePollObservation<'_>,
    control_events: &mut Vec<ChainEvent>,
    reason: &str,
) -> Result<(), BoxError> {
    let outcome = SourcePollObserverOutcome::Rejected {
        reason: reason.to_string(),
    };
    if control_events.is_empty() {
        observation
            .observe_empty(Duration::from_nanos(0), outcome)
            .await
    } else {
        observation
            .observe(
                control_events.as_mut_slice(),
                Duration::from_nanos(0),
                outcome,
            )
            .await
    }
}

pub(crate) fn stage_source_poll_outputs(
    events: Vec<ChainEvent>,
    stage_flow_context: &FlowContext,
    instrumentation: &Arc<StageInstrumentation>,
    poll_duration: Duration,
    scope: obzenflow_core::MiddlewareExecutionScope,
    pending_outputs: &mut VecDeque<
        crate::stages::common::supervision::backpressure_drain::PendingOutput,
    >,
) {
    let data_events_in_tick = events.iter().filter(|event| event.is_data()).count();
    let per_data_event_duration =
        per_data_event_duration_for_batch(poll_duration, data_events_in_tick);
    emit_batch_to_pending_outputs(
        events,
        stage_flow_context,
        instrumentation,
        per_data_event_duration,
        scope,
        pending_outputs,
    );
}

pub(crate) fn stage_boundary_control_events(
    control_events: Vec<ChainEvent>,
    stage_flow_context: &FlowContext,
    instrumentation: &Arc<StageInstrumentation>,
    scope: obzenflow_core::MiddlewareExecutionScope,
    pending_outputs: &mut VecDeque<
        crate::stages::common::supervision::backpressure_drain::PendingOutput,
    >,
) -> bool {
    if control_events.is_empty() {
        return false;
    }

    emit_batch_to_pending_outputs(
        control_events,
        stage_flow_context,
        instrumentation,
        Duration::from_nanos(0),
        scope,
        pending_outputs,
    );
    true
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_pending_outputs_sync(
    pending_outputs: &mut VecDeque<
        crate::stages::common::supervision::backpressure_drain::PendingOutput,
    >,
    stage_flow_context: &FlowContext,
    stage_id: StageId,
    heartbeat_state: Option<Arc<HeartbeatState>>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    error_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_stall: &mut Option<tokio::time::Instant>,
    output_contract: Option<&StageOutputContract>,
    observers: Option<&crate::stages::observer::StageObserverBundle>,
) -> Result<bool, BoxError> {
    while let Some(pending) = pending_outputs.pop_front() {
        if matches!(
            pending.event.processing_info.status,
            ProcessingStatus::Error { .. }
        ) {
            let event = pending
                .event
                .with_runtime_context(instrumentation.snapshot_with_control());
            error_journal
                .append(event, None)
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
            backpressure_stall,
            output_contract,
            observers,
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
    pending_outputs: &mut VecDeque<
        crate::stages::common::supervision::backpressure_drain::PendingOutput,
    >,
    stage_flow_context: &FlowContext,
    stage_id: StageId,
    heartbeat_state: Option<Arc<HeartbeatState>>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    error_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_stall: &mut Option<tokio::time::Instant>,
    output_contract: Option<&StageOutputContract>,
    observers: Option<&crate::stages::observer::StageObserverBundle>,
    external_events: &mut EventReceiver<E>,
    on_channel_closed: impl FnOnce() -> E,
) -> Result<Option<EventLoopDirective<E>>, BoxError>
where
    E: Send,
{
    let mut on_channel_closed = Some(on_channel_closed);

    while let Some(pending) = pending_outputs.pop_front() {
        if matches!(
            pending.event.processing_info.status,
            ProcessingStatus::Error { .. }
        ) {
            let event = pending
                .event
                .with_runtime_context(instrumentation.snapshot_with_control());
            error_journal
                .append(event, None)
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
            backpressure_stall,
            output_contract,
            observers,
            pending_outputs,
        )
        .await?
        {
            DrainAttempt::Committed { .. } => {}
            DrainAttempt::BackedOff { bound, waker } => {
                // Sources keep their out-of-band arm: the credit wait races
                // the external-events channel, so drain, EOF, cancellation,
                // and shutdown interrupt the wait directly. The chunk bound
                // still applies; the anchored stall episode in
                // `backpressure_stall` decides the stall on re-entry.
                let wait_started = tokio::time::Instant::now();
                let wake = crate::stages::common::control_strategies::WakeOn::Notify(waker);
                let waited = tokio::select! {
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
                    _ = crate::stages::common::supervision::suspension::suspend_until(
                        &wake,
                        Some(bound),
                    ) => wait_started.elapsed(),
                };
                // The chunk elapsed (the external-event arm returns early
                // above, so this only runs on a real wait): record the wait
                // and feed the blocked pulse through the same append/mirror
                // path the sync drain uses, so async-source pulses are not
                // silently dropped.
                backpressure_writer.record_wait(waited);
                crate::stages::common::supervision::backpressure_drain::emit_blocked_pulse(
                    stage_id,
                    stage_flow_context,
                    waited,
                    data_journal,
                    system_journal,
                    instrumentation,
                    backpressure_writer,
                    backpressure_pulse,
                )
                .await;
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

        async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
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

    /// A journal that records appended events so a test can inspect what the
    /// drain authored (the blocked pulse, in particular).
    struct RecordingJournal<T> {
        id: JournalId,
        appended: std::sync::Mutex<Vec<T>>,
        _phantom: PhantomData<T>,
    }

    impl<T: Clone> RecordingJournal<T> {
        fn new() -> Self {
            Self {
                id: JournalId::new(),
                appended: std::sync::Mutex::new(Vec::new()),
                _phantom: PhantomData,
            }
        }

        fn appended(&self) -> Vec<T> {
            self.appended
                .lock()
                .expect("recording journal lock")
                .clone()
        }
    }

    #[async_trait]
    impl<T> Journal<T> for RecordingJournal<T>
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
            self.appended
                .lock()
                .expect("recording journal lock")
                .push(event.clone());
            Ok(EventEnvelope::new(JournalWriterId::new(), event))
        }

        async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
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

    #[test]
    fn stage_boundary_control_events_stages_rejected_outbox() {
        let stage_id = StageId::new();
        let instrumentation = Arc::new(StageInstrumentation::new());
        let stage_flow_context = FlowContext {
            flow_name: "flow".to_string(),
            flow_id: "flow_id".to_string(),
            stage_name: "source".to_string(),
            stage_id,
            stage_type: obzenflow_core::event::context::StageType::FiniteSource,
        };
        let control_event = ChainEventFactory::data_event(
            WriterId::from(stage_id),
            "test.boundary_control",
            serde_json::json!({"value": 1}),
        );
        let mut pending_outputs = VecDeque::new();

        assert!(stage_boundary_control_events(
            vec![control_event],
            &stage_flow_context,
            &instrumentation,
            obzenflow_core::MiddlewareExecutionScope::LiveHandler,
            &mut pending_outputs,
        ));
        assert_eq!(pending_outputs.len(), 1);
        let staged = pending_outputs.pop_front().expect("staged control event");
        assert_eq!(staged.event.flow_context.stage_name, "source");
        assert_eq!(staged.event.flow_context.stage_id, stage_id);

        assert!(!stage_boundary_control_events(
            Vec::new(),
            &stage_flow_context,
            &instrumentation,
            obzenflow_core::MiddlewareExecutionScope::LiveHandler,
            &mut pending_outputs,
        ));
        assert!(pending_outputs.is_empty());
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

        let plan = BackpressurePlan::disabled().with_stage_enforced(
            s,
            NonZeroU64::new(1).expect("window"),
            std::time::Duration::from_secs(30),
        );
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
        pending_outputs.push_back(
            crate::stages::common::supervision::backpressure_drain::PendingOutput {
                event: ChainEventFactory::data_event(
                    WriterId::from(s),
                    "test.event",
                    serde_json::json!({"x": 1}),
                ),
                scope: obzenflow_core::MiddlewareExecutionScope::LiveHandler,
            },
        );

        let mut backpressure_pulse = BackpressureActivityPulse::new();
        let mut backpressure_stall: Option<tokio::time::Instant> = None;

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
                &mut backpressure_stall,
                None,
                None,
                &mut receiver,
                || TestEvent::ChannelClosed,
            )
            .await
        });

        // Give the drain task a moment to enter the credit-wait select.
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

    // Regression guard: the async-source blocked wait must flush its activity
    // pulse to the journal, not just accumulate it in the coalescer. The
    // credit wait races the external-events channel; when the wait wins (no
    // event delivered), the pulse feeds through the shared append/mirror path.
    #[tokio::test(start_paused = true)]
    async fn async_source_blocked_wait_flushes_activity_pulse_to_journal() {
        use obzenflow_core::event::payloads::observability_payload::{
            BackpressureEvent, MiddlewareLifecycle, ObservabilityPayload,
        };

        if BackpressureWriter::is_bypass_enabled() {
            return;
        }

        let mut builder = TopologyBuilder::new();
        let s_top = builder.add_stage(Some("s".to_string()));
        // Downstream stage: gives `s` an outgoing edge to gate; its id is unused.
        builder.add_stage(Some("d".to_string()));
        let topology = builder.build_unchecked().expect("topology");
        let s = StageId::from_topology_id(s_top);

        let plan = BackpressurePlan::disabled().with_stage_enforced(
            s,
            NonZeroU64::new(1).expect("window"),
            std::time::Duration::from_secs(30),
        );
        let registry = BackpressureRegistry::new(&topology, &plan);
        let writer = registry.writer(s);
        writer.reserve(1).expect("reserve").commit(1);
        assert!(writer.reserve(1).is_none(), "credit should be 0");

        let data_journal: Arc<RecordingJournal<ChainEvent>> = Arc::new(RecordingJournal::new());
        let data_journal_dyn: Arc<dyn Journal<ChainEvent>> = data_journal.clone();
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

        let mut backpressure_pulse = BackpressureActivityPulse::new();
        let mut backpressure_stall: Option<tokio::time::Instant> = None;

        // Keep the sender alive and never send: recv() pends, so the biased
        // select always resolves through the credit wait, which auto-advances
        // the paused clock one control cap per call.
        let (_sender, mut receiver, _watcher) = ChannelBuilder::<TestEvent, ()>::new()
            .with_event_buffer(1)
            .build(());

        let mut pending_outputs = VecDeque::new();
        pending_outputs.push_back(
            crate::stages::common::supervision::backpressure_drain::PendingOutput {
                event: ChainEventFactory::data_event(
                    WriterId::from(s),
                    "test.event",
                    serde_json::json!({"x": 1}),
                ),
                scope: obzenflow_core::MiddlewareExecutionScope::LiveHandler,
            },
        );

        // Each call waits one control cap and requeues; after the coalescer's
        // one-second window elapses the pulse is appended.
        for _ in 0..6 {
            let directive = drain_pending_outputs_async(
                &mut pending_outputs,
                &stage_flow_context,
                s,
                None,
                &data_journal_dyn,
                &error_journal,
                &system_journal,
                &instrumentation,
                &writer,
                &mut backpressure_pulse,
                &mut backpressure_stall,
                None,
                None,
                &mut receiver,
                || TestEvent::ChannelClosed,
            )
            .await
            .expect("drain");
            assert!(
                matches!(directive, Some(EventLoopDirective::Continue)),
                "credit is exhausted, so each pass backs off"
            );
        }

        let has_pulse = data_journal.appended().iter().any(|event| {
            matches!(
                &event.content,
                obzenflow_core::event::ChainEventContent::Observability(
                    ObservabilityPayload::Middleware(MiddlewareLifecycle::Backpressure(
                        BackpressureEvent::ActivityPulse { .. }
                    ))
                )
            )
        });
        assert!(
            has_pulse,
            "async-source blocked wait must flush a backpressure activity pulse to the journal"
        );
    }
}
