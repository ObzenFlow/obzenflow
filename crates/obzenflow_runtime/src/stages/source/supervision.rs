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
use crate::stages::common::control_strategies::{
    AdmissionDecision, AdmissionGate, AdmissionPosition, AttemptObserver, AttemptOutcome,
    PostAdmitDecision,
};
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::supervision::backpressure_drain::{
    drain_one_pending, drain_one_pending_resolve, DrainAttempt, DrainOutcome,
};
use crate::stages::common::supervision::suspension::{suspend_until, AdmittedAttempt};
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

pub(crate) fn batch_attempt_outcome(events: &[ChainEvent]) -> AttemptOutcome {
    if events
        .iter()
        .any(|event| matches!(event.processing_info.status, ProcessingStatus::Error { .. }))
    {
        AttemptOutcome::Failed {
            cause: "source poll produced an error-marked event".to_string(),
        }
    } else {
        AttemptOutcome::Succeeded
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
    output_contract: Option<&StageOutputContract>,
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
            output_contract,
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
    output_contract: Option<&StageOutputContract>,
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
            output_contract,
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

/// Outcome of consulting the source `PrePoll` admission gate around one poll
/// (FLOWIP-115a).
pub(crate) enum PrePollDecision<E> {
    /// The gate allows the poll, or there is no gate.
    Proceed(AdmittedAttempt),
    /// An external control event (drain, EOF, cancellation, shutdown) won the
    /// race against a gate pause; the supervisor transitions on it.
    Interrupted(EventLoopDirective<E>),
    /// A circuit breaker rejected the attempt terminally; the supervisor routes
    /// this to the completion path so the `CompletionGate` emits a poison EOF.
    RejectToCompletion { reason: String },
}

/// Outcome of consulting the source `PostAdmit` admission gate for a held finite
/// batch (the FLOWIP-114m charge point).
pub(crate) enum HeldDecision<E> {
    /// The held batch may be staged.
    Proceed,
    /// An external control event won the race; the caller aborts the held batch
    /// and transitions on the event.
    Interrupted(EventLoopDirective<E>),
}

/// Consult the source `PrePoll` admission gate, with the supervisor owning any
/// pause wait (FLOWIP-115a, FLOWIP-115c).
///
/// When `external_events` is `Some` (async sources) a gate `Pause` is raced in a
/// `biased` select against the control channel, so drain, EOF, cancellation, and
/// shutdown interrupt the wait (FLOWIP-114o). When `None` (sync sources) the wait
/// is documented-blocking. Source policies pause with `WakeOn::At`, which is
/// self-bounded, so no stall cap is passed.
pub(crate) async fn admit_prepoll<E>(
    gates: &[Arc<dyn AdmissionGate>],
    observers: Vec<Arc<dyn AttemptObserver>>,
    external_events: Option<&mut EventReceiver<E>>,
    on_channel_closed: impl FnOnce() -> E,
) -> Result<PrePollDecision<E>, BoxError> {
    let attempt = AdmittedAttempt::new(AdmissionPosition::PrePoll, observers);
    let mut external_events = external_events;
    let mut on_channel_closed = Some(on_channel_closed);

    for gate in gates {
        loop {
            match gate.admit(AdmissionPosition::PrePoll) {
                AdmissionDecision::Continue => break,
                AdmissionDecision::RejectAttempt { reason } => {
                    attempt.settle(AttemptOutcome::Skipped).await?;
                    return Ok(PrePollDecision::RejectToCompletion { reason });
                }
                // Source policies do not synthesize a fallback in 115a; proceed.
                AdmissionDecision::SynthesizeFallback { .. } => break,
                AdmissionDecision::Pause { wake } => match external_events.as_deref_mut() {
                    Some(receiver) => {
                        tokio::select! {
                            biased;
                            maybe_event = receiver.recv() => {
                                let event = match maybe_event {
                                    Some(event) => event,
                                    None => (on_channel_closed
                                        .take()
                                        .expect("on_channel_closed available"))(),
                                };
                                attempt
                                    .settle(AttemptOutcome::Aborted {
                                        reason: "source pre-poll admission interrupted".to_string(),
                                    })
                                    .await?;
                                return Ok(PrePollDecision::Interrupted(
                                    EventLoopDirective::Transition(event),
                                ));
                            }
                            _ = suspend_until(&wake, None) => {}
                        }
                    }
                    None => {
                        suspend_until(&wake, None).await;
                    }
                },
            }
        }
    }

    Ok(PrePollDecision::Proceed(attempt))
}

/// Consult the source `PostAdmit` admission gate for a held finite batch
/// (FLOWIP-115a, FLOWIP-114m).
///
/// Same interruption contract as [`admit_prepoll`]. On `Interrupted` the caller
/// must abort the held batch, settling its attempt as aborted and dropping the
/// batch without staging, so the batch is never delivered, charged, or counted.
pub(crate) async fn admit_held<E>(
    gates: &[Arc<dyn AdmissionGate>],
    external_events: Option<&mut EventReceiver<E>>,
    on_channel_closed: impl FnOnce() -> E,
) -> HeldDecision<E> {
    let mut external_events = external_events;
    let mut on_channel_closed = Some(on_channel_closed);

    for gate in gates {
        loop {
            match gate.admit_held() {
                PostAdmitDecision::Continue => break,
                PostAdmitDecision::Pause { wake } => match external_events.as_deref_mut() {
                    Some(receiver) => {
                        tokio::select! {
                            biased;
                            maybe_event = receiver.recv() => {
                                let event = match maybe_event {
                                    Some(event) => event,
                                    None => (on_channel_closed
                                        .take()
                                        .expect("on_channel_closed available"))(),
                                };
                                return HeldDecision::Interrupted(
                                    EventLoopDirective::Transition(event),
                                );
                            }
                            _ = suspend_until(&wake, None) => {}
                        }
                    }
                    None => {
                        suspend_until(&wake, None).await;
                    }
                },
            }
        }
    }

    HeldDecision::Proceed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure::{BackpressurePlan, BackpressureRegistry};
    use crate::id_conversions::StageIdExt;
    use crate::stages::common::control_strategies::WakeOn;
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct SequenceGate {
        pre_poll: Vec<AdmissionDecision>,
        held: Vec<PostAdmitDecision>,
        pre_poll_calls: AtomicUsize,
        held_calls: AtomicUsize,
    }

    impl SequenceGate {
        fn pre_poll(pre_poll: Vec<AdmissionDecision>) -> Self {
            Self {
                pre_poll,
                held: Vec::new(),
                pre_poll_calls: AtomicUsize::new(0),
                held_calls: AtomicUsize::new(0),
            }
        }

        fn held(held: Vec<PostAdmitDecision>) -> Self {
            Self {
                pre_poll: Vec::new(),
                held,
                pre_poll_calls: AtomicUsize::new(0),
                held_calls: AtomicUsize::new(0),
            }
        }

        fn pre_poll_call_count(&self) -> usize {
            self.pre_poll_calls.load(Ordering::SeqCst)
        }

        fn held_call_count(&self) -> usize {
            self.held_calls.load(Ordering::SeqCst)
        }
    }

    impl AdmissionGate for SequenceGate {
        fn admit(&self, _position: AdmissionPosition) -> AdmissionDecision {
            let index = self.pre_poll_calls.fetch_add(1, Ordering::SeqCst);
            self.pre_poll
                .get(index)
                .cloned()
                .unwrap_or(AdmissionDecision::Continue)
        }

        fn admit_held(&self) -> PostAdmitDecision {
            let index = self.held_calls.fetch_add(1, Ordering::SeqCst);
            self.held
                .get(index)
                .cloned()
                .unwrap_or(PostAdmitDecision::Continue)
        }
    }

    #[derive(Default)]
    struct RecordingObserver {
        settle_count: AtomicUsize,
        skipped_count: AtomicUsize,
        aborted_count: AtomicUsize,
    }

    #[async_trait]
    impl AttemptObserver for RecordingObserver {
        async fn settle(
            &self,
            _position: AdmissionPosition,
            outcome: &AttemptOutcome,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.settle_count.fetch_add(1, Ordering::SeqCst);
            match outcome {
                AttemptOutcome::Skipped => {
                    self.skipped_count.fetch_add(1, Ordering::SeqCst);
                }
                AttemptOutcome::Aborted { .. } => {
                    self.aborted_count.fetch_add(1, Ordering::SeqCst);
                }
                AttemptOutcome::Succeeded | AttemptOutcome::Failed { .. } => {}
            }
            Ok(())
        }
    }

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
    async fn admit_prepoll_drives_gates_in_order_and_returns_attempt() {
        let first = Arc::new(SequenceGate::pre_poll(vec![AdmissionDecision::Continue]));
        let second = Arc::new(SequenceGate::pre_poll(vec![
            AdmissionDecision::Pause {
                wake: WakeOn::Immediate,
            },
            AdmissionDecision::Continue,
        ]));
        let gates: Vec<Arc<dyn AdmissionGate>> = vec![first.clone(), second.clone()];

        let decision = admit_prepoll(&gates, Vec::new(), None, || TestEvent::ChannelClosed)
            .await
            .expect("admission");

        match decision {
            PrePollDecision::Proceed(attempt) => {
                attempt
                    .settle(AttemptOutcome::Succeeded)
                    .await
                    .expect("settle");
            }
            PrePollDecision::Interrupted(_) | PrePollDecision::RejectToCompletion { .. } => {
                panic!("expected pre-poll admission to proceed")
            }
        }
        assert_eq!(first.pre_poll_call_count(), 1);
        assert_eq!(second.pre_poll_call_count(), 2);
    }

    #[tokio::test]
    async fn admit_prepoll_reject_settles_attempt_as_skipped() {
        let gate = Arc::new(SequenceGate::pre_poll(vec![
            AdmissionDecision::RejectAttempt {
                reason: "open".to_string(),
            },
        ]));
        let gates: Vec<Arc<dyn AdmissionGate>> = vec![gate.clone()];
        let observer = Arc::new(RecordingObserver::default());

        let decision = admit_prepoll(&gates, vec![observer.clone()], None, || {
            TestEvent::ChannelClosed
        })
        .await
        .expect("admission");

        match decision {
            PrePollDecision::RejectToCompletion { reason } => assert_eq!(reason, "open"),
            PrePollDecision::Proceed(_) | PrePollDecision::Interrupted(_) => {
                panic!("expected reject-to-completion")
            }
        }
        assert_eq!(observer.settle_count.load(Ordering::SeqCst), 1);
        assert_eq!(observer.skipped_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn admit_prepoll_external_event_interrupts_pause_and_settles_aborted() {
        let gate = Arc::new(SequenceGate::pre_poll(vec![AdmissionDecision::Pause {
            wake: WakeOn::At(std::time::Instant::now() + Duration::from_secs(30)),
        }]));
        let gates: Vec<Arc<dyn AdmissionGate>> = vec![gate.clone()];
        let observer = Arc::new(RecordingObserver::default());
        let (sender, mut receiver, _watcher) = ChannelBuilder::<TestEvent, ()>::new()
            .with_event_buffer(1)
            .build(());
        sender.send(TestEvent::BeginDrain).await.expect("send");

        let decision = admit_prepoll(&gates, vec![observer.clone()], Some(&mut receiver), || {
            TestEvent::ChannelClosed
        })
        .await
        .expect("admission");

        match decision {
            PrePollDecision::Interrupted(EventLoopDirective::Transition(TestEvent::BeginDrain)) => {
            }
            PrePollDecision::Proceed(_)
            | PrePollDecision::RejectToCompletion { .. }
            | PrePollDecision::Interrupted(_) => {
                panic!("expected begin-drain interruption")
            }
        }
        assert_eq!(observer.settle_count.load(Ordering::SeqCst), 1);
        assert_eq!(observer.aborted_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn admit_held_external_event_interrupts_pause() {
        let gate = Arc::new(SequenceGate::held(vec![PostAdmitDecision::Pause {
            wake: WakeOn::At(std::time::Instant::now() + Duration::from_secs(30)),
        }]));
        let gates: Vec<Arc<dyn AdmissionGate>> = vec![gate.clone()];
        let (sender, mut receiver, _watcher) = ChannelBuilder::<TestEvent, ()>::new()
            .with_event_buffer(1)
            .build(());
        sender.send(TestEvent::BeginDrain).await.expect("send");

        let decision = admit_held(&gates, Some(&mut receiver), || TestEvent::ChannelClosed).await;

        match decision {
            HeldDecision::Interrupted(EventLoopDirective::Transition(TestEvent::BeginDrain)) => {}
            HeldDecision::Proceed | HeldDecision::Interrupted(_) => {
                panic!("expected begin-drain interruption")
            }
        }
        assert_eq!(gate.held_call_count(), 1);
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
                None,
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
