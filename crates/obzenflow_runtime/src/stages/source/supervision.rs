// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared supervision helpers for source supervisors (finite/infinite, sync/async).
//!
//! Reduces duplication across source supervisors while preserving
//! responsiveness and backpressure semantics.

/// The stage owns command admission. An owned source turn only receives a copy
/// of the first admitted interrupt, used at cancellation-safe input/credit waits.
/// Publications and replay reads never select on this signal: they must settle.
pub(crate) enum SourceControl<E> {
    Mailbox {
        commands: crate::supervised_base::with_external_events::CommandMailbox<E>,
        interrupt: Option<tokio::sync::watch::Sender<Option<E>>>,
    },
    Turn(tokio::sync::watch::Receiver<Option<E>>),
}

impl<E> From<crate::supervised_base::EventReceiver<E>> for SourceControl<E> {
    fn from(receiver: crate::supervised_base::EventReceiver<E>) -> Self {
        Self::Mailbox {
            commands: receiver.into(),
            interrupt: None,
        }
    }
}

impl<E: Clone> SourceControl<E> {
    pub(crate) fn begin_turn(&mut self) -> Self {
        let Self::Mailbox { interrupt, .. } = self else {
            unreachable!("only the owner starts work")
        };
        let (sender, receiver) = tokio::sync::watch::channel(None);
        *interrupt = Some(sender);
        Self::Turn(receiver)
    }

    pub(crate) fn end_turn(&mut self) {
        let Self::Mailbox { interrupt, .. } = self else {
            unreachable!("only the owner settles work")
        };
        *interrupt = None;
    }

    pub(crate) async fn recv(&mut self, admits: impl Fn(&E) -> bool) -> Option<E> {
        match self {
            Self::Mailbox {
                commands,
                interrupt,
            } => {
                let event = commands.recv(admits).await?;
                if let Some(interrupt) = interrupt {
                    interrupt.send_if_modified(|retained| {
                        if retained.is_some() {
                            return false;
                        }
                        *retained = Some(event.clone());
                        true
                    });
                }
                Some(event)
            }
            Self::Turn(interrupt) => interrupt.wait_for(Option::is_some).await.ok()?.clone(),
        }
    }
}

impl<E: crate::supervised_base::with_external_events::ExternalControlEvent + Send + 'static>
    SourceControl<E>
{
    pub(crate) fn close_and_record(
        &mut self,
        publish: crate::supervised_base::with_external_events::CommandRecorder,
        supervisor: &str,
        state: &str,
    ) -> futures::future::BoxFuture<'static, Result<(), Box<dyn std::error::Error + Send + Sync>>>
    {
        let Self::Mailbox { commands, .. } = self else {
            unreachable!("only the owner closes admission")
        };
        commands.close_and_record(publish, supervisor, state)
    }
}

use crate::backpressure::BackpressureWriter;
use crate::feed_plan::StageOutputContract;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::handler_error::StageFatal;
use crate::stages::common::handlers::source::traits::SourceError;
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::supervision::backpressure_drain::{
    drain_one_pending, drain_one_pending_resolve, DrainAttempt, DrainOutcome,
};
use crate::stages::common::supervision::stage_fatal::{record_stage_fatal, StageFatalCommit};
use crate::stages::observer::dispatch::run_source_poll_observers;
use crate::stages::observer::{
    SourcePollObserverContext, SourcePollObserverOutcome, StageObserverBundle,
};
use crate::stages::source::boundary::{
    SourceBoundary, SourceBoundaryOutcome, SourceBoundaryReport, SourcePollExecution,
};
use crate::supervised_base::idle_backoff::IdleBackoff;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::MiddlewareExecutionScope;
use obzenflow_core::event::payloads::execution_payload::{SourcePollErrorKind, SourcePollKind};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::journal::AppendOptions;

use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, EventId, FlowId, StageId, WriterId};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(crate) async fn acquire_replay_input(
    execution: &crate::execution::RuntimeExecution,
    flow_context: &FlowContext,
    stage_type: obzenflow_core::event::context::StageType,
    journal: &Arc<dyn Journal<ChainEvent>>,
) -> Result<crate::replay::ReplayDriver, BoxError> {
    let archive = execution
        .archive_for_io()
        .expect("replaying source has an archive");
    if stage_type == obzenflow_core::event::context::StageType::InfiniteSource {
        if let Some(control) = execution.resume_control() {
            control.register_infinite_source(flow_context.stage_id);
        }
    }
    let stage_key = &flow_context.stage_name;
    let path = archive.source_data_journal_path(stage_key)?;
    let reader = archive.open_source_reader(stage_key, stage_type).await?;
    let replay = crate::replay::ReplayDriver::new(
        reader,
        path,
        crate::replay::ReplayContextTemplate {
            original_flow_id: archive.archive_flow_id().to_string(),
            original_stage_id: archive.archived_stage_id(stage_key)?,
        },
    );
    let event = ChainEventFactory::execution_event(
        flow_context.stage_id.into(),
        obzenflow_core::event::payloads::execution_payload::ExecutionPayload::ReplayLifecycle(
            obzenflow_core::event::ReplayLifecycleEvent::Started {
                archive_path: archive.archive_path().to_path_buf(),
                archive_flow_id: archive.archive_flow_id().to_string(),
                archive_status: archive.archive_status(),
                archive_status_derivation: archive.status_derivation(),
                allow_incomplete: archive.allow_incomplete_archive(),
                source_stages: archive.source_stage_keys(),
            },
        ),
    )
    .with_flow_context(flow_context.clone());
    crate::supervised_base::publication::append(journal, event, Default::default()).await?;
    Ok(replay)
}

pub(crate) fn source_error_kind(error: &SourceError) -> ErrorKind {
    error.kind().processing_error_kind()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Normalise a source-owned poll failure into the routable error event after
/// source policies have observed the typed handler error.
pub(crate) fn normalise_source_poll_error(
    writer_id: WriterId,
    source_type: SourcePollKind,
    error: &SourceError,
) -> ChainEvent {
    use obzenflow_core::event::payloads::execution_payload::{
        ExecutionPayload, SourcePollErrorFact,
    };
    let error_type = error.kind();
    ChainEventFactory::execution_event(
        writer_id,
        ExecutionPayload::SourcePollError(SourcePollErrorFact {
            source_type,
            error_type,
            continuation: error.continuation(),
            diagnostic: error.diagnostic().clone(),
            timestamp_ms: now_ms(),
        }),
    )
    .mark_as_error(error.safe_summary(), error_type.processing_error_kind())
}

/// A failure selected during a turn. Its staged events drain first; the
/// lifecycle failure links to `causal_event_id` only after that commit.
pub(crate) struct PendingFailure {
    pub(crate) message: String,
    pub(crate) causal_event_id: Option<EventId>,
}

impl PendingFailure {
    /// Hands the committed cause to the lifecycle failure and returns its message.
    pub(crate) fn select(self, failure_causal_event_id: &mut Option<EventId>) -> String {
        *failure_causal_event_id = self.causal_event_id;
        self.message
    }
}

/// A terminal report stops polling; its staged diagnostic becomes the cause.
pub(crate) fn terminal_poll_failure(
    stage_name: &str,
    error: &SourceError,
    diagnostic_event: &ChainEvent,
) -> Option<PendingFailure> {
    error.is_terminal().then(|| PendingFailure {
        message: format!("Source '{stage_name}' cannot continue: {error}"),
        causal_event_id: Some(diagnostic_event.id),
    })
}

/// Record-local rejections advanced the reader and terminal reports end it, so
/// neither earns an idle backoff.
pub(crate) fn poll_error_backoff(
    error: &SourceError,
    backoff: &mut IdleBackoff,
) -> Option<Duration> {
    if error.is_terminal() || error.kind() == SourcePollErrorKind::Validation {
        backoff.reset();
        None
    } else {
        Some(backoff.next_delay())
    }
}

/// Where a failed acquisition commits its opening evidence.
pub(crate) struct SourceOpenFailureCommit<'a> {
    pub(crate) stage_flow_context: &'a FlowContext,
    pub(crate) source_type: SourcePollKind,
    pub(crate) resuming: bool,
    pub(crate) error_journal: &'a Arc<dyn Journal<ChainEvent>>,
    pub(crate) instrumentation: &'a Arc<StageInstrumentation>,
    pub(crate) scope: MiddlewareExecutionScope,
}

/// Commit opening evidence before the stage fails, so the lifecycle failure can
/// link to it. An unsuccessful append returns an error and claims no link.
pub(crate) async fn commit_source_open_failure(
    commit: SourceOpenFailureCommit<'_>,
    error: &SourceError,
) -> Result<PendingFailure, BoxError> {
    use obzenflow_core::event::payloads::execution_payload::{
        ExecutionPayload, SourceOpenFailedFact, SourceOpenIntent,
    };
    let error_type = error.kind();
    let event = ChainEventFactory::execution_event(
        WriterId::from(commit.stage_flow_context.stage_id),
        ExecutionPayload::SourceOpenFailed(SourceOpenFailedFact {
            source_type: commit.source_type,
            error_type,
            intent: if commit.resuming {
                SourceOpenIntent::Resume
            } else {
                SourceOpenIntent::Start
            },
            diagnostic: error.diagnostic().clone(),
            timestamp_ms: now_ms(),
        }),
    )
    .with_flow_context(commit.stage_flow_context.clone())
    .mark_as_error(error.safe_summary(), error_type.processing_error_kind());
    commit
        .instrumentation
        .record_error(error_type.processing_error_kind());
    let event = commit.instrumentation.capture_accounting().attach_to(event);
    let recorded = crate::supervised_base::publication::append(
        commit.error_journal,
        event,
        AppendOptions::default().with_capture(
            commit
                .instrumentation
                .journal_capture(Some(commit.scope), vec![(0, false)]),
        ),
    )
    .await?;
    Ok(PendingFailure {
        message: source_open_failure(
            &commit.stage_flow_context.stage_name,
            commit.resuming,
            error,
        ),
        causal_event_id: Some(recorded.envelope.provenance.event.id),
    })
}

/// Record a source adapter/runtime invariant through the common fatal lane.
/// Fatal polls contribute no source output, operational outbox, or policy
/// settlement evidence.
pub(crate) async fn record_source_stage_fatal(
    fatal: &StageFatal,
    stage_id: StageId,
    stage_key: &str,
    error_journal: &Arc<dyn Journal<ChainEvent>>,
) -> Result<(), BoxError> {
    record_stage_fatal(
        fatal,
        StageFatalCommit {
            error_journal,
            writer_id: WriterId::from(stage_id),
            stage_id,
            stage_key,
            input_position: None,
            parent: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
        },
    )
    .await?;
    Ok(())
}

pub(crate) async fn record_source_cleanup_failed(
    context: &FlowContext,
    error: &SourceError,
    journal: &Arc<dyn Journal<ChainEvent>>,
) -> Result<(), BoxError> {
    let event = ChainEventFactory::execution_event(
        context.stage_id.into(),
        obzenflow_core::event::payloads::execution_payload::ExecutionPayload::SourceCleanupFailed {
            stage_id: context.stage_id,
            stage_name: context.stage_name.clone(),
            error: error.safe_summary().to_string(),
        },
    )
    .with_flow_context(context.clone());
    crate::supervised_base::publication::append(journal, event, Default::default()).await?;
    Ok(())
}

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
        if let ProcessingStatus::Error { kind, .. } = &staged_event.processing.status {
            let k = kind.clone().unwrap_or(ErrorKind::Unknown);
            instrumentation.record_error(k);
        }

        if staged_event.consumes_data_credit() {
            instrumentation
                .events_processed_total
                .fetch_add(1, Ordering::Relaxed);
            instrumentation.record_processing_time(per_data_event_duration);
        }
        pending_outputs.push_back(
            crate::stages::common::supervision::backpressure_drain::PendingOutput {
                causal: crate::supervised_base::publication::capture(),
                event: staged_event,
                scope,
            },
        );
    }
}

pub(crate) struct SourcePollObservation<'a> {
    flow_id: FlowId,
    stage_flow_context: &'a FlowContext,
    observers: &'a StageObserverBundle,
    scope: MiddlewareExecutionScope,
}

impl<'a> SourcePollObservation<'a> {
    pub(crate) fn new(
        flow_id: FlowId,
        stage_flow_context: &'a FlowContext,
        observers: &'a StageObserverBundle,
        scope: MiddlewareExecutionScope,
    ) -> Self {
        Self {
            flow_id,
            stage_flow_context,
            observers,
            scope,
        }
    }

    fn is_enabled(&self) -> bool {
        self.observers.has_source_poll() && !self.scope.is_deterministic_replay()
    }

    pub(crate) async fn observe(
        &self,
        outputs: &[ChainEvent],
        _poll_duration: Duration,
        outcome: SourcePollObserverOutcome,
    ) {
        if !self.is_enabled() {
            return;
        }
        let observer_ctx =
            SourcePollObserverContext::new(self.flow_id, self.stage_flow_context, outcome);
        run_source_poll_observers(self.observers, self.scope, &observer_ctx, outputs);
    }

    pub(crate) async fn observe_empty(
        &self,
        poll_duration: Duration,
        outcome: SourcePollObserverOutcome,
    ) {
        self.observe(&[], poll_duration, outcome).await;
    }
}

pub(crate) async fn observe_source_boundary_rejection(
    observation: &SourcePollObservation<'_>,
    control_events: &[ChainEvent],
    policy: Option<&str>,
) {
    if !observation.is_enabled() {
        return;
    }
    let outcome = SourcePollObserverOutcome::Rejected {
        policy: policy.map(str::to_string),
    };
    if control_events.is_empty() {
        observation
            .observe_empty(Duration::from_nanos(0), outcome)
            .await;
    } else {
        observation
            .observe(control_events, Duration::from_nanos(0), outcome)
            .await;
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
    let data_events_in_tick = events
        .iter()
        .filter(|event| event.consumes_data_credit())
        .count();
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
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_stall: &mut Option<tokio::time::Instant>,
    output_contract: Option<&StageOutputContract>,
) -> Result<bool, BoxError> {
    while let Some(pending) = pending_outputs.pop_front() {
        if matches!(
            pending.event.processing.status,
            ProcessingStatus::Error { .. }
        ) {
            let event = instrumentation
                .capture_accounting()
                .attach_to(pending.event);
            crate::supervised_base::publication::append(
                error_journal,
                event,
                AppendOptions::default().with_capture(
                    instrumentation.journal_capture(Some(pending.scope), vec![(0, false)]),
                ),
            )
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
            None,
            instrumentation,
            backpressure_writer,
            backpressure_pulse,
            backpressure_stall,
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
    pending_outputs: &mut VecDeque<
        crate::stages::common::supervision::backpressure_drain::PendingOutput,
    >,
    stage_flow_context: &FlowContext,
    stage_id: StageId,
    heartbeat_state: Option<Arc<HeartbeatState>>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    error_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_stall: &mut Option<tokio::time::Instant>,
    output_contract: Option<&StageOutputContract>,
    receive_control: impl std::future::Future<Output = Option<E>> + Send,
    on_channel_closed: impl FnOnce() -> E,
) -> Result<Option<EventLoopDirective<E>>, BoxError>
where
    E: Send,
{
    let mut on_channel_closed = Some(on_channel_closed);

    while let Some(pending) = pending_outputs.pop_front() {
        if matches!(
            pending.event.processing.status,
            ProcessingStatus::Error { .. }
        ) {
            let event = instrumentation
                .capture_accounting()
                .attach_to(pending.event);
            crate::supervised_base::publication::append(
                error_journal,
                event,
                AppendOptions::default().with_capture(
                    instrumentation.journal_capture(Some(pending.scope), vec![(0, false)]),
                ),
            )
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
            None,
            instrumentation,
            backpressure_writer,
            backpressure_pulse,
            backpressure_stall,
            output_contract,
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
                    maybe_event = receive_control => {
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

/// Attribute acquisition failure from typed diagnostic fields only; connector
/// text can contain credentials, connection strings or untrusted bodies.
pub(crate) fn source_open_failure(stage_name: &str, resuming: bool, error: &SourceError) -> String {
    let reason = error.diagnostic();
    if resuming {
        format!("Cannot resume source '{stage_name}': {reason}. Restore the original input and check its configuration and access before resuming.")
    } else {
        format!("Cannot start source '{stage_name}': {reason}. Check the input, configuration and access before starting again.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure::{BackpressurePlan, BackpressureRegistry};
    use crate::execution::{RuntimeExecution, RuntimeMode};
    use crate::id_conversions::StageIdExt;
    use crate::supervised_base::ChannelBuilder;
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, SourcePollKind};
    use obzenflow_core::event::types::EventId;
    use obzenflow_core::event::{ChainEventFactory, ChainPayload, JournalWriterId, WriterId};
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::{JournalError, JournalReader};
    use obzenflow_core::{ChainEvent, FlowId, Journal, JournalRecord};
    use obzenflow_topology::TopologyBuilder;
    use std::marker::PhantomData;
    use std::num::NonZeroU64;

    struct EmptyReader<T> {
        position: u64,
        _phantom: PhantomData<T>,
    }

    #[async_trait]
    impl<T> obzenflow_core::journal::JournalStorageReader<T> for EmptyReader<T>
    where
        T: obzenflow_core::event::JournalEvent,
    {
        async fn storage_next(
            &mut self,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(None)
        }

        fn storage_position(&self) -> u64 {
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
    impl<T> obzenflow_core::journal::JournalStorage<T> for NoopJournal<T>
    where
        T: obzenflow_core::event::JournalEvent + Clone + 'static,
    {
        fn storage_id(&self) -> &JournalId {
            &self.id
        }

        fn storage_owner(&self) -> Option<&obzenflow_core::JournalOwner> {
            None
        }

        async fn storage_append(
            &self,
            event: T,
            mut options: obzenflow_core::journal::AppendOptions<T>,
        ) -> Result<JournalRecord<T::Payload>, JournalError> {
            let event = options.capture.prepare(0, event);
            Ok(JournalRecord::new(JournalWriterId::new(), event))
        }

        async fn storage_read_all_unordered(
            &self,
        ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            Ok(Vec::new())
        }

        async fn storage_read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(None)
        }

        async fn storage_reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(EmptyReader {
                position,
                _phantom: PhantomData,
            }))
        }

        async fn storage_read_last_n(
            &self,
            _count: usize,
        ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            Ok(Vec::new())
        }
    }

    // Test-local double for the `Journal<T>` port: retains appended events so
    // a test can read them back through the trait. Not an infra adapter, not
    // exported. Consolidating these doubles codebase-wide is FLOWIP-114t.
    struct RecordingJournal<T: obzenflow_core::event::JournalEvent> {
        id: JournalId,
        events: std::sync::Mutex<Vec<JournalRecord<T::Payload>>>,
        _phantom: PhantomData<T>,
    }

    impl<T: obzenflow_core::event::JournalEvent> RecordingJournal<T> {
        fn new() -> Self {
            Self {
                id: JournalId::new(),
                events: std::sync::Mutex::new(Vec::new()),
                _phantom: PhantomData,
            }
        }
    }

    #[async_trait]
    impl<T> obzenflow_core::journal::JournalStorage<T> for RecordingJournal<T>
    where
        T: obzenflow_core::event::JournalEvent + Clone + 'static,
    {
        fn storage_id(&self) -> &JournalId {
            &self.id
        }

        fn storage_owner(&self) -> Option<&obzenflow_core::JournalOwner> {
            None
        }

        async fn storage_append(
            &self,
            event: T,
            mut options: obzenflow_core::journal::AppendOptions<T>,
        ) -> Result<JournalRecord<T::Payload>, JournalError> {
            let event = options.capture.prepare(0, event);
            let envelope = JournalRecord::new(JournalWriterId::new(), event);
            self.events
                .lock()
                .expect("RecordingJournal: poisoned lock")
                .push(envelope.clone());
            Ok(envelope)
        }

        async fn storage_read_all_unordered(
            &self,
        ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            Ok(self
                .events
                .lock()
                .expect("RecordingJournal: poisoned lock")
                .clone())
        }

        async fn storage_read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(None)
        }

        async fn storage_reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(EmptyReader {
                position,
                _phantom: PhantomData,
            }))
        }

        async fn storage_read_last_n(
            &self,
            _count: usize,
        ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            Ok(Vec::new())
        }
    }

    #[derive(Debug)]
    enum TestEvent {
        BeginDrain,
        ChannelClosed,
    }

    #[test]
    fn source_poll_errors_normalise_to_typed_error_marked_rows() {
        use obzenflow_core::event::payloads::execution_payload::SourcePollContinuation;
        use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason};

        let writer_id = WriterId::from(StageId::new());
        let diagnostic = SourceDiagnostic::new(SourceDiagnosticReason::InvalidValue)
            .within_field("amount_cents")
            .record(4);
        let cases = [
            (
                SourceError::Timeout(diagnostic.clone()),
                ErrorKind::Timeout,
                "source timeout",
                SourcePollContinuation::Recoverable,
            ),
            (
                SourceError::Transport(diagnostic.clone()),
                ErrorKind::Remote,
                "source transport error",
                SourcePollContinuation::Recoverable,
            ),
            (
                SourceError::Deserialization(diagnostic.clone()),
                ErrorKind::Deserialization,
                "source deserialization error",
                SourcePollContinuation::Recoverable,
            ),
            (
                SourceError::Validation(diagnostic.clone()),
                ErrorKind::Validation,
                "source validation error",
                SourcePollContinuation::Recoverable,
            ),
            (
                SourceError::Other(diagnostic.clone()),
                ErrorKind::Unknown,
                "source error",
                SourcePollContinuation::Recoverable,
            ),
            (
                SourceError::Terminal {
                    kind: SourcePollErrorKind::Validation,
                    diagnostic: diagnostic.clone(),
                },
                ErrorKind::Validation,
                "source validation error",
                SourcePollContinuation::Terminal,
            ),
        ];

        for (error, expected_kind, expected_message, expected_continuation) in cases {
            let event = normalise_source_poll_error(writer_id, SourcePollKind::AsyncFinite, &error);
            assert!(matches!(
                event.processing.status,
                ProcessingStatus::Error {
                    ref message,
                    kind: Some(ref kind),
                } if *kind == expected_kind && message == expected_message
            ));
            match event.payload {
                ChainPayload::Execution(ExecutionPayload::SourcePollError(failure)) => {
                    assert_eq!(failure.source_type, SourcePollKind::AsyncFinite);
                    assert_eq!(failure.error_type.processing_error_kind(), expected_kind);
                    assert_eq!(failure.continuation, expected_continuation);
                    assert_eq!(failure.diagnostic, diagnostic);
                }
                other => panic!("expected source.poll_error lifecycle row, got {other:?}"),
            }
        }
    }

    #[test]
    fn only_terminal_reports_select_a_failure_and_rejections_skip_backoff() {
        use obzenflow_core::event::SourceDiagnosticReason;

        let rejected = SourceError::Validation(SourceDiagnosticReason::InvalidRecord.into());
        let transport = SourceError::Transport(SourceDiagnosticReason::InputUnavailable.into());
        let terminal = SourceError::Terminal {
            kind: SourcePollErrorKind::Transport,
            diagnostic: SourceDiagnosticReason::InputUnavailable.into(),
        };
        let writer_id = WriterId::from(StageId::new());
        let mut backoff =
            IdleBackoff::exponential_with_cap(Duration::from_millis(1), Duration::from_millis(10));

        let event = normalise_source_poll_error(writer_id, SourcePollKind::Finite, &rejected);
        assert!(terminal_poll_failure("orders", &rejected, &event).is_none());
        assert_eq!(poll_error_backoff(&rejected, &mut backoff), None);
        assert!(poll_error_backoff(&transport, &mut backoff).is_some());

        let event = normalise_source_poll_error(writer_id, SourcePollKind::Finite, &terminal);
        let failure = terminal_poll_failure("orders", &terminal, &event).expect("terminal");
        assert_eq!(failure.causal_event_id, Some(event.id));
        assert_eq!(
            failure.message,
            "Source 'orders' cannot continue: terminal source transport error: the input is unavailable"
        );
        assert_eq!(poll_error_backoff(&terminal, &mut backoff), None);
    }

    #[test]
    fn open_failure_messages_render_the_typed_reason() {
        use obzenflow_core::event::SourceDiagnosticReason;

        let error = SourceError::Validation(SourceDiagnosticReason::SizeLimitExceeded.into());
        assert_eq!(
            source_open_failure("web_orders", false, &error),
            "Cannot start source 'web_orders': the input exceeds its configured size limit. Check the input, configuration and access before starting again."
        );
        assert!(source_open_failure("web_orders", true, &error)
            .starts_with("Cannot resume source 'web_orders': the input exceeds"));
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
            std::num::NonZeroU32::MIN,
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
                causal: crate::supervised_base::publication::capture(),
                event: ChainEventFactory::data_event(
                    WriterId::from(s),
                    "test.event",
                    std::num::NonZeroU32::MIN,
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
                &instrumentation,
                &writer,
                &mut backpressure_pulse,
                &mut backpressure_stall,
                None,
                receiver.recv(),
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
    async fn async_source_blocked_wait_offers_activity_pulse_without_journal_rows() {
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

        let data_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(RecordingJournal::new());
        let error_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(NoopJournal::new());
        let instrumentation = Arc::new(StageInstrumentation::new());
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        instrumentation.bind_observations(FlowId::new(), s.into(), &execution);
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
                causal: crate::supervised_base::publication::capture(),
                event: ChainEventFactory::data_event(
                    WriterId::from(s),
                    "test.event",
                    std::num::NonZeroU32::MIN,
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
                &data_journal,
                &error_journal,
                &instrumentation,
                &writer,
                &mut backpressure_pulse,
                &mut backpressure_stall,
                None,
                receiver.recv(),
                || TestEvent::ChannelClosed,
            )
            .await
            .expect("drain");
            assert!(
                matches!(directive, Some(EventLoopDirective::Continue)),
                "credit is exhausted, so each pass backs off"
            );
        }

        let appended = data_journal
            .read_all_unordered()
            .await
            .expect("read data journal");
        use obzenflow_core::event::observability::{ObservationRecord, ObservationSource};
        assert!(
            appended.is_empty(),
            "waiting must not append measurement rows"
        );
        assert!(
            execution
                .observations()
                .snapshot()
                .iter()
                .flat_map(|packet| &packet.records)
                .any(|record| matches!(record, ObservationRecord::BackpressureActivity { .. })),
            "blocked wait offers a live backpressure sample"
        );
    }
}
