// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115g source-poll causal-order and idle-scheduling proof.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow::effects::Port;
use obzenflow::stages::sources::ValuesSource;
use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, SourceAdmission, SourceAfterPoll, SourceBatchFacts, SourcePolicy,
    SourcePolicyCtx, SourcePollAttachment, SourcePollOutcome,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory, ChainPayload};
use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::journal::Journal;
use obzenflow_core::{JournalRecord, TypedPayload, WriterId};
use obzenflow_dsl::{sink, source, test_flow};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{EffectContext, EffectError};
use obzenflow_runtime::stages::common::handlers::source::{ContinuousRead, FiniteRead, SourceRead};
use obzenflow_runtime::stages::common::handlers::{
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::observer::{
    SourcePollObserver, SourcePollObserverContext, SourcePollObserverOutcome,
};
use serde::{Deserialize, Serialize};
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

type PollTimes = Arc<Mutex<Vec<tokio::time::Instant>>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PollRead(usize);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PollTake;

#[obzenflow_runtime::effects::port]
#[async_trait]
trait PollInput {
    #[effect(type = "polling_contract.input.read", exactly_once, records = reply)]
    async fn read(
        &self,
        from: &PollRead,
        ctx: &EffectContext,
    ) -> Result<FiniteRead<PollingEvent, PollRead>, EffectError>;

    #[effect(type = "polling_contract.input.take", exactly_once, records = reply)]
    async fn take(
        &self,
        from: &PollTake,
        ctx: &EffectContext,
    ) -> Result<ContinuousRead<PollingEvent, PollTake>, EffectError>;
}

/// What a scripted input does on each physical read or take.
enum Script {
    /// One event after `raw_delay`, then the end of input.
    Once {
        calls: Arc<AtomicUsize>,
        raw_delay: Duration,
        ordinal: usize,
    },
    /// The first read never answers; the next one ends the input.
    PendingThenEof { calls: Arc<AtomicUsize> },
    /// One step per read, each read's time recorded.
    Steps {
        steps: Vec<ScriptStep>,
        poll_times: PollTimes,
    },
    /// The input ends at the first read.
    Eof { calls: Arc<AtomicUsize> },
    /// One live event after `raw_delay`, then empty takes, or takes that never
    /// answer when `then_pending` is set.
    LiveOnce {
        calls: Arc<AtomicUsize>,
        raw_delay: Duration,
        then_pending: bool,
    },
    /// Empty live takes. The first take notifies `first_poll`.
    Idle {
        calls: Arc<AtomicUsize>,
        first_poll: Arc<Notify>,
    },
}

struct ScriptedInput(Script);

#[obzenflow_runtime::effects::system("scripted-input")]
#[async_trait]
impl PollInput for ScriptedInput {
    async fn read(
        &self,
        from: &PollRead,
        _ctx: &EffectContext,
    ) -> Result<FiniteRead<PollingEvent, PollRead>, EffectError> {
        let next = PollRead(from.0 + 1);
        match &self.0 {
            Script::Once {
                calls,
                raw_delay,
                ordinal,
            } => {
                if calls.fetch_add(1, Ordering::SeqCst) > 0 {
                    return Ok(FiniteRead::EndOfInput);
                }
                tokio::time::sleep(*raw_delay).await;
                Ok(FiniteRead::Batch {
                    records: vec![PollingEvent { ordinal: *ordinal }],
                    next,
                })
            }
            Script::PendingThenEof { calls } => {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    pending::<()>().await;
                }
                Ok(FiniteRead::EndOfInput)
            }
            Script::Steps { steps, poll_times } => {
                poll_times
                    .lock()
                    .expect("poll-times lock poisoned")
                    .push(tokio::time::Instant::now());
                match steps[from.0] {
                    ScriptStep::NonData => Ok(FiniteRead::Batch {
                        records: Vec::new(),
                        next,
                    }),
                    ScriptStep::Data => Ok(FiniteRead::Batch {
                        records: vec![PollingEvent { ordinal: from.0 }],
                        next,
                    }),
                    ScriptStep::Eof => Ok(FiniteRead::EndOfInput),
                }
            }
            Script::Eof { calls } => {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(FiniteRead::EndOfInput)
            }
            Script::LiveOnce { .. } | Script::Idle { .. } => Err(EffectError::Validation(
                "a live script answers takes".into(),
            )),
        }
    }

    async fn take(
        &self,
        _from: &PollTake,
        _ctx: &EffectContext,
    ) -> Result<ContinuousRead<PollingEvent, PollTake>, EffectError> {
        let records = match &self.0 {
            Script::LiveOnce {
                calls,
                raw_delay,
                then_pending,
            } => {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    tokio::time::sleep(*raw_delay).await;
                    vec![PollingEvent { ordinal: 0 }]
                } else if *then_pending {
                    pending::<Vec<PollingEvent>>().await
                } else {
                    Vec::new()
                }
            }
            Script::Idle { calls, first_poll } => {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    first_poll.notify_one();
                }
                Vec::new()
            }
            _ => {
                return Err(EffectError::Validation(
                    "a finite script answers reads".into(),
                ))
            }
        };
        Ok(ContinuousRead {
            records,
            next: PollTake,
        })
    }
}

fn scripted(script: Script) -> Port<dyn PollInput> {
    Port::to(ScriptedInput(script))
}

fn finite(port: &Port<dyn PollInput>) -> SourceRead<PollingEvent> {
    SourceRead::finite(port.clone(), PollRead(0), Ok)
}

fn live(port: &Port<dyn PollInput>) -> SourceRead<PollingEvent> {
    SourceRead::continuous(
        port.clone(),
        PollTake,
        || Box::pin(async { Ok(()) }),
        ReattachGuarantee::AtMostOnce,
        Ok,
    )
}

struct AsyncFiniteOnce(Port<dyn PollInput>);

impl AsyncFiniteOnce {
    fn new(raw_delay: Duration) -> Self {
        Self(scripted(Script::Once {
            calls: Arc::default(),
            raw_delay,
            ordinal: 0,
        }))
    }
}

impl TypedAsyncFiniteSourceHandler for AsyncFiniteOnce {
    type Output = PollingEvent;

    fn read(&self) -> Option<SourceRead<PollingEvent>> {
        Some(finite(&self.0))
    }
}

struct SyncInfiniteOnce(Port<dyn PollInput>);

impl SyncInfiniteOnce {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self(scripted(Script::LiveOnce {
            calls,
            raw_delay: Duration::ZERO,
            then_pending: false,
        }))
    }
}

impl TypedInfiniteSourceHandler for SyncInfiniteOnce {
    type Output = PollingEvent;

    fn read(&self) -> Option<SourceRead<PollingEvent>> {
        Some(live(&self.0))
    }
}

struct AsyncInfiniteOnce(Port<dyn PollInput>);

impl AsyncInfiniteOnce {
    fn new(calls: Arc<AtomicUsize>, raw_delay: Duration) -> Self {
        Self(scripted(Script::LiveOnce {
            calls,
            raw_delay,
            then_pending: true,
        }))
    }
}

impl TypedAsyncInfiniteSourceHandler for AsyncInfiniteOnce {
    type Output = PollingEvent;

    fn read(&self) -> Option<SourceRead<PollingEvent>> {
        Some(live(&self.0))
    }
}

struct TimeoutThenEof {
    port: Port<dyn PollInput>,
    timeout: Duration,
}

impl TimeoutThenEof {
    fn new(calls: Arc<AtomicUsize>, timeout: Duration) -> Self {
        Self {
            port: scripted(Script::PendingThenEof { calls }),
            timeout,
        }
    }
}

impl TypedAsyncFiniteSourceHandler for TimeoutThenEof {
    type Output = PollingEvent;

    fn poll_timeout(&self) -> Option<Duration> {
        Some(self.timeout)
    }

    fn read(&self) -> Option<SourceRead<PollingEvent>> {
        Some(finite(&self.port))
    }
}

struct DisabledTimeoutThenEof(Port<dyn PollInput>);

impl DisabledTimeoutThenEof {
    fn new(calls: Arc<AtomicUsize>, raw_delay: Duration) -> Self {
        Self(scripted(Script::Once {
            calls,
            raw_delay,
            ordinal: 1,
        }))
    }
}

impl TypedAsyncFiniteSourceHandler for DisabledTimeoutThenEof {
    type Output = PollingEvent;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    fn read(&self) -> Option<SourceRead<PollingEvent>> {
        Some(finite(&self.0))
    }
}

struct SyncScriptedSource(Port<dyn PollInput>);

impl SyncScriptedSource {
    fn new(steps: Vec<ScriptStep>, poll_times: PollTimes) -> Self {
        Self(scripted(Script::Steps { steps, poll_times }))
    }
}

impl TypedFiniteSourceHandler for SyncScriptedSource {
    type Output = PollingEvent;

    fn read(&self) -> Option<SourceRead<PollingEvent>> {
        Some(finite(&self.0))
    }
}

struct AsyncScriptedSource(Port<dyn PollInput>);

impl AsyncScriptedSource {
    fn new(steps: Vec<ScriptStep>, poll_times: PollTimes) -> Self {
        Self(scripted(Script::Steps { steps, poll_times }))
    }
}

impl TypedAsyncFiniteSourceHandler for AsyncScriptedSource {
    type Output = PollingEvent;

    fn read(&self) -> Option<SourceRead<PollingEvent>> {
        Some(finite(&self.0))
    }
}

struct AsyncIdleInfiniteSource(Port<dyn PollInput>);

impl AsyncIdleInfiniteSource {
    fn new(calls: Arc<AtomicUsize>, first_poll: Arc<Notify>) -> Self {
        Self(scripted(Script::Idle { calls, first_poll }))
    }
}

impl TypedAsyncInfiniteSourceHandler for AsyncIdleInfiniteSource {
    type Output = PollingEvent;

    fn read(&self) -> Option<SourceRead<PollingEvent>> {
        Some(live(&self.0))
    }
}

struct ImmediateEof(Port<dyn PollInput>);

impl ImmediateEof {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self(scripted(Script::Eof { calls }))
    }
}

impl TypedFiniteSourceHandler for ImmediateEof {
    type Output = PollingEvent;

    fn read(&self) -> Option<SourceRead<PollingEvent>> {
        Some(finite(&self.0))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PollingEvent {
    ordinal: usize,
}

impl TypedPayload for PollingEvent {
    const EVENT_TYPE: &'static str = "flowip_115g.source_polling";
}

type NoopSink = obzenflow::stages::sinks::DiscardSink<PollingEvent>;

// A closed factual control report exercises the source outbox without a
// retired generic metrics payload.
fn policy_report(writer_id: WriterId, reason: impl Into<String>) -> ChainEvent {
    ChainEventFactory::execution_event(
        writer_id,
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Draining {
            accounting: None,
            stage_id: *writer_id.as_stage().expect("stage writer"),
            reason: Some(reason.into()),
        }),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PolicyOutcomeKind {
    Delivered {
        event_count: usize,
        has_error_marked: bool,
    },
    Empty,
    Eof,
    Failed,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PolicyObservation {
    outcome: PolicyOutcomeKind,
    poll_duration: Option<Duration>,
}

#[derive(Default)]
struct PolicyLog {
    observations: Mutex<Vec<PolicyObservation>>,
    changed: Notify,
}

impl PolicyLog {
    fn push(&self, observation: PolicyObservation) {
        self.observations
            .lock()
            .expect("policy log lock poisoned")
            .push(observation);
        self.changed.notify_waiters();
    }

    fn snapshot(&self) -> Vec<PolicyObservation> {
        self.observations
            .lock()
            .expect("policy log lock poisoned")
            .clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObserverObservation {
    outcome: SourcePollObserverOutcome,
}

#[derive(Default)]
struct ObserverLog {
    observations: Mutex<Vec<ObserverObservation>>,
    changed: Notify,
}

impl ObserverLog {
    fn push(&self, observation: ObserverObservation) {
        self.observations
            .lock()
            .expect("observer log lock poisoned")
            .push(observation);
        self.changed.notify_waiters();
    }

    fn snapshot(&self) -> Vec<ObserverObservation> {
        self.observations
            .lock()
            .expect("observer log lock poisoned")
            .clone()
    }

    async fn wait_for_len(&self, expected: usize) {
        loop {
            let notified = self.changed.notified();
            if self
                .observations
                .lock()
                .expect("observer log lock poisoned")
                .len()
                >= expected
            {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone)]
struct PolicySettings {
    after_poll_delay: Duration,
    emit_after_poll: bool,
    emit_on_observe: bool,
    reject: bool,
}

impl Default for PolicySettings {
    fn default() -> Self {
        Self {
            after_poll_delay: Duration::ZERO,
            emit_after_poll: false,
            emit_on_observe: false,
            reject: false,
        }
    }
}

struct SourceContractPolicy {
    settings: PolicySettings,
    outbox_ordinal: Arc<AtomicUsize>,
    log: Arc<PolicyLog>,
}

#[async_trait]
impl SourcePolicy for SourceContractPolicy {
    fn label(&self) -> &'static str {
        "source_contract_policy"
    }

    async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        if self.settings.reject {
            ctx.write_control_event(policy_report(ctx.writer_id(), "policy.rejection_outbox"));
            return SourceAdmission::Reject {
                reason: "source contract rejection".to_string(),
            };
        }
        SourceAdmission::Admit(None)
    }

    async fn after_poll(
        &self,
        _batch: SourceBatchFacts,
        ctx: &mut SourcePolicyCtx,
    ) -> SourceAfterPoll {
        if !self.settings.after_poll_delay.is_zero() {
            tokio::time::sleep(self.settings.after_poll_delay).await;
        }
        if self.settings.emit_after_poll {
            let ordinal = self.outbox_ordinal.fetch_add(1, Ordering::SeqCst);
            ctx.write_control_event(policy_report(
                ctx.writer_id(),
                format!("policy.outbox.{ordinal}"),
            ));
        }
        SourceAfterPoll::Proceed
    }

    fn observe(&self, outcome: &SourcePollOutcome<'_>, ctx: &mut SourcePolicyCtx) {
        let observation = match outcome {
            SourcePollOutcome::Delivered {
                batch,
                poll_duration,
            } => PolicyObservation {
                outcome: PolicyOutcomeKind::Delivered {
                    event_count: batch.event_count,
                    has_error_marked: batch.has_error_marked,
                },
                poll_duration: Some(*poll_duration),
            },
            SourcePollOutcome::Empty { poll_duration } => PolicyObservation {
                outcome: PolicyOutcomeKind::Empty,
                poll_duration: Some(*poll_duration),
            },
            SourcePollOutcome::Eof { poll_duration } => PolicyObservation {
                outcome: PolicyOutcomeKind::Eof,
                poll_duration: Some(*poll_duration),
            },
            SourcePollOutcome::Failed { poll_duration, .. } => PolicyObservation {
                outcome: PolicyOutcomeKind::Failed,
                poll_duration: Some(*poll_duration),
            },
            SourcePollOutcome::RejectedBy { .. } => PolicyObservation {
                outcome: PolicyOutcomeKind::Rejected,
                poll_duration: None,
            },
        };
        self.log.push(observation);
        if self.settings.emit_on_observe {
            ctx.write_control_event(policy_report(ctx.writer_id(), "policy.observe_outbox"));
        }
    }
}

struct SourceContractPolicyFamily;

struct SourceContractPolicyFactory {
    settings: PolicySettings,
    outbox_ordinal: Arc<AtomicUsize>,
    log: Arc<PolicyLog>,
}

impl SourceContractPolicyFactory {
    fn new(settings: PolicySettings, log: Arc<PolicyLog>) -> Self {
        Self {
            settings,
            outbox_ordinal: Arc::new(AtomicUsize::new(0)),
            log,
        }
    }
}

impl MiddlewareFactory for SourceContractPolicyFactory {
    fn label(&self) -> &'static str {
        "source_contract_policy"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<SourceContractPolicyFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::SourcePoll])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        Ok(MiddlewareSurfaceAttachment::source_poll(
            SourcePollAttachment {
                policy: Arc::new(SourceContractPolicy {
                    settings: self.settings.clone(),
                    outbox_ordinal: self.outbox_ordinal.clone(),
                    log: self.log.clone(),
                }),
                completion_gate: None,
            },
        ))
    }
}

struct SourceContractObserver {
    log: Arc<ObserverLog>,
}

impl SourcePollObserver for SourceContractObserver {
    fn after_source_poll(&self, ctx: &SourcePollObserverContext<'_>, _outputs: &[ChainEvent]) {
        self.log.push(ObserverObservation {
            outcome: ctx.outcome().clone(),
        });
    }
}

struct SourceContractObserverFamily;

struct SourceContractObserverFactory {
    log: Arc<ObserverLog>,
}

impl MiddlewareFactory for SourceContractObserverFactory {
    fn label(&self) -> &'static str {
        "source_contract_observer"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<SourceContractObserverFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer(self.label(), vec![MiddlewareSurfaceKind::SourcePoll])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        Ok(MiddlewareSurfaceAttachment::source_poll_observer(Arc::new(
            SourceContractObserver {
                log: self.log.clone(),
            },
        )))
    }
}

async fn stop_after_first_poll(
    handle: obzenflow_runtime::pipeline::FlowHandle,
    observer_log: &ObserverLog,
) -> Result<()> {
    handle
        .start()
        .await
        .map_err(|error| anyhow!("infinite source flow failed to start: {error}"))?;
    observer_log.wait_for_len(1).await;
    handle
        .stop()
        .await
        .map_err(|error| anyhow!("infinite source flow failed to stop: {error}"))?;
    handle
        .wait_for_completion()
        .await
        .map_err(|error| anyhow!("infinite source flow failed to complete: {error}"))
}

#[tokio::test(start_paused = true)]
async fn control_policy_owns_raw_poll_duration_across_all_four_source_supervisors() -> Result<()> {
    let policy_delay = Duration::from_secs(30);
    let async_raw_delay = Duration::from_secs(3);

    let sync_finite_observer_log = Arc::new(ObserverLog::default());
    let sync_finite_observer_for_flow = sync_finite_observer_log.clone();
    let sync_finite_policy_log = Arc::new(PolicyLog::default());
    let sync_finite_policy_for_flow = sync_finite_policy_log.clone();
    let source = ValuesSource::new([PollingEvent { ordinal: 0 }]);
    let sink = NoopSink::new();
    let sync_finite = test_flow! {
        name: "flowip_115g_sync_finite_timing",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    sync_finite_policy_for_flow,
                ), SourceContractObserverFactory { log: sync_finite_observer_for_flow } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("sync finite timing flow failed to build: {error}"))?;
    sync_finite
        .into_inner()
        .run()
        .await
        .map_err(|error| anyhow!("sync finite timing flow failed: {error}"))?;
    assert_eq!(
        sync_finite_policy_log.snapshot()[0].poll_duration,
        Some(Duration::ZERO),
        "sync finite timing must stop before the async policy delay"
    );

    let async_finite_observer_log = Arc::new(ObserverLog::default());
    let async_finite_observer_for_flow = async_finite_observer_log.clone();
    let async_finite_policy_log = Arc::new(PolicyLog::default());
    let async_finite_policy_for_flow = async_finite_policy_log.clone();
    let source = AsyncFiniteOnce::new(async_raw_delay);
    let sink = NoopSink::new();
    let async_finite = test_flow! {
        name: "flowip_115g_async_finite_timing",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    async_finite_policy_for_flow,
                ), SourceContractObserverFactory { log: async_finite_observer_for_flow } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("async finite timing flow failed to build: {error}"))?;
    async_finite
        .into_inner()
        .run()
        .await
        .map_err(|error| anyhow!("async finite timing flow failed: {error}"))?;
    assert_eq!(
        async_finite_policy_log.snapshot()[0].poll_duration,
        Some(async_raw_delay),
        "async finite timing must include the raw poll and exclude the policy delay"
    );

    let sync_infinite_observer_log = Arc::new(ObserverLog::default());
    let sync_infinite_observer_for_flow = sync_infinite_observer_log.clone();
    let sync_infinite_policy_log = Arc::new(PolicyLog::default());
    let sync_infinite_policy_for_flow = sync_infinite_policy_log.clone();
    let sync_infinite_calls = Arc::new(AtomicUsize::new(0));
    let source = SyncInfiniteOnce::new(sync_infinite_calls);
    let sink = NoopSink::new();
    let sync_infinite = test_flow! {
        name: "flowip_115g_sync_infinite_timing",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    sync_infinite_policy_for_flow,
                ), SourceContractObserverFactory { log: sync_infinite_observer_for_flow } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("sync infinite timing flow failed to build: {error}"))?;
    stop_after_first_poll(sync_infinite.into_inner(), &sync_infinite_observer_log).await?;
    assert_eq!(
        sync_infinite_policy_log.snapshot()[0].poll_duration,
        Some(Duration::ZERO),
        "sync infinite timing must stop before the async policy delay"
    );

    let async_infinite_observer_log = Arc::new(ObserverLog::default());
    let async_infinite_observer_for_flow = async_infinite_observer_log.clone();
    let async_infinite_policy_log = Arc::new(PolicyLog::default());
    let async_infinite_policy_for_flow = async_infinite_policy_log.clone();
    let async_infinite_calls = Arc::new(AtomicUsize::new(0));
    let source = AsyncInfiniteOnce::new(async_infinite_calls, async_raw_delay);
    let sink = NoopSink::new();
    let async_infinite = test_flow! {
        name: "flowip_115g_async_infinite_timing",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    async_infinite_policy_for_flow,
                ), SourceContractObserverFactory { log: async_infinite_observer_for_flow } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("async infinite timing flow failed to build: {error}"))?;
    stop_after_first_poll(async_infinite.into_inner(), &async_infinite_observer_log).await?;
    assert_eq!(
        async_infinite_policy_log.snapshot()[0].poll_duration,
        Some(async_raw_delay),
        "async infinite timing must include the raw poll and exclude the policy delay"
    );

    Ok(())
}

#[test]
fn test_flow_handler_construction_stays_cold_inside_its_enclosing_async_future() {
    let constructor_calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&constructor_calls);

    let build = async move {
        observed_calls.fetch_add(1, Ordering::SeqCst);
        let source = TimeoutThenEof::new(Arc::new(AtomicUsize::new(0)), Duration::from_secs(1));
        let sink = NoopSink::new();

        test_flow! {
            name: "flowip_133a_test_flow_cold_probe",
            journals: memory_journals(),

            stages: {
                src = source!(PollingEvent => source);
                snk = sink!(PollingEvent => sink);
            },

            topology: {
                src |> snk;
            }
        }
        .await
    };

    assert_eq!(constructor_calls.load(Ordering::SeqCst), 0);
    drop(build);
    assert_eq!(constructor_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn timeout_duration_precedes_boundary_settlement_and_error_normalisation() -> Result<()> {
    let timeout = Duration::from_secs(7);
    let policy_delay = Duration::from_secs(40);
    let policy_log = Arc::new(PolicyLog::default());
    let observer_log = Arc::new(ObserverLog::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let policy_log_for_flow = policy_log.clone();
    let observer_log_for_flow = observer_log.clone();
    let calls_for_flow = calls.clone();
    let source = TimeoutThenEof::new(calls_for_flow, timeout);
    let sink = NoopSink::new();
    let harness = test_flow! {
        name: "flowip_115g_timeout_normalisation",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    policy_log_for_flow,
                ), SourceContractObserverFactory { log: observer_log_for_flow } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("timeout flow failed to build: {error}"))?;

    harness
        .into_inner()
        .run()
        .await
        .map_err(|error| anyhow!("timeout flow failed: {error}"))?;

    let policy_observations = policy_log.snapshot();
    assert_eq!(
        policy_observations[0],
        PolicyObservation {
            outcome: PolicyOutcomeKind::Failed,
            poll_duration: Some(timeout),
        },
        "the typed timeout crosses the source boundary before supervisor normalization"
    );
    assert_eq!(
        policy_observations
            .iter()
            .filter(|observation| observation.outcome == PolicyOutcomeKind::Failed)
            .count(),
        1,
        "the timed-out poll settles dependency policy exactly once"
    );
    let observer_observations = observer_log.snapshot();
    assert!(matches!(
        &observer_observations[0].outcome,
        SourcePollObserverOutcome::Error {
            kind: ErrorKind::Timeout
        }
    ));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the source resumes after the normalized timeout and reaches EOF"
    );

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn configured_none_disables_the_finite_source_poll_timeout() -> Result<()> {
    let raw_delay = Duration::from_secs(31);
    let policy_log = Arc::new(PolicyLog::default());
    let observer_log = Arc::new(ObserverLog::default());
    let policy_log_for_flow = Arc::clone(&policy_log);
    let observer_log_for_flow = Arc::clone(&observer_log);
    let calls = Arc::new(AtomicUsize::new(0));
    let source = DisabledTimeoutThenEof::new(Arc::clone(&calls), raw_delay);
    let sink = NoopSink::new();
    let harness = test_flow! {
        name: "flowip_133a_disabled_poll_timeout",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings::default(),
                    policy_log_for_flow,
                ), SourceContractObserverFactory { log: observer_log_for_flow } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("disabled-timeout flow failed to build: {error}"))?;

    harness
        .into_inner()
        .run()
        .await
        .map_err(|error| anyhow!("disabled-timeout flow failed: {error}"))?;

    assert_eq!(
        policy_log.snapshot()[0],
        PolicyObservation {
            outcome: PolicyOutcomeKind::Delivered {
                event_count: 1,
                has_error_marked: false,
            },
            poll_duration: Some(raw_delay),
        },
        "a poll longer than the finite default must complete when the handler disables enforcement"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum ScriptStep {
    NonData,
    Data,
    Eof,
}

fn offsets(times: &[tokio::time::Instant]) -> Vec<Duration> {
    let start = times[0];
    times.iter().map(|instant| *instant - start).collect()
}

fn policy_report_names(events: &[JournalRecord<ChainPayload>]) -> Vec<String> {
    events
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                StageLifecycleFact::Draining {
                    reason: Some(name), ..
                },
            )) if name.starts_with("source.batch.")
                || name.starts_with("policy.outbox.")
                || name == "policy.observe_outbox" =>
            {
                Some(name.clone())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn sync_and_async_idle_backoff_use_locked_caps_and_reset_on_data() -> Result<()> {
    let sync_poll_times = Arc::new(Mutex::new(Vec::new()));
    let sync_poll_times_for_flow = sync_poll_times.clone();
    let source = SyncScriptedSource::new(
        vec![
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::Data,
            ScriptStep::NonData,
            ScriptStep::Eof,
        ],
        sync_poll_times_for_flow,
    );
    let sink = NoopSink::new();
    let sync_harness = test_flow! {
        name: "flowip_115g_sync_backoff",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        emit_after_poll: true,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ), SourceContractObserverFactory { log: Arc::new(ObserverLog::default()) } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("sync backoff flow failed to build: {error}"))?;
    sync_harness
        .into_inner()
        .run()
        .await
        .map_err(|error| anyhow!("sync backoff flow failed: {error}"))?;
    assert_eq!(
        offsets(
            &sync_poll_times
                .lock()
                .expect("sync poll-times lock poisoned")
        ),
        vec![
            Duration::ZERO,
            Duration::from_millis(1),
            Duration::from_millis(3),
            Duration::from_millis(7),
            Duration::from_millis(15),
            Duration::from_millis(25),
            Duration::from_millis(25),
            Duration::from_millis(26),
        ],
        "sync sources use 1/2/4/8/10ms and reset to 1ms after data"
    );

    let async_poll_times = Arc::new(Mutex::new(Vec::new()));
    let async_poll_times_for_flow = async_poll_times.clone();
    let source = AsyncScriptedSource::new(
        vec![
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::Data,
            ScriptStep::NonData,
            ScriptStep::Eof,
        ],
        async_poll_times_for_flow,
    );
    let sink = NoopSink::new();
    let async_harness = test_flow! {
        name: "flowip_115g_async_backoff",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        emit_after_poll: true,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ), SourceContractObserverFactory { log: Arc::new(ObserverLog::default()) } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("async backoff flow failed to build: {error}"))?;
    let (_, source_journal) = async_harness
        .stage_journal_for_test("src")
        .map_err(|error| anyhow!("source journal lookup failed: {error}"))?;
    async_harness
        .into_inner()
        .run()
        .await
        .map_err(|error| anyhow!("async backoff flow failed: {error}"))?;
    assert_eq!(
        offsets(
            &async_poll_times
                .lock()
                .expect("async poll-times lock poisoned")
        ),
        vec![
            Duration::ZERO,
            Duration::from_millis(1),
            Duration::from_millis(3),
            Duration::from_millis(7),
            Duration::from_millis(15),
            Duration::from_millis(31),
            Duration::from_millis(63),
            Duration::from_millis(113),
            Duration::from_millis(113),
            Duration::from_millis(114),
        ],
        "async sources use 1/2/4/8/16/32/50ms and reset to 1ms after data"
    );

    let names = policy_report_names(
        &source_journal
            .read_causally_ordered()
            .await
            .map_err(|error| anyhow!("source journal read failed: {error}"))?,
    );
    assert_eq!(
        names,
        vec!["policy.outbox.0"],
        "empty typed polls neither author source rows nor enter the successful-batch after-poll hook"
    );

    Ok(())
}

async fn wait_for_counter(counter: &AtomicUsize, expected: usize) {
    for _ in 0..1_000 {
        if counter.load(Ordering::SeqCst) >= expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "counter did not reach {expected} without advancing paused time; observed {}",
        counter.load(Ordering::SeqCst)
    );
}

async fn wait_for_custom_rows(
    journal: &Arc<dyn Journal<ChainEvent>>,
    expected: usize,
) -> Vec<JournalRecord<ChainPayload>> {
    for _ in 0..1_000 {
        let rows = journal
            .read_causally_ordered()
            .await
            .expect("source journal read succeeds");
        if policy_report_names(&rows).len() >= expected {
            return rows;
        }
        tokio::task::yield_now().await;
    }
    panic!("source journal did not reach {expected} custom rows without advancing paused time");
}

#[tokio::test(start_paused = true)]
async fn async_control_interrupts_idle_delay_after_completed_rows_are_committed() -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_flow = calls.clone();
    let first_poll = Arc::new(Notify::new());
    let source = AsyncIdleInfiniteSource::new(calls_for_flow, first_poll.clone());
    let sink = NoopSink::new();
    let harness = test_flow! {
        name: "flowip_115g_async_idle_interrupt",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        emit_on_observe: true,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ), SourceContractObserverFactory { log: Arc::new(ObserverLog::default()) } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("idle-interruption flow failed to build: {error}"))?;
    let (_, source_journal) = harness
        .stage_journal_for_test("src")
        .map_err(|error| anyhow!("source journal lookup failed: {error}"))?;
    let handle = harness.into_inner();
    handle
        .start()
        .await
        .map_err(|error| anyhow!("idle-interruption flow failed to start: {error}"))?;

    // Startup facts cross independently polled journals. Await the first actual
    // source poll before measuring its idle delays without advancing time.
    first_poll.notified().await;
    wait_for_counter(&calls, 1).await;
    wait_for_custom_rows(&source_journal, 1).await;
    for (expected, advance_by) in [
        (2, Duration::from_millis(1)),
        (3, Duration::from_millis(2)),
        (4, Duration::from_millis(4)),
        (5, Duration::from_millis(8)),
        (6, Duration::from_millis(16)),
        (7, Duration::from_millis(32)),
    ] {
        tokio::time::advance(advance_by).await;
        wait_for_counter(&calls, expected).await;
        wait_for_custom_rows(&source_journal, expected).await;
    }

    let rows = wait_for_custom_rows(&source_journal, 7).await;
    let names = policy_report_names(&rows);
    assert_eq!(
        names.len(),
        7,
        "seven policy observation rows drain before the 50ms delay; empty typed polls author no rows"
    );
    assert!(names.iter().all(|name| name == "policy.observe_outbox"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        7,
        "the eighth poll is still behind the pending 50ms delay"
    );

    handle
        .stop()
        .await
        .map_err(|error| anyhow!("idle-interruption flow failed to stop: {error}"))?;
    handle
        .wait_for_completion()
        .await
        .map_err(|error| anyhow!("idle-interruption flow failed to complete: {error}"))?;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        7,
        "cancel interrupts the supervisor-owned delay without starting another poll"
    );
    assert_eq!(
        policy_report_names(
            &source_journal
                .read_causally_ordered()
                .await
                .map_err(|error| anyhow!("source journal read failed: {error}"))?
        ),
        names,
        "control cannot retract the completed source batch or policy report"
    );

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn eof_and_boundary_rejection_do_not_reenter_live_polling() -> Result<()> {
    let eof_calls = Arc::new(AtomicUsize::new(0));
    let eof_policy_log = Arc::new(PolicyLog::default());
    let eof_calls_for_flow = eof_calls.clone();
    let eof_policy_log_for_flow = eof_policy_log.clone();
    let source = ImmediateEof::new(eof_calls_for_flow);
    let sink = NoopSink::new();
    let eof_harness = test_flow! {
        name: "flowip_115g_eof_no_delay",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        emit_on_observe: true,
                        ..PolicySettings::default()
                    },
                    eof_policy_log_for_flow,
                ), SourceContractObserverFactory { log: Arc::new(ObserverLog::default()) } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("EOF flow failed to build: {error}"))?;
    eof_harness
        .into_inner()
        .run()
        .await
        .map_err(|error| anyhow!("EOF flow failed: {error}"))?;
    assert_eq!(eof_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        eof_policy_log.snapshot()[0],
        PolicyObservation {
            outcome: PolicyOutcomeKind::Eof,
            poll_duration: Some(Duration::ZERO),
        }
    );

    let rejected_calls = Arc::new(AtomicUsize::new(0));
    let rejected_observer_log = Arc::new(ObserverLog::default());
    let rejected_calls_for_flow = rejected_calls.clone();
    let rejected_observer_log_for_flow = rejected_observer_log.clone();
    let source = ImmediateEof::new(rejected_calls_for_flow);
    let sink = NoopSink::new();
    let rejected_harness = test_flow! {
        name: "flowip_115g_rejection_no_delay",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent with { SourceContractPolicyFactory::new(
                    PolicySettings {
                        reject: true,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ), SourceContractObserverFactory { log: rejected_observer_log_for_flow } } => source);
            snk = sink!(PollingEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|error| anyhow!("rejection flow failed to build: {error}"))?;
    rejected_harness
        .into_inner()
        .run()
        .await
        .map_err(|error| anyhow!("rejection flow failed: {error}"))?;
    assert_eq!(
        rejected_calls.load(Ordering::SeqCst),
        0,
        "boundary rejection must terminate without entering the live poll"
    );
    assert!(matches!(
        rejected_observer_log.snapshot()[0].outcome,
        SourcePollObserverOutcome::Rejected { .. }
    ));

    Ok(())
}
