// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115g source-poll causal-order and idle-scheduling proof.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::observer::{
    ObserverReport, SourcePollObserver, SourcePollObserverContext, SourcePollObserverOutcome,
};
use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, SourceAdmission, SourceAfterPoll, SourceBatchFacts, SourcePolicy,
    SourcePolicyCtx, SourcePollAttachment, SourcePollOutcome,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventContent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{
    async_infinite_source, async_source, infinite_source, sink, source, test_flow,
};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler, SinkHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PollingEvent {
    ordinal: usize,
}

impl TypedPayload for PollingEvent {
    const EVENT_TYPE: &'static str = "flowip_115g.source_polling";
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl SinkHandler for NoopSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("source-polling-proof".to_string()),
            None,
        ))
    }
}

fn custom_metric(writer_id: WriterId, name: impl Into<String>) -> ChainEvent {
    ChainEventFactory::observability_event(
        writer_id,
        ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
            name: name.into(),
            value: json!({}),
            tags: None,
        }),
    )
}

fn data_event(writer_id: WriterId, ordinal: usize) -> ChainEvent {
    ChainEventFactory::data_event(
        writer_id,
        PollingEvent::versioned_event_type(),
        json!(PollingEvent { ordinal }),
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
    poll_duration: Duration,
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
            ctx.write_control_event(custom_metric(ctx.writer_id(), "policy.rejection_outbox"));
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
            ctx.write_control_event(custom_metric(
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
            ctx.write_control_event(custom_metric(ctx.writer_id(), "policy.observe_outbox"));
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
    fn label(&self) -> &'static str {
        "source_contract_observer"
    }

    fn after_source_poll(
        &self,
        ctx: &SourcePollObserverContext<'_>,
        _outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        self.log.push(ObserverObservation {
            outcome: ctx.outcome.clone(),
            poll_duration: ctx.poll_duration,
        });
        ObserverReport::empty()
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

#[derive(Clone, Debug)]
struct SyncFiniteOnce {
    emitted: bool,
    writer_id: WriterId,
}

impl SyncFiniteOnce {
    fn new() -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for SyncFiniteOnce {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![data_event(self.writer_id, 0)]))
    }
}

#[derive(Clone, Debug)]
struct AsyncFiniteOnce {
    emitted: bool,
    writer_id: WriterId,
    raw_delay: Duration,
}

#[async_trait]
impl AsyncFiniteSourceHandler for AsyncFiniteOnce {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        tokio::time::sleep(self.raw_delay).await;
        Ok(Some(vec![data_event(self.writer_id, 0)]))
    }
}

#[derive(Clone, Debug)]
struct SyncInfiniteOnce {
    calls: Arc<AtomicUsize>,
    writer_id: WriterId,
}

impl InfiniteSourceHandler for SyncInfiniteOnce {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            Ok(vec![data_event(self.writer_id, 0)])
        } else {
            Ok(Vec::new())
        }
    }
}

#[derive(Clone, Debug)]
struct AsyncInfiniteOnce {
    calls: Arc<AtomicUsize>,
    writer_id: WriterId,
    raw_delay: Duration,
}

#[async_trait]
impl AsyncInfiniteSourceHandler for AsyncInfiniteOnce {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            tokio::time::sleep(self.raw_delay).await;
            Ok(vec![data_event(self.writer_id, 0)])
        } else {
            pending::<Result<Vec<ChainEvent>, SourceError>>().await
        }
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
async fn poll_duration_is_raw_poll_only_across_all_four_source_supervisors() -> Result<()> {
    let policy_delay = Duration::from_secs(30);
    let async_raw_delay = Duration::from_secs(3);

    let sync_finite_observer_log = Arc::new(ObserverLog::default());
    let sync_finite_observer_for_flow = sync_finite_observer_log.clone();
    let source = SyncFiniteOnce::new();
    let sink = NoopSink;
    let sync_finite = test_flow! {
        name: "flowip_115g_sync_finite_timing",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ),
                SourceContractObserverFactory { log: sync_finite_observer_for_flow }
            ]);
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
        sync_finite_observer_log.snapshot()[0].poll_duration,
        Duration::ZERO,
        "sync finite timing must stop before the async policy delay"
    );

    let async_finite_observer_log = Arc::new(ObserverLog::default());
    let async_finite_observer_for_flow = async_finite_observer_log.clone();
    let source = AsyncFiniteOnce {
        emitted: false,
        writer_id: WriterId::from(StageId::new()),
        raw_delay: async_raw_delay,
    };
    let sink = NoopSink;
    let async_finite = test_flow! {
        name: "flowip_115g_async_finite_timing",
        journals: memory_journals(),

        stages: {
            src = async_source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ),
                SourceContractObserverFactory { log: async_finite_observer_for_flow }
            ]);
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
        async_finite_observer_log.snapshot()[0].poll_duration,
        async_raw_delay,
        "async finite timing must include the raw poll and exclude the policy delay"
    );

    let sync_infinite_observer_log = Arc::new(ObserverLog::default());
    let sync_infinite_observer_for_flow = sync_infinite_observer_log.clone();
    let sync_infinite_calls = Arc::new(AtomicUsize::new(0));
    let source = SyncInfiniteOnce {
        calls: sync_infinite_calls,
        writer_id: WriterId::from(StageId::new()),
    };
    let sink = NoopSink;
    let sync_infinite = test_flow! {
        name: "flowip_115g_sync_infinite_timing",
        journals: memory_journals(),

        stages: {
            src = infinite_source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ),
                SourceContractObserverFactory { log: sync_infinite_observer_for_flow }
            ]);
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
        sync_infinite_observer_log.snapshot()[0].poll_duration,
        Duration::ZERO,
        "sync infinite timing must stop before the async policy delay"
    );

    let async_infinite_observer_log = Arc::new(ObserverLog::default());
    let async_infinite_observer_for_flow = async_infinite_observer_log.clone();
    let async_infinite_calls = Arc::new(AtomicUsize::new(0));
    let source = AsyncInfiniteOnce {
        calls: async_infinite_calls,
        writer_id: WriterId::from(StageId::new()),
        raw_delay: async_raw_delay,
    };
    let sink = NoopSink;
    let async_infinite = test_flow! {
        name: "flowip_115g_async_infinite_timing",
        journals: memory_journals(),

        stages: {
            src = async_infinite_source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ),
                SourceContractObserverFactory { log: async_infinite_observer_for_flow }
            ]);
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
        async_infinite_observer_log.snapshot()[0].poll_duration,
        async_raw_delay,
        "async infinite timing must include the raw poll and exclude the policy delay"
    );

    Ok(())
}

#[derive(Clone, Debug)]
struct TimeoutThenEof {
    calls: Arc<AtomicUsize>,
    timeout: Duration,
}

#[async_trait]
impl AsyncFiniteSourceHandler for TimeoutThenEof {
    fn poll_timeout(&self) -> Option<Duration> {
        Some(self.timeout)
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            pending::<Result<Option<Vec<ChainEvent>>, SourceError>>().await
        } else {
            Ok(None)
        }
    }
}

#[test]
fn test_flow_handler_construction_stays_cold_inside_its_enclosing_async_future() {
    let constructor_calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&constructor_calls);

    let build = async move {
        observed_calls.fetch_add(1, Ordering::SeqCst);
        let source = TimeoutThenEof {
            calls: Arc::new(AtomicUsize::new(0)),
            timeout: Duration::from_secs(1),
        };
        let sink = NoopSink;

        test_flow! {
            name: "flowip_133a_test_flow_cold_probe",
            journals: memory_journals(),

            stages: {
                src = async_source!(PollingEvent => source);
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
async fn timeout_duration_and_error_normalisation_are_inside_the_raw_poll_execution() -> Result<()>
{
    let timeout = Duration::from_secs(7);
    let policy_delay = Duration::from_secs(40);
    let policy_log = Arc::new(PolicyLog::default());
    let observer_log = Arc::new(ObserverLog::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let policy_log_for_flow = policy_log.clone();
    let observer_log_for_flow = observer_log.clone();
    let calls_for_flow = calls.clone();
    let source = TimeoutThenEof {
        calls: calls_for_flow,
        timeout,
    };
    let sink = NoopSink;
    let harness = test_flow! {
        name: "flowip_115g_timeout_normalisation",
        journals: memory_journals(),

        stages: {
            src = async_source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        after_poll_delay: policy_delay,
                        ..PolicySettings::default()
                    },
                    policy_log_for_flow,
                ),
                SourceContractObserverFactory { log: observer_log_for_flow }
            ]);
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
            outcome: PolicyOutcomeKind::Delivered {
                event_count: 1,
                has_error_marked: true,
            },
            poll_duration: Some(timeout),
        },
        "the timeout is normalized exactly once before policy settlement"
    );
    assert!(
        policy_observations
            .iter()
            .all(|observation| observation.outcome != PolicyOutcomeKind::Failed),
        "a normalized timeout must never surface as SourcePollOutcome::Failed"
    );
    assert_eq!(
        observer_log.snapshot()[0].poll_duration,
        timeout,
        "timeout measurement excludes normalization and the later policy delay"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the source resumes after the normalized timeout and reaches EOF"
    );

    Ok(())
}

#[derive(Clone, Debug)]
struct DisabledTimeoutThenEof {
    calls: Arc<AtomicUsize>,
    writer_id: WriterId,
    raw_delay: Duration,
}

#[async_trait]
impl AsyncFiniteSourceHandler for DisabledTimeoutThenEof {
    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(self.raw_delay).await;
            Ok(Some(vec![data_event(self.writer_id, 1)]))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test(start_paused = true)]
async fn configured_none_disables_the_finite_source_poll_timeout() -> Result<()> {
    let raw_delay = Duration::from_secs(31);
    let policy_log = Arc::new(PolicyLog::default());
    let observer_log = Arc::new(ObserverLog::default());
    let policy_log_for_flow = Arc::clone(&policy_log);
    let observer_log_for_flow = Arc::clone(&observer_log);
    let calls = Arc::new(AtomicUsize::new(0));
    let source = DisabledTimeoutThenEof {
        calls: Arc::clone(&calls),
        writer_id: WriterId::from(StageId::new()),
        raw_delay,
    };
    let sink = NoopSink;
    let harness = test_flow! {
        name: "flowip_133a_disabled_poll_timeout",
        journals: memory_journals(),

        stages: {
            src = async_source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings::default(),
                    policy_log_for_flow,
                ),
                SourceContractObserverFactory { log: observer_log_for_flow }
            ]);
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
    assert_eq!(observer_log.snapshot()[0].poll_duration, raw_delay);
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum ScriptStep {
    NonData,
    Data,
    Eof,
}

fn scripted_result(
    step: ScriptStep,
    ordinal: usize,
    writer_id: WriterId,
) -> Option<Vec<ChainEvent>> {
    match step {
        ScriptStep::NonData => Some(vec![custom_metric(
            writer_id,
            format!("source.batch.{ordinal}"),
        )]),
        ScriptStep::Data => Some(vec![data_event(writer_id, ordinal)]),
        ScriptStep::Eof => None,
    }
}

#[derive(Clone, Debug)]
struct SyncScriptedSource {
    steps: Vec<ScriptStep>,
    index: usize,
    writer_id: WriterId,
    poll_times: Arc<Mutex<Vec<tokio::time::Instant>>>,
}

impl FiniteSourceHandler for SyncScriptedSource {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.poll_times
            .lock()
            .expect("sync poll-times lock poisoned")
            .push(tokio::time::Instant::now());
        let ordinal = self.index;
        let step = self.steps[self.index];
        self.index += 1;
        Ok(scripted_result(step, ordinal, self.writer_id))
    }
}

#[derive(Clone, Debug)]
struct AsyncScriptedSource {
    steps: Vec<ScriptStep>,
    index: usize,
    writer_id: WriterId,
    poll_times: Arc<Mutex<Vec<tokio::time::Instant>>>,
}

#[async_trait]
impl AsyncFiniteSourceHandler for AsyncScriptedSource {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.poll_times
            .lock()
            .expect("async poll-times lock poisoned")
            .push(tokio::time::Instant::now());
        let ordinal = self.index;
        let step = self.steps[self.index];
        self.index += 1;
        Ok(scripted_result(step, ordinal, self.writer_id))
    }
}

fn offsets(times: &[tokio::time::Instant]) -> Vec<Duration> {
    let start = times[0];
    times.iter().map(|instant| *instant - start).collect()
}

fn custom_metric_names(events: &[obzenflow_core::EventEnvelope<ChainEvent>]) -> Vec<String> {
    events
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Observability(ObservabilityPayload::Metrics(
                MetricsLifecycle::Custom { name, .. },
            )) if name.starts_with("source.batch.") || name.starts_with("policy.outbox.") => {
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
    let source = SyncScriptedSource {
        steps: vec![
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::NonData,
            ScriptStep::Data,
            ScriptStep::NonData,
            ScriptStep::Eof,
        ],
        index: 0,
        writer_id: WriterId::from(StageId::new()),
        poll_times: sync_poll_times_for_flow,
    };
    let sink = NoopSink;
    let sync_harness = test_flow! {
        name: "flowip_115g_sync_backoff",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        emit_after_poll: true,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ),
                SourceContractObserverFactory { log: Arc::new(ObserverLog::default()) }
            ]);
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
    let source = AsyncScriptedSource {
        steps: vec![
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
        index: 0,
        writer_id: WriterId::from(StageId::new()),
        poll_times: async_poll_times_for_flow,
    };
    let sink = NoopSink;
    let async_harness = test_flow! {
        name: "flowip_115g_async_backoff",
        journals: memory_journals(),

        stages: {
            src = async_source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        emit_after_poll: true,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ),
                SourceContractObserverFactory { log: Arc::new(ObserverLog::default()) }
            ]);
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

    let names = custom_metric_names(
        &source_journal
            .read_causally_ordered()
            .await
            .map_err(|error| anyhow!("source journal read failed: {error}"))?,
    );
    for ordinal in 0..7 {
        let source_name = format!("source.batch.{ordinal}");
        let outbox_name = format!("policy.outbox.{ordinal}");
        let source_position = names
            .iter()
            .position(|name| name == &source_name)
            .expect("normalized source batch row is journaled");
        let outbox_position = names
            .iter()
            .position(|name| name == &outbox_name)
            .expect("policy outbox row is journaled");
        assert_eq!(
            outbox_position,
            source_position + 1,
            "source batch {ordinal} must be committed immediately before its policy outbox"
        );
    }
    let reset_source_position = names
        .iter()
        .position(|name| name == "source.batch.8")
        .expect("post-data non-data batch is journaled");
    let reset_outbox_position = names
        .iter()
        .position(|name| name == "policy.outbox.8")
        .expect("post-data policy outbox is journaled");
    assert_eq!(reset_outbox_position, reset_source_position + 1);

    Ok(())
}

#[derive(Clone, Debug)]
struct AsyncIdleInfiniteSource {
    calls: Arc<AtomicUsize>,
    writer_id: WriterId,
}

#[async_trait]
impl AsyncInfiniteSourceHandler for AsyncIdleInfiniteSource {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![custom_metric(
            self.writer_id,
            format!("source.batch.{ordinal}"),
        )])
    }
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
) -> Vec<obzenflow_core::EventEnvelope<ChainEvent>> {
    for _ in 0..1_000 {
        let rows = journal
            .read_causally_ordered()
            .await
            .expect("source journal read succeeds");
        if custom_metric_names(&rows).len() >= expected {
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
    let source = AsyncIdleInfiniteSource {
        calls: calls_for_flow,
        writer_id: WriterId::from(StageId::new()),
    };
    let sink = NoopSink;
    let harness = test_flow! {
        name: "flowip_115g_async_idle_interrupt",
        journals: memory_journals(),

        stages: {
            src = async_infinite_source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        emit_after_poll: true,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ),
                SourceContractObserverFactory { log: Arc::new(ObserverLog::default()) }
            ]);
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

    wait_for_counter(&calls, 1).await;
    wait_for_custom_rows(&source_journal, 2).await;
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
        wait_for_custom_rows(&source_journal, expected * 2).await;
    }

    let rows = wait_for_custom_rows(&source_journal, 14).await;
    let names = custom_metric_names(&rows);
    assert_eq!(
        names.len(),
        14,
        "seven completed batches and seven policy outbox rows drain before the 50ms delay"
    );
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
        custom_metric_names(
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

#[derive(Clone, Debug)]
struct ImmediateEof {
    calls: Arc<AtomicUsize>,
}

impl FiniteSourceHandler for ImmediateEof {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

#[tokio::test(start_paused = true)]
async fn eof_and_boundary_rejection_do_not_reenter_live_polling() -> Result<()> {
    let eof_calls = Arc::new(AtomicUsize::new(0));
    let eof_policy_log = Arc::new(PolicyLog::default());
    let eof_calls_for_flow = eof_calls.clone();
    let eof_policy_log_for_flow = eof_policy_log.clone();
    let source = ImmediateEof {
        calls: eof_calls_for_flow,
    };
    let sink = NoopSink;
    let eof_harness = test_flow! {
        name: "flowip_115g_eof_no_delay",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        emit_on_observe: true,
                        ..PolicySettings::default()
                    },
                    eof_policy_log_for_flow,
                ),
                SourceContractObserverFactory { log: Arc::new(ObserverLog::default()) }
            ]);
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
    let source = ImmediateEof {
        calls: rejected_calls_for_flow,
    };
    let sink = NoopSink;
    let rejected_harness = test_flow! {
        name: "flowip_115g_rejection_no_delay",
        journals: memory_journals(),

        stages: {
            src = source!(PollingEvent => source with [
                SourceContractPolicyFactory::new(
                    PolicySettings {
                        reject: true,
                        ..PolicySettings::default()
                    },
                    Arc::new(PolicyLog::default()),
                ),
                SourceContractObserverFactory { log: rejected_observer_log_for_flow }
            ]);
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
