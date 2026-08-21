// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_adapters::middleware::{handler_observer, stage_lifecycle_observer};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{SystemEvent, SystemEventType};
use obzenflow_core::journal::Journal;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::common::stage_handle::{STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP};
use obzenflow_runtime::stages::observer::{
    HandlerObserver, HandlerObserverContext, StageLifecycleObserver, StageLifecycleObserverContext,
    StageLifecyclePhase,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};

/// File-local payload for the hung-handler cancellation test. The JSON
/// shape matches what `OneEventSource` emits; the type fingerprints the
/// stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProbeEvent {
    value: u64,
}

impl TypedPayload for ProbeEvent {
    const EVENT_TYPE: &'static str = "probe.event";
}
use std::future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Debug)]
struct OneEventSource {
    emitted: bool,
}

impl OneEventSource {
    fn new() -> Self {
        Self { emitted: false }
    }
}

impl TypedFiniteSourceHandler for OneEventSource {
    type Output = ProbeEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ProbeEvent { value: 1 }]))
    }
}

#[derive(Clone, Debug)]
struct HungTransform {
    entered: Arc<Notify>,
    dropped: Arc<AtomicBool>,
    drop_observed: Arc<Notify>,
}

impl HungTransform {
    fn new(entered: Arc<Notify>, dropped: Arc<AtomicBool>, drop_observed: Arc<Notify>) -> Self {
        Self {
            entered,
            dropped,
            drop_observed,
        }
    }
}

struct PendingInvocationGuard {
    dropped: Arc<AtomicBool>,
    drop_observed: Arc<Notify>,
}

impl Drop for PendingInvocationGuard {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        self.drop_observed.notify_one();
    }
}

#[async_trait]
impl EffectfulTransformHandler for HungTransform {
    type Input = ProbeEvent;
    type Output = ProbeEvent;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        _input: ProbeEvent,
        _fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let _pending = PendingInvocationGuard {
            dropped: self.dropped.clone(),
            drop_observed: self.drop_observed.clone(),
        };
        self.entered.notify_one();
        future::pending::<()>().await;
        unreachable!("pending() never resolves")
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl InlineSink for NoopSink {
    type Input = ProbeEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: ProbeEvent,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        )))
    }
}

struct CountsHungHandlerOccurrences {
    before: Arc<AtomicUsize>,
    after: Arc<AtomicUsize>,
}

impl HandlerObserver for CountsHungHandlerOccurrences {
    fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {
        self.before.fetch_add(1, Ordering::SeqCst);
    }

    fn after_handle(
        &self,
        _ctx: &HandlerObserverContext<'_>,
        _outputs: &[obzenflow_core::ChainEvent],
    ) {
        self.after.fetch_add(1, Ordering::SeqCst);
    }
}

struct RecordsHungLifecycle {
    phases: Arc<Mutex<Vec<StageLifecyclePhase>>>,
}

impl StageLifecycleObserver for RecordsHungLifecycle {
    fn on_stage_lifecycle(&self, ctx: &StageLifecycleObserverContext<'_>) {
        self.phases
            .lock()
            .expect("hung lifecycle observation lock")
            .push(ctx.phase());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_hung_handler_can_be_cancelled_without_contract_failure() {
    let flow_handle_slot: Arc<Mutex<Option<Arc<FlowHandle>>>> = Arc::new(Mutex::new(None));
    let system_journal_slot: Arc<Mutex<Option<Arc<dyn Journal<SystemEvent>>>>> =
        Arc::new(Mutex::new(None));
    let flow_handle_slot_hook = flow_handle_slot.clone();
    let system_journal_slot_hook = system_journal_slot.clone();
    let handler_entered = Arc::new(Notify::new());
    let handler_dropped = Arc::new(AtomicBool::new(false));
    let handler_drop_observed = Arc::new(Notify::new());
    let handler_entered_for_flow = handler_entered.clone();
    let handler_dropped_for_flow = handler_dropped.clone();
    let handler_drop_observed_for_flow = handler_drop_observed.clone();
    let before_callbacks = Arc::new(AtomicUsize::new(0));
    let after_callbacks = Arc::new(AtomicUsize::new(0));
    let lifecycle_phases = Arc::new(Mutex::new(Vec::new()));
    let before_callbacks_for_flow = before_callbacks.clone();
    let after_callbacks_for_flow = after_callbacks.clone();
    let lifecycle_phases_for_flow = lifecycle_phases.clone();

    let hook = Box::new(move |handle: &Arc<FlowHandle>| {
        *flow_handle_slot_hook.lock().expect("flow_handle_slot lock") = Some(handle.clone());
        let system_journal = handle.system_journal().expect("system journal available");
        *system_journal_slot_hook
            .lock()
            .expect("system_journal_slot lock") = Some(system_journal);
        tokio::spawn(async {})
    });

    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let one_event_source = OneEventSource::new();
        let hung_transform = HungTransform::new(
            handler_entered_for_flow.clone(),
            handler_dropped_for_flow.clone(),
            handler_drop_observed_for_flow.clone(),
        );
        let noop_sink = NoopSink;

        Ok(flow! {
            name: "liveness_hung_handler_cancel",
            journals: memory_journals(),

            stages: {
                numbers = source!(ProbeEvent => one_event_source);
                hung = effectful_transform!(
                    ProbeEvent -> ProbeEvent => hung_transform,
                    observers: [
                        handler_observer(
                            "hung-handler-occurrences",
                            CountsHungHandlerOccurrences {
                                before: before_callbacks_for_flow,
                                after: after_callbacks_for_flow,
                            }
                        ),
                        stage_lifecycle_observer(
                            "hung-lifecycle-occurrences",
                            RecordsHungLifecycle {
                                phases: lifecycle_phases_for_flow,
                            }
                        )
                    ],
                );
                snk = sink!(ProbeEvent => noop_sink);
            },

            topology: {
                numbers |> hung;
                hung |> snk;
            }
        })
    });

    let run_task = tokio::spawn(async move {
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .with_flow_handle_hook(hook)
            .run_async(flow_definition)
            .await
    });

    // Wait for FlowHandle to be available and running before requesting stop.
    let flow_handle = {
        let mut captured_running = None;
        for _ in 0..200 {
            if let Some(handle) = flow_handle_slot
                .lock()
                .expect("flow_handle_slot lock")
                .clone()
            {
                if handle.is_running() {
                    captured_running = Some(handle);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        captured_running.expect("flow handle captured and running")
    };

    tokio::time::timeout(Duration::from_secs(5), handler_entered.notified())
        .await
        .expect("hung handler should enter before stop is requested");

    flow_handle
        .stop_graceful(Duration::from_millis(100))
        .await
        .expect("stop_graceful request");

    // The supervisor uses real-time deadlines for graceful stop escalation, so use real time here.
    let _ = tokio::time::timeout(Duration::from_secs(5), run_task)
        .await
        .expect("flow should terminate after graceful stop timeout escalation")
        .expect("flow task join");

    // FlowApplication can tear down background tasks quickly on stop; give the pipeline
    // a moment to finish appending terminal lifecycle events to the system journal.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let system_journal = system_journal_slot
        .lock()
        .expect("system_journal_slot lock")
        .clone()
        .expect("system journal captured by hook");

    let envelopes = system_journal
        .read_causally_ordered()
        .await
        .expect("read system journal");

    let mut pipeline_events: Vec<String> = Vec::new();
    let mut saw_stop_requested = false;
    let mut saw_cancelled = false;
    let mut saw_stop_failed = false;
    for envelope in envelopes {
        match &envelope.event.event {
            SystemEventType::PipelineLifecycle(event) => {
                pipeline_events.push(format!("{event:?}"));
                match event {
                    obzenflow_core::event::PipelineLifecycleEvent::StopRequested { .. } => {
                        saw_stop_requested = true
                    }
                    obzenflow_core::event::PipelineLifecycleEvent::Cancelled { .. } => {
                        saw_cancelled = true
                    }
                    obzenflow_core::event::PipelineLifecycleEvent::Failed { reason, .. }
                        if reason == STOP_REASON_USER_STOP || reason == STOP_REASON_TIMEOUT =>
                    {
                        saw_stop_failed = true
                    }
                    _ => {}
                }
            }
            SystemEventType::StageLifecycle {
                event: obzenflow_core::event::StageLifecycleEvent::Cancelled { .. },
                ..
            } => saw_cancelled = true,
            SystemEventType::ContractStatus { pass, .. } => {
                assert!(
                    *pass,
                    "unexpected ContractStatus(pass=false) while exercising hung handler cancellation"
                );
            }
            _ => {}
        }
    }

    if !(saw_stop_requested || saw_cancelled || saw_stop_failed) {
        panic!(
            "expected a stop/cancel lifecycle event after stop escalation; saw pipeline events: {pipeline_events:?}"
        );
    }

    if !handler_dropped.load(Ordering::SeqCst) {
        tokio::time::timeout(Duration::from_secs(5), handler_drop_observed.notified())
            .await
            .expect("cancelled handler invocation should be dropped");
    }
    assert!(
        handler_dropped.load(Ordering::SeqCst),
        "cancelling the stage must drop the directly awaited handler invocation"
    );
    assert_eq!(
        before_callbacks.load(Ordering::SeqCst),
        1,
        "the live handler invocation receives one before callback"
    );
    assert_eq!(
        after_callbacks.load(Ordering::SeqCst),
        0,
        "cancelling an in-flight handler invocation must not manufacture an after callback"
    );
    assert_eq!(
        *lifecycle_phases
            .lock()
            .expect("hung lifecycle assertion lock"),
        [StageLifecyclePhase::Running],
        "force cancellation aborts the stage task and must not manufacture a terminal lifecycle transition"
    );
}
