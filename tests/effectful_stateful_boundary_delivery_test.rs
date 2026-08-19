// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120l: running and draining inputs both reach `decide` through the
//! descriptor-owned effect boundary.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    validate_attachment_request, EffectAttemptOutcome, EffectPolicy, MiddlewareAttachmentRequest,
    MiddlewareContext, MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError,
    MiddlewareFactoryResult, MiddlewareMaterializationContext, MiddlewareOverrideKey,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, PolicyAdmission,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{async_infinite_source, effectful_stateful, flow, sink, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError, EffectSafety, Effects};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, TypedAsyncInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, Semaphore};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BoundaryProbeInput {
    value: u64,
}

impl TypedPayload for BoundaryProbeInput {
    const EVENT_TYPE: &'static str = "effectful_stateful_boundary.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BoundaryProbeFact {
    value: u64,
}

impl TypedPayload for BoundaryProbeFact {
    const EVENT_TYPE: &'static str = "effectful_stateful_boundary.fact";
}

#[derive(Debug)]
struct BoundaryProbe {
    admissions: AtomicUsize,
    admitted: Semaphore,
}

impl Default for BoundaryProbe {
    fn default() -> Self {
        Self {
            admissions: AtomicUsize::new(0),
            admitted: Semaphore::new(0),
        }
    }
}

impl BoundaryProbe {
    fn record_admission(&self) {
        self.admissions.fetch_add(1, Ordering::SeqCst);
        self.admitted.add_permits(1);
    }

    fn admissions(&self) -> usize {
        self.admissions.load(Ordering::SeqCst)
    }

    async fn wait_for_admission(&self) {
        self.admitted
            .acquire()
            .await
            .expect("boundary admission semaphore remains open")
            .forget();
    }
}

struct BoundaryProbeFamily;

struct BoundaryProbeFactory {
    probe: Arc<BoundaryProbe>,
}

impl MiddlewareFactory for BoundaryProbeFactory {
    fn label(&self) -> &'static str {
        "effectful_stateful_boundary_probe"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<BoundaryProbeFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::Effect])
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
        Ok(MiddlewareSurfaceAttachment::effect(Arc::new(
            BoundaryProbePolicy {
                probe: self.probe.clone(),
            },
        )))
    }
}

struct BoundaryProbePolicy {
    probe: Arc<BoundaryProbe>,
}

#[async_trait]
impl EffectPolicy for BoundaryProbePolicy {
    fn label(&self) -> &'static str {
        "effectful_stateful_boundary_probe"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        self.probe.record_admission();
        PolicyAdmission::Admit
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
}

#[derive(Debug)]
struct EffectGate {
    starts: Semaphore,
    releases: Semaphore,
    calls: AtomicUsize,
}

impl Default for EffectGate {
    fn default() -> Self {
        Self {
            starts: Semaphore::new(0),
            releases: Semaphore::new(0),
            calls: AtomicUsize::new(0),
        }
    }
}

impl EffectGate {
    async fn wait_for_start(&self) {
        self.starts
            .acquire()
            .await
            .expect("effect-start semaphore remains open")
            .forget();
    }

    fn release_one(&self) {
        self.releases.add_permits(1);
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Debug)]
struct BoundaryProbeEffect {
    value: u64,
    gate: Arc<EffectGate>,
}

#[async_trait]
impl Effect for BoundaryProbeEffect {
    const EFFECT_TYPE: &'static str = "effectful_stateful_boundary.probe";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = obzenflow_runtime::effects::Portless;
    type Outcome = BoundaryProbeFact;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "boundary-probe"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.gate.calls.fetch_add(1, Ordering::SeqCst);
        self.gate.starts.add_permits(1);
        self.gate
            .releases
            .acquire()
            .await
            .expect("effect-release semaphore remains open")
            .forget();
        Ok(BoundaryProbeFact { value: self.value })
    }
}

#[derive(Clone, Debug)]
struct BoundaryProbeHandler {
    gate: Arc<EffectGate>,
}

#[async_trait]
impl EffectfulStatefulHandler for BoundaryProbeHandler {
    type State = Vec<u64>;
    type Input = BoundaryProbeInput;
    type Output = BoundaryProbeFact;
    type AllowedEffects = obzenflow_runtime::effect_set![BoundaryProbeEffect];

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        fx.perform(BoundaryProbeEffect {
            value: input.value,
            gate: self.gate.clone(),
        })
        .await?;
        Ok(fx.complete()?)
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
        state.push(fact.value);
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effectful-stateful-boundary-delivery-v1"
    }
}

#[derive(Clone, Debug)]
struct ChannelSource {
    receiver: Arc<Mutex<mpsc::UnboundedReceiver<u64>>>,
}

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for ChannelSource {
    type Output = BoundaryProbeInput;

    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        let mut receiver = self.receiver.lock().await;
        let first = receiver
            .recv()
            .await
            .ok_or_else(|| SourceError::Transport("boundary-probe input closed".to_string()))?;
        let mut outputs = vec![BoundaryProbeInput { value: first }];
        while let Ok(value) = receiver.try_recv() {
            outputs.push(BoundaryProbeInput { value });
        }
        Ok(outputs)
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl InlineSink for NoopSink {
    type Input = BoundaryProbeFact;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: Self::Input,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Noop,
            None,
        )))
    }
}

fn build_probe_flow(
    journal_root: std::path::PathBuf,
    receiver: mpsc::UnboundedReceiver<u64>,
    probe: Arc<BoundaryProbe>,
    gate: Arc<EffectGate>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let source = ChannelSource {
            receiver: Arc::new(Mutex::new(receiver)),
        };
        let handler = BoundaryProbeHandler { gate };
        let policy: Box<dyn MiddlewareFactory> = Box::new(BoundaryProbeFactory { probe });

        Ok(flow! {
            name: "effectful_stateful_boundary_delivery",
            journals: disk_journals(journal_root),

            stages: {
                input = async_infinite_source!(BoundaryProbeInput => source);
                guarded = effectful_stateful!(
                    BoundaryProbeInput -> BoundaryProbeFact
                    uses BoundaryProbeEffect with policy
                    => handler,
                    observers: [],
                );
                output = sink!(BoundaryProbeFact => NoopSink);
            },

            topology: {
                input |> guarded;
                guarded |> output;
            }
        })
    })
}

async fn wait_for_state(handle: &FlowHandle, expected: PipelineState) -> Result<()> {
    let mut receiver = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if *receiver.borrow() == expected {
                return Ok(());
            }
            receiver
                .changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for pipeline state {expected:?}"))?
}

async fn build_running_flow(
    journal_root: std::path::PathBuf,
) -> Result<(
    FlowHandle,
    mpsc::UnboundedSender<u64>,
    Arc<BoundaryProbe>,
    Arc<EffectGate>,
)> {
    let (sender, receiver) = mpsc::unbounded_channel();
    let probe = Arc::new(BoundaryProbe::default());
    let gate = Arc::new(EffectGate::default());
    let handle = build_probe_flow(journal_root, receiver, probe.clone(), gate.clone())
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|error| anyhow!("boundary probe flow failed to build: {error:?}"))?;
    wait_for_state(&handle, PipelineState::Running).await?;
    Ok((handle, sender, probe, gate))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_input_delivers_descriptor_boundary_to_decide() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let (handle, sender, probe, gate) = build_running_flow(temp.path().join("running")).await?;

    sender.send(1)?;
    probe.wait_for_admission().await;
    gate.wait_for_start().await;

    assert_eq!(probe.admissions(), 1);
    assert_eq!(gate.calls(), 1);
    assert_eq!(handle.current_state(), PipelineState::Running);

    gate.release_one();
    handle.stop_graceful(Duration::from_secs(10)).await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("running boundary probe did not complete"))??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draining_input_delivers_descriptor_boundary_to_decide() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let (handle, sender, probe, gate) = build_running_flow(temp.path().join("draining")).await?;

    sender.send(1)?;
    sender.send(2)?;
    probe.wait_for_admission().await;
    gate.wait_for_start().await;
    assert_eq!(handle.current_state(), PipelineState::Running);

    handle.stop_graceful(Duration::from_secs(10)).await?;
    wait_for_state(&handle, PipelineState::Draining).await?;
    gate.release_one();

    probe.wait_for_admission().await;
    gate.wait_for_start().await;
    assert_eq!(probe.admissions(), 2);
    assert_eq!(gate.calls(), 2);
    assert_eq!(handle.current_state(), PipelineState::Draining);

    gate.release_one();
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("draining boundary probe did not complete"))??;
    Ok(())
}
