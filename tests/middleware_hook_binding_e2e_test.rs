// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115b: end-to-end proof for the hook-bound middleware rails.
//!
//! A third-party control middleware declares `SourcePoll`, `Effect`, and
//! `SinkDelivery` surfaces, is placed by the DSL binder through the public
//! carrier, rejects one effect protected unit, and observes a live run plus
//! strict replay of the same archive.

use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    validate_attachment_request, ControlMiddlewareRole, EffectPolicy, EffectSurface, EffectTypeKey,
    EffectUnitId, Middleware, MiddlewareAttachmentRequest, MiddlewareContext,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareKind, MiddlewareMaterializationContext, MiddlewareOrigin, MiddlewareOverrideKey,
    MiddlewarePlanContribution, MiddlewareSurface, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind::Effect, MiddlewareSurfaceKind::SinkDelivery,
    MiddlewareSurfaceKind::SourcePoll, PolicyAdmission, ProtectedUnit, ProtectedUnitId,
    SinkAdmission, SinkDeliveryPolicyOutcome, SinkDeliverySurface, SinkDeliveryTarget,
    SinkDeliveryUnitId, SinkPolicy, SinkPolicyCtx, SourceAdmission, SourcePolicy, SourcePolicyCtx,
    SourcePollAttachment, SourcePollOutcome, SourcePollSurface, SourcePollUnitId,
    TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect as RuntimeEffect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{EffectfulTransformHandler, FiniteSourceHandler};
use obzenflow_runtime::stages::sink::{DeliveryContext, DeliveryProvenance, SinkTyped};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct HookCounters {
    source_materialized: AtomicUsize,
    effect_materialized: AtomicUsize,
    sink_materialized: AtomicUsize,
    source_admits: AtomicUsize,
    source_observes: AtomicUsize,
    effect_attempts: AtomicUsize,
    effect_admits: AtomicUsize,
    effect_rejections: AtomicUsize,
    effect_observes: AtomicUsize,
    sink_admits: AtomicUsize,
    sink_observes: AtomicUsize,
}

impl HookCounters {
    fn materialized_surfaces(&self) -> (usize, usize, usize) {
        (
            self.source_materialized.load(Ordering::SeqCst),
            self.effect_materialized.load(Ordering::SeqCst),
            self.sink_materialized.load(Ordering::SeqCst),
        )
    }
}

struct HookProofFamily;

struct HookProofFactory {
    counters: Arc<HookCounters>,
    reject_effect_value: u64,
}

impl HookProofFactory {
    fn new(counters: Arc<HookCounters>, reject_effect_value: u64) -> Self {
        Self {
            counters,
            reject_effect_value,
        }
    }
}

impl MiddlewareFactory for HookProofFactory {
    fn label(&self) -> &'static str {
        "hook_proof_control"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<HookProofFamily>("hook_proof_control")
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        ControlMiddlewareRole::None
    }

    fn kind(&self) -> MiddlewareKind {
        MiddlewareKind::Policy
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        None
    }

    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
        Err(MiddlewareFactoryError::not_hook_bound(self.label()))
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![SourcePoll, Effect, SinkDelivery])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        let declaration = self.declaration();
        validate_attachment_request(&declaration, &request).map_err(|err| {
            MiddlewareFactoryError::materialization_failed(self.label(), &context.config.name, err)
        })?;

        match request.surface {
            MiddlewareSurface::SourcePoll(_) => {
                self.counters
                    .source_materialized
                    .fetch_add(1, Ordering::SeqCst);
                Ok(MiddlewareSurfaceAttachment::SourcePoll(
                    SourcePollAttachment {
                        policy: Arc::new(HookProofSourcePolicy {
                            counters: self.counters.clone(),
                        }),
                        completion_gate: None,
                    },
                ))
            }
            MiddlewareSurface::Effect(_) => {
                self.counters
                    .effect_materialized
                    .fetch_add(1, Ordering::SeqCst);
                Ok(MiddlewareSurfaceAttachment::Effect(Arc::new(
                    HookProofEffectPolicy {
                        counters: self.counters.clone(),
                        reject_value: self.reject_effect_value,
                    },
                )))
            }
            MiddlewareSurface::SinkDelivery(_) => {
                self.counters
                    .sink_materialized
                    .fetch_add(1, Ordering::SeqCst);
                Ok(MiddlewareSurfaceAttachment::SinkDelivery(Arc::new(
                    HookProofSinkPolicy {
                        counters: self.counters.clone(),
                    },
                )))
            }
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!("unsupported surface {:?}", other.kind())),
            )),
        }
    }
}

struct HookProofSourcePolicy {
    counters: Arc<HookCounters>,
}

#[async_trait]
impl SourcePolicy for HookProofSourcePolicy {
    fn label(&self) -> &'static str {
        "hook_proof_source"
    }

    async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        self.counters.source_admits.fetch_add(1, Ordering::SeqCst);
        SourceAdmission::Admit(None)
    }

    fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
        self.counters.source_observes.fetch_add(1, Ordering::SeqCst);
    }
}

struct HookProofEffectPolicy {
    counters: Arc<HookCounters>,
    reject_value: u64,
}

#[async_trait]
impl EffectPolicy for HookProofEffectPolicy {
    fn label(&self) -> &'static str {
        "hook_proof_effect"
    }

    async fn admit(
        &self,
        _identity: &obzenflow_runtime::effects::EffectIdentity,
        event: &ChainEvent,
        _ctx: &mut MiddlewareContext,
    ) -> PolicyAdmission {
        self.counters.effect_attempts.fetch_add(1, Ordering::SeqCst);
        let value = HookInput::from_event(event)
            .map(|input| input.value)
            .unwrap_or_default();
        if value == self.reject_value {
            self.counters
                .effect_rejections
                .fetch_add(1, Ordering::SeqCst);
            return PolicyAdmission::Reject(obzenflow_adapters::middleware::MiddlewareAbortCause {
                source: EffectFailureSource::new("hook_proof_control"),
                code: EffectFailureCode::new("rejected_by_hook_proof"),
                message: "hook proof effect rejected".to_string(),
                retry: RetryDisposition::NotRetryable,
                event: None,
            });
        }
        self.counters.effect_admits.fetch_add(1, Ordering::SeqCst);
        PolicyAdmission::Admit
    }

    fn observe(
        &self,
        _identity: &obzenflow_runtime::effects::EffectIdentity,
        _event: &ChainEvent,
        _attempt: &obzenflow_adapters::middleware::EffectAttemptOutcome<'_>,
        _ctx: &mut MiddlewareContext,
    ) {
        self.counters.effect_observes.fetch_add(1, Ordering::SeqCst);
    }
}

struct HookProofSinkPolicy {
    counters: Arc<HookCounters>,
}

#[async_trait]
impl SinkPolicy for HookProofSinkPolicy {
    fn label(&self) -> &'static str {
        "hook_proof_sink"
    }

    async fn admit(&self, _ctx: &mut SinkPolicyCtx) -> SinkAdmission {
        self.counters.sink_admits.fetch_add(1, Ordering::SeqCst);
        SinkAdmission::Admit(None)
    }

    fn observe(&self, _outcome: &SinkDeliveryPolicyOutcome<'_>, _ctx: &mut SinkPolicyCtx) {
        self.counters.sink_observes.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HookInput {
    value: u64,
}

impl TypedPayload for HookInput {
    const EVENT_TYPE: &'static str = "middleware_hook_binding.input";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HookEffectValue {
    value: u64,
}

impl TypedPayload for HookEffectValue {
    const EVENT_TYPE: &'static str = "middleware_hook_binding.effect_value";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HookOutput {
    value: u64,
    path: String,
}

impl TypedPayload for HookOutput {
    const EVENT_TYPE: &'static str = "middleware_hook_binding.output";
}

#[derive(Clone, Debug)]
struct HookSource {
    next: u64,
    writer_id: WriterId,
}

impl HookSource {
    fn new() -> Self {
        Self {
            next: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for HookSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next > 2 {
            return Ok(None);
        }
        let value = self.next;
        self.next += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            HookInput::EVENT_TYPE,
            json!(HookInput { value }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct HookEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl RuntimeEffect for HookEffect {
    const EFFECT_TYPE: &'static str = "middleware_hook_binding.effect";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = HookEffectValue;

    fn label(&self) -> &str {
        "hook_effect"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(HookEffectValue {
            value: self.value + 100,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("hook-effect:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct HookTransform {
    effect_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for HookTransform {
    type Input = HookInput;

    async fn process(&self, input: HookInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let result = fx
            .perform(HookEffect {
                value: input.value,
                calls: self.effect_calls.clone(),
            })
            .await;

        let path = match result {
            Ok(value) => format!("effect:{}", value.value),
            Err(err) => err.semantic_reason().into_owned(),
        };

        fx.emit(HookOutput {
            value: input.value,
            path,
        })
        .await
        .map_err(|err| HandlerError::Other(err.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "middleware-hook-binding-transform-v1"
    }
}

type Delivered = Arc<Mutex<Vec<(HookOutput, DeliveryProvenance)>>>;

fn sink_probe(
    delivered: &Delivered,
) -> impl FnMut(HookOutput, DeliveryContext) -> std::future::Ready<()> + Send + Sync + Clone {
    let delivered = delivered.clone();
    move |output, delivery| {
        delivered
            .lock()
            .expect("delivery probe lock poisoned")
            .push((output, delivery.provenance()));
        std::future::ready(())
    }
}

fn build_flow(
    journal_base: PathBuf,
    counters: Arc<HookCounters>,
    effect_calls: Arc<AtomicUsize>,
    delivered: &Delivered,
) -> FlowDefinition {
    let probe = sink_probe(delivered);
    flow! {
        name: "middleware_hook_binding_e2e",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            input = source!(HookInput => HookSource::new(), [
                HookProofFactory::new(counters.clone(), 1)
            ]);
            transform = effectful_transform!(
                HookInput -> { HookOutput, HookEffectValue } => HookTransform { effect_calls },
                effects: [HookEffect],
                middleware: [
                    HookProofFactory::new(counters.clone(), 1)
                ]
            );
            output = sink!(HookOutput => SinkTyped::with_delivery(probe), [
                HookProofFactory::new(counters.clone(), 1)
            ]);
        },

        topology: {
            input |> transform;
            transform |> output;
        }
    }
}

fn delivered_payloads(delivered: &Delivered, provenance: DeliveryProvenance) -> Vec<HookOutput> {
    let values = delivered
        .lock()
        .expect("delivery probe lock poisoned")
        .clone();
    assert!(
        !values.is_empty(),
        "expected sink delivery probe to receive output"
    );
    for (_, actual) in &values {
        assert_eq!(*actual, provenance);
    }
    values.into_iter().map(|(output, _)| output).collect()
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries.pop().expect("run should have produced an archive")
}

#[tokio::test]
async fn third_party_control_middleware_binds_all_surfaces_and_replays() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_counters = Arc::new(HookCounters::default());
    let live_effect_calls = Arc::new(AtomicUsize::new(0));
    let live_delivered = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_counters.clone(),
            live_effect_calls.clone(),
            &live_delivered,
        ))
        .await
        .expect("live flow should complete");

    assert_eq!(live_counters.materialized_surfaces(), (1, 1, 1));
    assert!(
        live_counters.source_admits.load(Ordering::SeqCst) > 0,
        "source-poll hook should run in the live flow"
    );
    assert_eq!(live_counters.effect_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(live_counters.effect_rejections.load(Ordering::SeqCst), 1);
    assert_eq!(live_counters.effect_admits.load(Ordering::SeqCst), 1);
    assert_eq!(live_effect_calls.load(Ordering::SeqCst), 1);
    assert_eq!(live_counters.sink_admits.load(Ordering::SeqCst), 2);
    assert_eq!(live_counters.sink_observes.load(Ordering::SeqCst), 2);

    let live_outputs = delivered_payloads(&live_delivered, DeliveryProvenance::Live);
    assert_eq!(
        live_outputs,
        vec![
            HookOutput {
                value: 1,
                path: "hook proof effect rejected".to_string(),
            },
            HookOutput {
                value: 2,
                path: "effect:102".to_string(),
            },
        ]
    );

    let live_run = latest_run_dir(&journal_base);

    let replay_counters = Arc::new(HookCounters::default());
    let replay_effect_calls = Arc::new(AtomicUsize::new(0));
    let replay_delivered = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_run.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            replay_counters.clone(),
            replay_effect_calls.clone(),
            &replay_delivered,
        ))
        .await
        .expect("strict replay should complete");

    assert_eq!(replay_counters.materialized_surfaces(), (1, 1, 1));
    assert_eq!(
        replay_effect_calls.load(Ordering::SeqCst),
        0,
        "strict replay must return recorded effect outcomes without executing the effect"
    );
    assert_eq!(
        replay_counters.effect_attempts.load(Ordering::SeqCst),
        0,
        "strict replay must not invoke the live effect hook chain"
    );
    assert_eq!(
        replay_counters.effect_rejections.load(Ordering::SeqCst),
        0,
        "the third-party rejection is replayed from the archive, not re-decided live"
    );
    // FLOWIP-115b AC48: replay suppression must hold on every delivered surface,
    // not only the effect surface. The source-poll boundary is structurally
    // bypassed by the replay driver, and the sink-delivery boundary is bypassed by
    // the dispatch-scope gate, so no source or sink hook runs during strict replay.
    assert_eq!(
        replay_counters.source_admits.load(Ordering::SeqCst),
        0,
        "strict replay must not run the source-poll hook"
    );
    assert_eq!(
        replay_counters.source_observes.load(Ordering::SeqCst),
        0,
        "strict replay must not observe source polls live"
    );
    assert_eq!(
        replay_counters.sink_admits.load(Ordering::SeqCst),
        0,
        "strict replay must not run the sink-delivery hook"
    );
    assert_eq!(
        replay_counters.sink_observes.load(Ordering::SeqCst),
        0,
        "strict replay must not observe sink deliveries live"
    );

    let replay_outputs = delivered_payloads(&replay_delivered, DeliveryProvenance::Replayed);
    assert_eq!(replay_outputs, live_outputs);
}

#[test]
fn hook_proof_factory_validates_surface_and_protected_unit_identity() {
    let counters = Arc::new(HookCounters::default());
    let factory = HookProofFactory::new(counters, 1);
    let config = StageConfig {
        stage_id: StageId::new(),
        name: "validation_probe".to_string(),
        flow_name: "middleware_hook_binding_e2e".to_string(),
        cycle_guard: None,
    };
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id: config.stage_id,
        effect_type: EffectTypeKey::from("expected"),
    });
    let mismatched_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::Effect(EffectUnitId {
            effect_type: EffectTypeKey::from("actual"),
        }),
    };
    let origin = MiddlewareOrigin::Stage;
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &mismatched_unit,
        origin: &origin,
    };

    assert!(validate_attachment_request(&factory.declaration(), &request).is_err());

    let sink_surface = MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
        stage_id: config.stage_id,
        configured_target: None,
    });
    let sink_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
            target: SinkDeliveryTarget::Stage,
        }),
    };
    let sink_request = MiddlewareAttachmentRequest {
        surface: &sink_surface,
        protected_unit: &sink_unit,
        origin: &origin,
    };
    assert!(validate_attachment_request(&factory.declaration(), &sink_request).is_ok());

    let source_surface = MiddlewareSurface::SourcePoll(SourcePollSurface {
        stage_id: config.stage_id,
    });
    let source_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
    };
    let source_request = MiddlewareAttachmentRequest {
        surface: &source_surface,
        protected_unit: &source_unit,
        origin: &origin,
    };
    assert!(validate_attachment_request(&factory.declaration(), &source_request).is_ok());
}
