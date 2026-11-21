// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    validate_attachment_request, EffectSurface, EffectTypeKey, EffectUnitId,
    EventAwareEffectPolicy, MiddlewareAttachmentRequest, MiddlewareAttachmentSite,
    MiddlewareContext, MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError,
    MiddlewareFactoryResult, MiddlewareMaterializationContext, MiddlewareOverrideKey,
    MiddlewareSurface, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
    MiddlewareSurfaceKind::Effect, MiddlewareSurfaceKind::SourcePoll, PolicyAdmission,
    ProtectedUnit, ProtectedUnitId, ProtectedUnitOwner, SourceAdmission, SourcePolicy,
    SourcePolicyCtx, SourcePollAttachment, SourcePollOutcome, SourcePollSurface, SourcePollUnitId,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_core::{StageId, TypedPayload};
use obzenflow_runtime::pipeline::config::StageConfig;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct HookCounters {
    source_materialized: AtomicUsize,
    effect_materialized: AtomicUsize,
    source_admits: AtomicUsize,
    source_observes: AtomicUsize,
    effect_attempts: AtomicUsize,
    effect_admits: AtomicUsize,
    effect_rejections: AtomicUsize,
    effect_observes: AtomicUsize,
}

impl HookCounters {}

struct HookProofFamily;

struct HookProofFactory {
    counters: Arc<HookCounters>,
    reject_effect_value: u64,
    intent: MiddlewareSurfaceKind,
}

impl HookProofFactory {
    fn new(
        counters: Arc<HookCounters>,
        reject_effect_value: u64,
        intent: MiddlewareSurfaceKind,
    ) -> Self {
        Self {
            counters,
            reject_effect_value,
            intent,
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

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![SourcePoll, Effect])
            .with_control_intent(self.intent)
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
                Ok(MiddlewareSurfaceAttachment::source_poll(
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
                Ok(MiddlewareSurfaceAttachment::event_aware_effect(Arc::new(
                    HookProofEffectPolicy {
                        counters: self.counters.clone(),
                        reject_value: self.reject_effect_value,
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
impl EventAwareEffectPolicy for HookProofEffectPolicy {
    fn label(&self) -> &'static str {
        "hook_proof_effect"
    }

    async fn admit(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        self.counters.effect_attempts.fetch_add(1, Ordering::SeqCst);
        let value = HookInput::from_event(event)
            .map(|input| input.value)
            .unwrap_or_default();
        if value == self.reject_value {
            self.counters
                .effect_rejections
                .fetch_add(1, Ordering::SeqCst);
            return PolicyAdmission::Reject(Box::new(
                obzenflow_adapters::middleware::MiddlewareAbortCause {
                    source: EffectFailureSource::new("hook_proof_control"),
                    code: EffectFailureCode::new("rejected_by_hook_proof"),
                    message: "hook proof effect rejected".to_string(),
                    retry: RetryDisposition::NotRetryable,
                    event: None,
                },
            ));
        }
        self.counters.effect_admits.fetch_add(1, Ordering::SeqCst);
        PolicyAdmission::Admit
    }

    fn observe(
        &self,
        _event: &ChainEvent,
        _attempt: &obzenflow_adapters::middleware::EffectAttemptOutcome<'_>,
        _ctx: &mut MiddlewareContext,
    ) {
        self.counters.effect_observes.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HookInput {
    value: u64,
}

impl TypedPayload for HookInput {
    const EVENT_TYPE: &'static str = "middleware_hook_binding.input";
}

#[test]
fn hook_proof_factory_validates_surface_and_protected_unit_identity() {
    let counters = Arc::new(HookCounters::default());
    let factory = HookProofFactory::new(counters.clone(), 1, Effect);
    let config = StageConfig {
        stage_id: StageId::new(),
        name: "validation_probe".to_string(),
        flow_name: "middleware_hook_binding_e2e".to_string(),
        cycle_guard: None,
        lineage: obzenflow_core::config::LineagePolicy::default(),
        effective_config: std::sync::Arc::new(
            obzenflow_runtime::runtime_config::FlowEffectiveConfig::default(),
        ),
    };
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id: config.stage_id,
        effect_type: EffectTypeKey::from("expected"),
        safety: obzenflow_runtime::effects::EffectSafety::Idempotent,
    });
    let mismatched_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        owner: ProtectedUnitOwner::Stage,
        unit: ProtectedUnit::Effect(EffectUnitId {
            effect_type: EffectTypeKey::from("actual"),
        }),
    };
    let request = MiddlewareAttachmentRequest {
        stage_key: "test_stage",
        surface: &surface,
        protected_unit: &mismatched_unit,
        authored_site: MiddlewareAttachmentSite::Effect,
    };

    assert!(validate_attachment_request(&factory.declaration(), &request).is_err());

    let source_surface = MiddlewareSurface::SourcePoll(SourcePollSurface {
        stage_id: config.stage_id,
    });
    let source_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        owner: ProtectedUnitOwner::Stage,
        unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
    };
    let source_request = MiddlewareAttachmentRequest {
        stage_key: "test_stage",
        surface: &source_surface,
        protected_unit: &source_unit,
        authored_site: MiddlewareAttachmentSite::Implementation,
    };
    let source_factory = HookProofFactory::new(counters, 1, SourcePoll);
    assert!(validate_attachment_request(&source_factory.declaration(), &source_request).is_ok());
}
