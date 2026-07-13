// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115b: middleware hook binder.
//!
//! The binder is the only layer that sees both the adapter-owned carrier
//! (`MiddlewareSurfaceAttachment`, `MiddlewareOrigin`, ...) and the
//! runtime/infra neutral boundary seams. It maps DSL resolution provenance into
//! the adapter-owned origin, calls `MiddlewareFactory::materialize`, and hands
//! only neutral seams inward (a composed source boundary, a completion gate).

use crate::middleware_resolution::MiddlewareSource;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    effect_policy_from_middleware, validate_attachment_request, EffectPolicyAttachment,
    EffectSurface, EffectTypeKey, EffectUnitId, HostedIngressTargetKey, IngressRouteScope,
    IngressSurface, IngressUnitId, Middleware, MiddlewareAttachmentRequest,
    MiddlewareDeclarationIndex, MiddlewareFactory, MiddlewareMaterializationContext,
    MiddlewareOrigin, MiddlewareSurface, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
    ProtectedUnit, ProtectedUnitId, SinkDeliverySurface, SinkDeliveryTarget, SinkDeliveryUnitId,
    SinkPolicy, SourcePolicy, SourcePollSurface, SourcePollUnitId, SourceStageIngressOwner,
    TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::ingress::IngressBoundaryMiddleware;
use obzenflow_core::ingress::IngressKey;
use obzenflow_core::StageKey;
use obzenflow_runtime::effects::{EffectDeclaration, EffectSafety, SinkDeliverySafety};
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::handlers::{
    SourcePollRetryOwnership, SourcePollRetrySafety,
};
use obzenflow_runtime::stages::source::strategies::CompletionGate;
use std::sync::Arc;

/// Whether the raw source handler executes a blocking or cancellable async poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourcePollExecutionKind {
    Synchronous,
    Asynchronous,
}

/// Retry evidence captured from the concrete source handler before middleware
/// wrapping erases its trait methods.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SourcePollRetryEvidence {
    pub execution_kind: SourcePollExecutionKind,
    pub safety: Option<SourcePollRetrySafety>,
    pub ownership: SourcePollRetryOwnership,
    /// The declaration originates at an adapter request decoder. This affects
    /// only the remediation shown for an absent proof; it cannot grant retry.
    pub request_level_safety: bool,
}

impl SourcePollRetryEvidence {
    pub(crate) const fn synchronous() -> Self {
        Self {
            execution_kind: SourcePollExecutionKind::Synchronous,
            safety: None,
            ownership: SourcePollRetryOwnership::NoNestedRetry,
            request_level_safety: false,
        }
    }

    pub(crate) const fn asynchronous(
        safety: Option<SourcePollRetrySafety>,
        ownership: SourcePollRetryOwnership,
    ) -> Self {
        Self {
            execution_kind: SourcePollExecutionKind::Asynchronous,
            safety,
            ownership,
            request_level_safety: false,
        }
    }

    pub(crate) const fn asynchronous_request_level(
        safety: Option<SourcePollRetrySafety>,
        ownership: SourcePollRetryOwnership,
    ) -> Self {
        Self {
            execution_kind: SourcePollExecutionKind::Asynchronous,
            safety,
            ownership,
            request_level_safety: true,
        }
    }
}

pub(crate) fn is_retry_enabled_circuit_breaker(factory: &dyn MiddlewareFactory) -> bool {
    factory.topology_config_slot() == Some(TopologyMiddlewareConfigSlot::CircuitBreaker)
        && factory.hints().retry.is_some()
}

pub(crate) fn reject_handler_shell_retry(factory: &dyn MiddlewareFactory) -> Result<(), String> {
    if is_retry_enabled_circuit_breaker(factory) {
        return Err(
            "circuit_breaker retry requires a source-poll, declared-effect, or sink-delivery attachment; handler-shell retry is not supported"
                .to_string(),
        );
    }
    Ok(())
}

pub(crate) fn register_retry_enabled_breaker(
    factory: &dyn MiddlewareFactory,
    already_seen: &mut bool,
    surface: &str,
    protected_unit: &str,
) -> Result<(), String> {
    if !is_retry_enabled_circuit_breaker(factory) {
        return Ok(());
    }
    if *already_seen {
        return Err(format!(
            "circuit_breaker retry rejected for {surface} '{protected_unit}': multiple retry-enabled circuit breakers resolve to this protected unit; keep exactly one recovery owner"
        ));
    }
    *already_seen = true;
    Ok(())
}

fn validate_source_retry_attachment(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    evidence: SourcePollRetryEvidence,
) -> Result<(), String> {
    if !is_retry_enabled_circuit_breaker(factory) {
        return Ok(());
    }

    let (sync_trait, async_trait) = match stage_type {
        StageType::FiniteSource => ("FiniteSourceHandler", "AsyncFiniteSourceHandler"),
        StageType::InfiniteSource => ("InfiniteSourceHandler", "AsyncInfiniteSourceHandler"),
        _ => ("SourceHandler", "AsyncSourceHandler"),
    };

    if evidence.execution_kind == SourcePollExecutionKind::Synchronous {
        return Err(format!(
            "circuit_breaker retry rejected for synchronous source '{}': max_total_wall_time and force-abort cannot pre-empt {sync_trait}::next(); use an async source with a poll timeout, or keep retry inside the handler",
            config.name
        ));
    }

    if let SourcePollRetryOwnership::HandlerOwned { owner } = evidence.ownership {
        let remedy = if owner == "HttpRetryConfig" {
            "use HttpRetryConfig::disabled() so the circuit-breaker boundary is the single retry owner".to_string()
        } else {
            format!(
                "disable handler-owned retry '{owner}' so the circuit-breaker boundary is the single retry owner"
            )
        };
        return Err(format!(
            "circuit_breaker retry rejected for source-poll '{}': observed SourcePollRetryOwnership::HandlerOwned {{ owner: \"{owner}\" }}; {remedy}",
            config.name
        ));
    }

    match evidence.safety {
        None if evidence.request_level_safety => Err(format!(
            "circuit_breaker retry rejected for source-poll '{}': observed retry safety 'undeclared' because PullDecoder does not declare every generated request repeatable; call retry_safe_requests() after verifying the request contract, implement PullDecoder::request_retry_safety(), or remove retry",
            config.name
        )),
        None => Err(format!(
            "circuit_breaker retry rejected for source-poll '{}': retry safety is undeclared; implement {async_trait}::poll_retry_safety() returning Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation), or remove breaker retry",
            config.name
        )),
        Some(SourcePollRetrySafety::NonRetrySafe) => Err(format!(
            "circuit_breaker retry rejected for source-poll '{}': retry safety is SourcePollRetrySafety::NonRetrySafe; remove breaker retry or provide a different source whose poll is repeatable after error and cancellation",
            config.name
        )),
        Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation) => Ok(()),
    }
}

fn validate_effect_retry_attachment(
    factory: &dyn MiddlewareFactory,
    declaration: &EffectDeclaration,
) -> Result<(), String> {
    if !is_retry_enabled_circuit_breaker(factory) {
        return Ok(());
    }

    match declaration.safety {
        EffectSafety::Idempotent | EffectSafety::NonIdempotentRequiresKey => Ok(()),
        EffectSafety::Transactional => Err(format!(
            "circuit_breaker retry rejected for effect '{}': EffectSafety::Transactional commits inside the protected attempt and has no rollback-proof retry contract; remove retry or use an idempotent/keyed effect",
            declaration.effect_type
        )),
    }
}

fn validate_sink_retry_attachment(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    safety: Option<SinkDeliverySafety>,
) -> Result<(), String> {
    if !is_retry_enabled_circuit_breaker(factory) {
        return Ok(());
    }

    match safety {
        None => Err(format!(
            "circuit_breaker retry rejected for sink-delivery '{}': delivery safety is undeclared; add delivery: idempotent, implement SinkHandler::delivery_safety(), or remove breaker retry",
            config.name
        )),
        Some(SinkDeliverySafety::NonIdempotentExternal) => Err(format!(
            "circuit_breaker retry rejected for sink-delivery '{}': delivery safety is SinkDeliverySafety::NonIdempotentExternal; declare idempotent delivery only if repeated delivery is absorbed, or remove breaker retry",
            config.name
        )),
        Some(SinkDeliverySafety::IdempotentProjection) => Ok(()),
    }
}

/// Map the DSL's middleware resolution provenance into the adapter-owned
/// `MiddlewareOrigin`, dropping the DSL-only `overrode_config` detail so adapter
/// APIs never depend on DSL resolution types.
pub(crate) fn middleware_origin_from_source(source: &MiddlewareSource) -> MiddlewareOrigin {
    match source {
        MiddlewareSource::Flow => MiddlewareOrigin::Flow,
        MiddlewareSource::Stage => MiddlewareOrigin::Stage,
        MiddlewareSource::StageOverride {
            family_label,
            flow_label,
            stage_label,
            ..
        } => MiddlewareOrigin::StageOverride {
            family_label: family_label.clone(),
            flow_label: flow_label.clone(),
            stage_label: stage_label.clone(),
        },
    }
}

/// The pieces destructured from one control middleware's `SourcePoll`
/// attachment: the composable source policy and the optional completion-gate
/// companion.
pub(crate) struct SourcePollBinding {
    pub policy: Arc<dyn SourcePolicy>,
    pub completion_gate: Option<Arc<dyn CompletionGate>>,
}

/// Materialize one hook-bound control middleware onto the source-poll surface,
/// returning the neutral pieces the descriptor wires into source runtime config.
pub(crate) fn materialize_source_poll(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    retry_evidence: SourcePollRetryEvidence,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    origin: &MiddlewareOrigin,
    declaration_index: MiddlewareDeclarationIndex,
) -> Result<SourcePollBinding, String> {
    validate_source_retry_attachment(factory, config, stage_type, retry_evidence)?;
    let surface = MiddlewareSurface::SourcePoll(SourcePollSurface {
        stage_id: config.stage_id,
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
    };
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin,
        declaration_index,
    };
    let declaration = factory.declaration();
    validate_attachment_request(&declaration, &request).map_err(|e| e.to_string())?;
    let ctx = MiddlewareMaterializationContext {
        config,
        control_middleware,
        stage_type,
    };
    match factory
        .materialize(request, &ctx)
        .map_err(|e| e.to_string())?
    {
        MiddlewareSurfaceAttachment::SourcePoll(attachment) => Ok(SourcePollBinding {
            policy: attachment.policy,
            completion_gate: attachment.completion_gate,
        }),
        _ => Err(format!(
            "binder expected a SourcePoll attachment from middleware '{}'",
            factory.label()
        )),
    }
}

/// Build the per-effect policy for one declared effect, in declared order.
///
/// A hook-bound control middleware (the circuit breaker) is materialized onto
/// the `Effect` surface; any other middleware is adapted through the legacy
/// chain surface so a still-legacy policy (the rate limiter) composes alongside.
pub(crate) fn bind_effect_policy(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    effect_declaration: &EffectDeclaration,
    origin: &MiddlewareOrigin,
    declaration_index: MiddlewareDeclarationIndex,
) -> Result<EffectPolicyAttachment, String> {
    validate_effect_retry_attachment(factory, effect_declaration)?;
    let effect_type = effect_declaration.effect_type;
    let declaration = factory.declaration();
    if declaration.is_observer() {
        return Err(format!(
            "observer middleware '{}' cannot be materialized as an effect policy",
            factory.label()
        ));
    }
    if declaration.is_control() && declaration.supports(MiddlewareSurfaceKind::Effect) {
        let surface = MiddlewareSurface::Effect(EffectSurface {
            stage_id: config.stage_id,
            effect_type: EffectTypeKey::from(effect_type),
        });
        let protected_unit = ProtectedUnitId {
            stage_id: config.stage_id,
            unit: ProtectedUnit::Effect(EffectUnitId {
                effect_type: EffectTypeKey::from(effect_type),
            }),
        };
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            origin,
            declaration_index,
        };
        validate_attachment_request(&declaration, &request).map_err(|e| e.to_string())?;
        let ctx = MiddlewareMaterializationContext {
            config,
            control_middleware,
            stage_type,
        };
        match factory
            .materialize(request, &ctx)
            .map_err(|e| e.to_string())?
        {
            MiddlewareSurfaceAttachment::Effect(policy) => Ok(policy),
            _ => Err(format!(
                "binder expected an Effect attachment from middleware '{}'",
                factory.label()
            )),
        }
    } else {
        let instance: Arc<dyn Middleware> = Arc::from(
            factory
                .create_for_effect(config, control_middleware.clone(), effect_type)
                .map_err(|e| e.to_string())?,
        );
        Ok(effect_policy_from_middleware(instance))
    }
}

pub(crate) fn materialize_effect_observer(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    effect_type: &'static str,
    origin: &MiddlewareOrigin,
    declaration_index: MiddlewareDeclarationIndex,
) -> Result<MiddlewareSurfaceAttachment, String> {
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id: config.stage_id,
        effect_type: EffectTypeKey::from(effect_type),
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::Effect(EffectUnitId {
            effect_type: EffectTypeKey::from(effect_type),
        }),
    };
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin,
        declaration_index,
    };
    let declaration = factory.declaration();
    validate_attachment_request(&declaration, &request).map_err(|e| e.to_string())?;
    let ctx = MiddlewareMaterializationContext {
        config,
        control_middleware,
        stage_type,
    };
    match factory
        .materialize(request, &ctx)
        .map_err(|e| e.to_string())?
    {
        attachment @ MiddlewareSurfaceAttachment::EffectObserver(_) => Ok(attachment),
        _ => Err(format!(
            "binder expected an EffectObserver attachment from middleware '{}'",
            factory.label()
        )),
    }
}

pub(crate) fn materialize_observer(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    surface_kind: MiddlewareSurfaceKind,
    origin: &MiddlewareOrigin,
    declaration_index: MiddlewareDeclarationIndex,
) -> Result<MiddlewareSurfaceAttachment, String> {
    let surface = match surface_kind {
        MiddlewareSurfaceKind::SourcePoll => MiddlewareSurface::SourcePoll(SourcePollSurface {
            stage_id: config.stage_id,
        }),
        MiddlewareSurfaceKind::SinkDelivery => {
            MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
                stage_id: config.stage_id,
                configured_target: None,
            })
        }
        MiddlewareSurfaceKind::Handler => MiddlewareSurface::Handler {
            stage_id: config.stage_id,
        },
        MiddlewareSurfaceKind::Stateful => MiddlewareSurface::Stateful {
            stage_id: config.stage_id,
        },
        MiddlewareSurfaceKind::Join => MiddlewareSurface::Join {
            stage_id: config.stage_id,
        },
        MiddlewareSurfaceKind::OutputCommit => MiddlewareSurface::OutputCommit {
            stage_id: config.stage_id,
        },
        MiddlewareSurfaceKind::StageLifecycle => MiddlewareSurface::StageLifecycle {
            stage_id: config.stage_id,
        },
        MiddlewareSurfaceKind::Effect | MiddlewareSurfaceKind::Ingress => {
            return Err(format!(
                "observer middleware '{}' requires a specialized {:?} binding",
                factory.label(),
                surface_kind
            ));
        }
    };
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: match surface_kind {
            MiddlewareSurfaceKind::SourcePoll => ProtectedUnit::SourcePoll(SourcePollUnitId),
            MiddlewareSurfaceKind::SinkDelivery => {
                ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
                    target: SinkDeliveryTarget::Stage,
                })
            }
            MiddlewareSurfaceKind::Handler => ProtectedUnit::Handler,
            MiddlewareSurfaceKind::Stateful => ProtectedUnit::Stateful,
            MiddlewareSurfaceKind::Join => ProtectedUnit::Join,
            MiddlewareSurfaceKind::OutputCommit => ProtectedUnit::OutputCommit,
            MiddlewareSurfaceKind::StageLifecycle => ProtectedUnit::StageLifecycle,
            MiddlewareSurfaceKind::Effect | MiddlewareSurfaceKind::Ingress => unreachable!(),
        },
    };
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin,
        declaration_index,
    };
    let declaration = factory.declaration();
    validate_attachment_request(&declaration, &request).map_err(|e| e.to_string())?;
    let ctx = MiddlewareMaterializationContext {
        config,
        control_middleware,
        stage_type,
    };
    factory
        .materialize(request, &ctx)
        .map_err(|e| e.to_string())
}

/// Materialize one hook-bound control middleware onto the sink-delivery surface,
/// returning the composable sink policy.
pub(crate) fn materialize_sink_delivery(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    delivery_safety: Option<SinkDeliverySafety>,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    origin: &MiddlewareOrigin,
    declaration_index: MiddlewareDeclarationIndex,
) -> Result<Arc<dyn SinkPolicy>, String> {
    validate_sink_retry_attachment(factory, config, delivery_safety)?;
    let surface = MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
        stage_id: config.stage_id,
        configured_target: None,
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
            target: SinkDeliveryTarget::Stage,
        }),
    };
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin,
        declaration_index,
    };
    let declaration = factory.declaration();
    validate_attachment_request(&declaration, &request).map_err(|e| e.to_string())?;
    let ctx = MiddlewareMaterializationContext {
        config,
        control_middleware,
        stage_type,
    };
    match factory
        .materialize(request, &ctx)
        .map_err(|e| e.to_string())?
    {
        MiddlewareSurfaceAttachment::SinkDelivery(policy) => Ok(policy),
        _ => Err(format!(
            "binder expected a SinkDelivery attachment from middleware '{}'",
            factory.label()
        )),
    }
}

/// Materialize one hook-bound control middleware onto the source-backed hosted
/// ingress surface (FLOWIP-115d), returning the neutral core-owned boundary the
/// hosted endpoints call. The ingress identity is source-stage-owned: the owner
/// is the linked source stage (its id plus the replay-stable `StageConfig.name`
/// key), and the hosted target is the protocol-neutral ingress key under the
/// default admission route scope.
pub(crate) fn materialize_ingress(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    ingress_key: &IngressKey,
    origin: &MiddlewareOrigin,
    declaration_index: MiddlewareDeclarationIndex,
) -> Result<Arc<dyn IngressBoundaryMiddleware>, String> {
    let stage_key = StageKey(config.name.clone());
    let target = HostedIngressTargetKey {
        surface: ingress_key.clone(),
        scope: IngressRouteScope::Admission,
    };
    let surface = MiddlewareSurface::Ingress(IngressSurface {
        owner: SourceStageIngressOwner {
            stage_id: config.stage_id,
            stage_key: stage_key.clone(),
        },
        target: target.clone(),
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::Ingress(IngressUnitId {
            source_stage_key: stage_key,
            target,
        }),
    };
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        origin,
        declaration_index,
    };
    let declaration = factory.declaration();
    validate_attachment_request(&declaration, &request).map_err(|e| e.to_string())?;
    let ctx = MiddlewareMaterializationContext {
        config,
        control_middleware,
        stage_type,
    };
    match factory
        .materialize(request, &ctx)
        .map_err(|e| e.to_string())?
    {
        MiddlewareSurfaceAttachment::Ingress(boundary) => Ok(boundary),
        _ => Err(format!(
            "binder expected an Ingress attachment from middleware '{}'",
            factory.label()
        )),
    }
}
