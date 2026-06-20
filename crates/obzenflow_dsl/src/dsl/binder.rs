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
    IngressSurface, IngressUnitId, Middleware,
    MiddlewareAttachmentRequest, MiddlewareDeclarationIndex, MiddlewareFactory,
    MiddlewareMaterializationContext, MiddlewareOrigin, MiddlewareSurface,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId,
    SinkDeliverySurface, SinkDeliveryTarget, SinkDeliveryUnitId, SinkPolicy, SourcePolicy,
    SourcePollSurface, SourcePollUnitId, SourceStageIngressOwner,
};
use obzenflow_core::ingress::IngressKey;
use obzenflow_core::StageKey;
use obzenflow_core::event::context::StageType;
use obzenflow_core::ingress::IngressBoundaryMiddleware;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::source::strategies::CompletionGate;
use std::sync::Arc;

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
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    origin: &MiddlewareOrigin,
    declaration_index: MiddlewareDeclarationIndex,
) -> Result<SourcePollBinding, String> {
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
    effect_type: &'static str,
    origin: &MiddlewareOrigin,
    declaration_index: MiddlewareDeclarationIndex,
) -> Result<EffectPolicyAttachment, String> {
    let declaration = factory.declaration();
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

/// Materialize one hook-bound control middleware onto the sink-delivery surface,
/// returning the composable sink policy.
pub(crate) fn materialize_sink_delivery(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    origin: &MiddlewareOrigin,
    declaration_index: MiddlewareDeclarationIndex,
) -> Result<Arc<dyn SinkPolicy>, String> {
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
