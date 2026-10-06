// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115b: middleware hook binder.
//!
//! The binder is the only layer that sees both the adapter-owned checked
//! attachment carrier and the runtime/infra neutral boundary seams. It calls
//! the adapter-owned checked materialisation gateway and hands only neutral
//! seams inward (a composed source boundary, a completion gate).

use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    materialize_factory_checked, materialize_factory_checked_with_declaration,
    CheckedMiddlewareSurfaceAttachment, EffectPolicyAttachment, EffectSurface, EffectTypeKey,
    EffectUnitId, HostedIngressTargetKey, IngressRouteScope, IngressSurface, IngressUnitId,
    MiddlewareAttachmentRequest, MiddlewareAttachmentSite, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareSurface, MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId,
    SinkDeliverySurface, SinkDeliveryTarget, SinkDeliveryUnitId, SinkPolicy, SourcePolicy,
    SourcePollSurface, SourcePollUnitId, SourceStageIngressOwner,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::ingress::IngressBoundaryMiddleware;
use obzenflow_core::ingress::IngressKey;
use obzenflow_core::StageKey;
use obzenflow_runtime::effects::EffectDeclaration;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::source::strategies::CompletionGate;
use std::sync::Arc;

/// Preflight uses the same typed surface and protected unit as materialisation.
/// It runs over every stage before any factory or connector can open.
#[allow(clippy::too_many_arguments)]
pub(crate) fn validate_factory_configuration(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    kind: MiddlewareSurfaceKind,
    effect: Option<&EffectDeclaration>,
    ingress: Option<&IngressKey>,
    authored_site: MiddlewareAttachmentSite,
) -> Result<(), crate::dsl::error::MiddlewarePlanError> {
    use crate::dsl::error::MiddlewarePlanError;
    let effect_name = effect.map(EffectDeclaration::effect_type);
    with_binding_request(
        factory.label(),
        config,
        kind,
        effect,
        ingress,
        authored_site,
        |request| {
            obzenflow_adapters::middleware::validate_attachment_request(
                &factory.declaration(),
                &request,
            )
            .map_err(|source| MiddlewarePlanError::InvalidBinding {
                stage: config.name.clone(),
                effect: MiddlewarePlanError::effect_context(effect_name),
                source,
            })?;
            factory
                .validate_configuration(request, config, stage_type)
                .map_err(|source| MiddlewarePlanError::InvalidConfiguration {
                    stage: config.name.clone(),
                    effect: MiddlewarePlanError::effect_context(effect_name),
                    source,
                })
        },
    )?
}

fn with_binding_request<T>(
    label: &str,
    config: &StageConfig,
    kind: MiddlewareSurfaceKind,
    effect: Option<&EffectDeclaration>,
    ingress: Option<&IngressKey>,
    authored_site: MiddlewareAttachmentSite,
    visit: impl FnOnce(MiddlewareAttachmentRequest<'_>) -> T,
) -> Result<T, crate::dsl::error::MiddlewarePlanError> {
    use crate::dsl::error::MiddlewarePlanError;
    let effect_name = effect.map(EffectDeclaration::effect_type);
    let (surface, unit) = match kind {
        MiddlewareSurfaceKind::SourcePoll => (
            MiddlewareSurface::SourcePoll(SourcePollSurface {
                stage_id: config.stage_id,
            }),
            ProtectedUnit::SourcePoll(SourcePollUnitId),
        ),
        MiddlewareSurfaceKind::SinkDelivery => (
            MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
                stage_id: config.stage_id,
                configured_target: None,
            }),
            ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
                target: SinkDeliveryTarget::Stage,
            }),
        ),
        MiddlewareSurfaceKind::Effect => {
            let effect = effect.ok_or_else(|| {
                MiddlewarePlanError::invalid(
                    &config.name,
                    effect_name,
                    format!("no effect subject for '{}'", label),
                )
            })?;
            (
                MiddlewareSurface::Effect(EffectSurface {
                    stage_id: config.stage_id,
                    effect_type: EffectTypeKey::from(effect.effect_type()),
                    safety: effect.safety(),
                }),
                ProtectedUnit::Effect(EffectUnitId {
                    effect_type: EffectTypeKey::from(effect.effect_type()),
                }),
            )
        }
        MiddlewareSurfaceKind::Ingress => {
            let ingress = ingress.ok_or_else(|| {
                MiddlewarePlanError::invalid(
                    &config.name,
                    effect_name,
                    format!("'{}' requires a hosted source", label),
                )
            })?;
            let stage_key = StageKey(config.name.clone());
            let target = HostedIngressTargetKey {
                surface: ingress.clone(),
                scope: IngressRouteScope::Admission,
            };
            (
                MiddlewareSurface::Ingress(IngressSurface {
                    owner: SourceStageIngressOwner {
                        stage_id: config.stage_id,
                        stage_key: stage_key.clone(),
                    },
                    target: target.clone(),
                }),
                ProtectedUnit::Ingress(IngressUnitId {
                    source_stage_key: stage_key,
                    target,
                }),
            )
        }
        MiddlewareSurfaceKind::Handler => (
            MiddlewareSurface::Handler {
                stage_id: config.stage_id,
            },
            ProtectedUnit::Handler,
        ),
        MiddlewareSurfaceKind::Stateful => (
            MiddlewareSurface::Stateful {
                stage_id: config.stage_id,
            },
            ProtectedUnit::Stateful,
        ),
        MiddlewareSurfaceKind::Join => (
            MiddlewareSurface::Join {
                stage_id: config.stage_id,
            },
            ProtectedUnit::Join,
        ),
        MiddlewareSurfaceKind::StageLifecycle => (
            MiddlewareSurface::StageLifecycle {
                stage_id: config.stage_id,
            },
            ProtectedUnit::StageLifecycle,
        ),
    };
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit,
    };
    let request = MiddlewareAttachmentRequest {
        stage_key: &config.name,
        surface: &surface,
        protected_unit: &protected_unit,
        authored_site,
    };
    Ok(visit(request))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn attachment_key(
    declaration: &MiddlewareDeclaration,
    config: &StageConfig,
    kind: MiddlewareSurfaceKind,
    effect: Option<&EffectDeclaration>,
    ingress: Option<&IngressKey>,
    authored_site: MiddlewareAttachmentSite,
) -> Result<obzenflow_topology::MiddlewareAttachmentKey, crate::dsl::error::MiddlewarePlanError> {
    with_binding_request(
        declaration.label,
        config,
        kind,
        effect,
        ingress,
        authored_site,
        |request| {
            obzenflow_topology::MiddlewareAttachmentKey::from_bytes(
                obzenflow_adapters::middleware::MiddlewareAttachmentId::from_declaration_and_request(
                    declaration,
                    &request,
                )
                .as_ulid()
                .to_bytes(),
            )
        },
    )
}

/// The pieces destructured from one control middleware's `SourcePoll`
/// attachment: the composable source policy and the optional completion-gate
/// companion.
pub(crate) struct SourcePollBinding {
    pub policy: Arc<dyn SourcePolicy>,
    pub completion_gate: Option<Arc<dyn CompletionGate>>,
}

/// A factory paired with the exact declaration that passed the DSL's
/// complete-set structural validation. Keeping the pair together prevents a
/// stateful factory from changing its sealed claim before materialisation.
pub(crate) struct DeclaredMiddlewareFactory<'a> {
    factory: &'a dyn MiddlewareFactory,
    declaration: &'a MiddlewareDeclaration,
}

impl<'a> DeclaredMiddlewareFactory<'a> {
    pub(crate) fn new(
        factory: &'a dyn MiddlewareFactory,
        declaration: &'a MiddlewareDeclaration,
    ) -> Self {
        Self {
            factory,
            declaration,
        }
    }
}

/// Materialize one hook-bound control middleware onto the source-poll surface,
/// returning the neutral pieces the descriptor wires into source runtime config.
pub(crate) fn materialize_source_poll(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    authored_site: MiddlewareAttachmentSite,
) -> Result<SourcePollBinding, String> {
    let surface = MiddlewareSurface::SourcePoll(SourcePollSurface {
        stage_id: config.stage_id,
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
    };
    let request = MiddlewareAttachmentRequest {
        stage_key: &config.name,
        surface: &surface,
        protected_unit: &protected_unit,
        authored_site,
    };
    match materialize_factory_checked(factory, request, config, stage_type, control_middleware)
        .map_err(|error| error.to_string())?
        .into_source_poll()
    {
        Some(attachment) => Ok(SourcePollBinding {
            policy: attachment.policy,
            completion_gate: attachment.completion_gate,
        }),
        None => Err(format!(
            "binder expected a SourcePoll attachment from middleware '{}'",
            factory.label()
        )),
    }
}

/// Build the per-effect policy for one declared effect, in declared order.
///
/// A hook-bound control middleware is materialized onto the `Effect` surface.
/// There is no generic middleware-chain fallback.
pub(crate) fn bind_effect_policy(
    declared_factory: DeclaredMiddlewareFactory<'_>,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    effect: &EffectDeclaration,
    authored_site: MiddlewareAttachmentSite,
) -> Result<EffectPolicyAttachment, String> {
    let factory = declared_factory.factory;
    let declaration = declared_factory.declaration;
    let effect_type = effect.effect_type();
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id: config.stage_id,
        effect_type: EffectTypeKey::from(effect_type),
        safety: effect.safety(),
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::Effect(EffectUnitId {
            effect_type: EffectTypeKey::from(effect_type),
        }),
    };
    let request = MiddlewareAttachmentRequest {
        stage_key: &config.name,
        surface: &surface,
        protected_unit: &protected_unit,
        authored_site,
    };
    match materialize_factory_checked_with_declaration(
        factory,
        declaration,
        request,
        config,
        stage_type,
        control_middleware,
    )
    .map_err(|error| error.to_string())?
    .into_effect()
    {
        Some(policy) => Ok(policy),
        None => Err(format!(
            "binder expected an Effect attachment from middleware '{}'",
            factory.label()
        )),
    }
}

pub(crate) fn materialize_effect_observer(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    effect: &EffectDeclaration,
    authored_site: MiddlewareAttachmentSite,
) -> Result<CheckedMiddlewareSurfaceAttachment, String> {
    let effect_type = effect.effect_type();
    let surface = MiddlewareSurface::Effect(EffectSurface {
        stage_id: config.stage_id,
        effect_type: EffectTypeKey::from(effect_type),
        safety: effect.safety(),
    });
    let protected_unit = ProtectedUnitId {
        stage_id: config.stage_id,
        unit: ProtectedUnit::Effect(EffectUnitId {
            effect_type: EffectTypeKey::from(effect_type),
        }),
    };
    let request = MiddlewareAttachmentRequest {
        stage_key: &config.name,
        surface: &surface,
        protected_unit: &protected_unit,
        authored_site,
    };
    materialize_factory_checked(factory, request, config, stage_type, control_middleware)
        .map_err(|error| error.to_string())
}

pub(crate) fn materialize_observer(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    surface_kind: MiddlewareSurfaceKind,
    authored_site: MiddlewareAttachmentSite,
) -> Result<CheckedMiddlewareSurfaceAttachment, String> {
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
            MiddlewareSurfaceKind::StageLifecycle => ProtectedUnit::StageLifecycle,
            MiddlewareSurfaceKind::Effect | MiddlewareSurfaceKind::Ingress => unreachable!(),
        },
    };
    let request = MiddlewareAttachmentRequest {
        stage_key: &config.name,
        surface: &surface,
        protected_unit: &protected_unit,
        authored_site,
    };
    materialize_factory_checked(factory, request, config, stage_type, control_middleware)
        .map_err(|error| error.to_string())
}

/// Materialize one hook-bound control middleware onto the sink-delivery surface,
/// returning the composable sink policy.
pub(crate) fn materialize_sink_delivery(
    factory: &dyn MiddlewareFactory,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
    authored_site: MiddlewareAttachmentSite,
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
        stage_key: &config.name,
        surface: &surface,
        protected_unit: &protected_unit,
        authored_site,
    };
    match materialize_factory_checked(factory, request, config, stage_type, control_middleware)
        .map_err(|error| error.to_string())?
        .into_sink_delivery()
    {
        Some(policy) => Ok(policy),
        None => Err(format!(
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
    authored_site: MiddlewareAttachmentSite,
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
        stage_key: &config.name,
        surface: &surface,
        protected_unit: &protected_unit,
        authored_site,
    };
    match materialize_factory_checked(factory, request, config, stage_type, control_middleware)
        .map_err(|error| error.to_string())?
        .into_ingress()
    {
        Some(boundary) => Ok(boundary),
        None => Err(format!(
            "binder expected an Ingress attachment from middleware '{}'",
            factory.label()
        )),
    }
}
