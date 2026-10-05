// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115v: contextual planning of the supported middleware families.
//!
//! This is build-time lowering, never an execution router. Frozen declarations
//! determine the existing boundary that receives each attachment.

use super::error::MiddlewarePlanError;
use super::stage_descriptor::EffectPolicyAttachment;
use obzenflow_adapters::middleware::{
    capture_middleware, MiddlewareAttachmentSite, MiddlewareFactory, MiddlewareSurfaceKind,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::ingress::IngressKey;
use obzenflow_runtime::effects::EffectDeclaration;
use obzenflow_runtime::pipeline::config::StageConfig;
use std::collections::{BTreeMap, HashSet};

type Factories = Vec<Box<dyn MiddlewareFactory>>;
type EffectControls =
    BTreeMap<&'static str, Vec<(MiddlewareAttachmentSite, Box<dyn MiddlewareFactory>)>>;

fn freeze(factories: &mut Factories) {
    *factories = std::mem::take(factories)
        .into_iter()
        .map(capture_middleware)
        .collect();
}

fn sort(factories: &mut Factories) {
    factories.sort_by_key(|factory| {
        let declaration = factory.declaration();
        let order = if declaration.is_rate_limiter() {
            0
        } else if declaration.is_circuit_breaker() {
            1
        } else {
            2
        };
        (order, declaration.family_label, declaration.label)
    });
}

fn observer_labels(
    stage: &str,
    factories: &[Box<dyn MiddlewareFactory>],
) -> Result<(), MiddlewarePlanError> {
    let mut labels = HashSet::new();
    for factory in factories {
        if !labels.insert(factory.declaration().label) {
            return Err(MiddlewarePlanError::invalid(stage, None, format!("stage '{stage}': observer label '{}' occurs twice at the same attachment site; use distinct labels", factory.label())));
        }
    }
    Ok(())
}

pub(super) fn prepare_observers(
    stage: &str,
    stage_type: StageType,
    observers: &mut Factories,
) -> Result<(), MiddlewarePlanError> {
    freeze(observers);
    for factory in observers.iter() {
        if factory.declaration().is_control() {
            return Err(MiddlewarePlanError::invalid(stage, None, format!("stage '{stage}' ({stage_type:?}) has no declared live operation for control '{}'; attach it to a source, sink, or declared effect", factory.label())));
        }
    }
    observer_labels(stage, observers)?;
    sort(observers);
    Ok(())
}

fn control_surface(
    factory: &dyn MiddlewareFactory,
    operation: MiddlewareSurfaceKind,
    hosted: bool,
) -> Result<MiddlewareSurfaceKind, String> {
    let declaration = factory.declaration();
    if declaration.is_rate_limiter() && hosted {
        return Ok(MiddlewareSurfaceKind::Ingress);
    }
    if declaration.is_rate_limiter()
        || declaration.is_circuit_breaker()
        || declaration.is_retry()
        || declaration.is_effect_resilience()
    {
        return Ok(operation);
    }
    let intended = declaration.control_intent().or_else(|| {
        (declaration.surfaces.len() == 1).then(|| declaration.surfaces[0])
    }).ok_or_else(|| format!("custom control '{}' has ambiguous binding intent; its definition must declare the protected operation", declaration.label))?;
    if intended == operation || (hosted && intended == MiddlewareSurfaceKind::Ingress) {
        Ok(intended)
    } else {
        Err(format!(
            "custom control '{}' targets {intended:?}, which this implementation does not expose",
            declaration.label
        ))
    }
}

pub(super) fn prepare_io(
    stage: &str,
    stage_type: StageType,
    hosted: bool,
    controls: &mut Factories,
    ingress: &mut Option<Box<dyn MiddlewareFactory>>,
    observers: &mut Factories,
) -> Result<(), MiddlewarePlanError> {
    let mut all = std::mem::take(controls);
    all.extend(ingress.take());
    all.append(observers);
    freeze(&mut all);
    let operation = if stage_type == StageType::Sink {
        MiddlewareSurfaceKind::SinkDelivery
    } else {
        MiddlewareSurfaceKind::SourcePoll
    };
    let mut families = HashSet::new();
    let mut custom_controls = 0;
    for factory in all {
        let declaration = factory.declaration();
        if declaration.is_observer() {
            observers.push(factory);
            continue;
        }
        let target = control_surface(factory.as_ref(), operation, hosted)
            .map_err(|error| MiddlewarePlanError::invalid(stage, None, error))?;
        if !declaration.supports(target) || declaration.is_retry() {
            return Err(MiddlewarePlanError::invalid(stage, None, format!("stage '{stage}': control '{}' cannot protect {target:?}; retry is supported only on eligible declared effects", declaration.label)));
        }
        if !families.insert((target, declaration.family_label)) {
            return Err(MiddlewarePlanError::invalid(stage, None, format!("stage '{stage}': duplicate '{}' controls resolve to {target:?}; retain one definition", declaration.family_label)));
        }
        if target == MiddlewareSurfaceKind::Ingress {
            if ingress.is_some() {
                return Err(MiddlewarePlanError::invalid(stage, None, format!("stage '{stage}': multiple ingress controls require an ingress composition contract")));
            }
            *ingress = Some(factory);
        } else {
            if !declaration.is_rate_limiter() && !declaration.is_circuit_breaker() {
                custom_controls += 1;
            }
            controls.push(factory);
        }
    }
    if custom_controls != 0 && controls.len() > 1 {
        return Err(MiddlewarePlanError::invalid(stage, None, format!("stage '{stage}': multiple controls including custom middleware require a defined composition contract")));
    }
    observer_labels(stage, observers)?;
    sort(controls);
    sort(observers);
    Ok(())
}

pub(super) fn prepare_effects(
    stage: &str,
    effects: &[EffectDeclaration],
    implementation: &mut Factories,
    attachments: &mut Vec<EffectPolicyAttachment>,
) -> Result<(), MiddlewarePlanError> {
    freeze(implementation);
    let mut observers = Vec::new();
    for factory in std::mem::take(implementation) {
        if factory.declaration().is_observer() {
            observers.push(factory);
            continue;
        }
        if effects.len() != 1 {
            let choices = effects
                .iter()
                .map(EffectDeclaration::effect_type)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(MiddlewarePlanError::invalid(stage, None, format!("stage '{stage}': implementation control '{}' requires exactly one declared effect; attach it to a named effect from [{choices}]", factory.label())));
        }
        attachments.push(EffectPolicyAttachment {
            effect_type: effects[0].effect_type(),
            factory,
            authored_site: MiddlewareAttachmentSite::Implementation,
        });
    }
    observer_labels(stage, &observers)?;
    sort(&mut observers);
    *implementation = observers;
    let mut grouped = EffectControls::new();
    let mut effect_observers = Vec::new();
    let mut labels = HashSet::new();
    for mut attachment in std::mem::take(attachments) {
        if !effects
            .iter()
            .any(|effect| effect.effect_type() == attachment.effect_type)
        {
            return Err(MiddlewarePlanError::invalid(
                stage,
                None,
                format!(
                    "stage '{stage}': middleware targets undeclared effect '{}'",
                    attachment.effect_type
                ),
            ));
        }
        attachment.factory = capture_middleware(attachment.factory);
        let declaration = attachment.factory.declaration();
        if declaration.is_observer() {
            if !declaration.supports(MiddlewareSurfaceKind::Effect) {
                return Err(MiddlewarePlanError::invalid(stage, None, format!("stage '{stage}', effect '{}': observer '{}' has no effect observation point", attachment.effect_type, declaration.label)));
            }
            if !labels.insert((attachment.effect_type, declaration.label)) {
                return Err(MiddlewarePlanError::invalid(stage, None, format!("stage '{stage}', effect '{}': duplicate observer label '{}' at the named effect site", attachment.effect_type, declaration.label)));
            }
            effect_observers.push(attachment);
        } else {
            control_surface(
                attachment.factory.as_ref(),
                MiddlewareSurfaceKind::Effect,
                false,
            )
            .map_err(|error| {
                MiddlewarePlanError::invalid(stage, Some(attachment.effect_type), error)
            })?;
            grouped
                .entry(attachment.effect_type)
                .or_default()
                .push((attachment.authored_site, attachment.factory));
        }
    }
    for (effect_type, factories) in grouped {
        let authored_site = if factories.len() == 1 {
            factories[0].0
        } else {
            MiddlewareAttachmentSite::Effect
        };
        let factory = obzenflow_adapters::middleware::compose_effect_controls(factories).map_err(
            |source| MiddlewarePlanError::InvalidComposition {
                stage: stage.to_owned(),
                effect: effect_type.to_owned(),
                source,
            },
        )?;
        attachments.push(EffectPolicyAttachment {
            effect_type,
            factory: capture_middleware(factory),
            authored_site,
        });
    }
    effect_observers.sort_by_key(|attachment| {
        (
            attachment.effect_type,
            attachment.factory.declaration().family_label,
            attachment.factory.label(),
        )
    });
    attachments.extend(effect_observers);
    Ok(())
}

pub(super) fn validate_configuration(
    stage_type: StageType,
    stage: &str,
    factories: Vec<&dyn MiddlewareFactory>,
    attachments: &[EffectPolicyAttachment],
    effects: &[EffectDeclaration],
    ingress: Option<&IngressKey>,
    config: &StageConfig,
) -> Result<(), MiddlewarePlanError> {
    for factory in factories {
        let declaration = factory.declaration();
        if declaration.is_control() {
            let operation = match stage_type {
                StageType::FiniteSource | StageType::InfiniteSource => {
                    MiddlewareSurfaceKind::SourcePoll
                }
                StageType::Sink => MiddlewareSurfaceKind::SinkDelivery,
                _ => {
                    return Err(MiddlewarePlanError::invalid(
                        stage,
                        None,
                        format!(
                            "stage '{stage}': control '{}' has no protected operation",
                            declaration.label
                        ),
                    ))
                }
            };
            let surface = control_surface(factory, operation, ingress.is_some())
                .map_err(|error| MiddlewarePlanError::invalid(stage, None, error))?;
            super::binder::validate_factory_configuration(
                factory,
                config,
                stage_type,
                surface,
                None,
                ingress,
                MiddlewareAttachmentSite::Implementation,
            )?;
        } else {
            let mut matched = false;
            for surface in
                obzenflow_runtime::stages::observer::observer_shell_surfaces_for_stage(stage_type)
            {
                let kind = super::stage_descriptor::middleware_surface_kind(*surface);
                if declaration.supports(kind) {
                    super::binder::validate_factory_configuration(
                        factory,
                        config,
                        stage_type,
                        kind,
                        None,
                        ingress,
                        MiddlewareAttachmentSite::Implementation,
                    )?;
                    matched = true;
                }
            }
            if declaration.supports(MiddlewareSurfaceKind::Effect) {
                for effect in effects {
                    super::binder::validate_factory_configuration(
                        factory,
                        config,
                        stage_type,
                        MiddlewareSurfaceKind::Effect,
                        Some(effect),
                        ingress,
                        MiddlewareAttachmentSite::Implementation,
                    )?;
                    matched = true;
                }
            }
            if !matched {
                return Err(MiddlewarePlanError::invalid(
                    stage,
                    None,
                    format!(
                        "stage '{stage}': observer '{}' has no matching observation point",
                        declaration.label
                    ),
                ));
            }
        }
    }
    for attachment in attachments {
        let effect = effects
            .iter()
            .find(|effect| effect.effect_type() == attachment.effect_type)
            .ok_or_else(|| {
                MiddlewarePlanError::invalid(
                    stage,
                    Some(attachment.effect_type),
                    "middleware targets an undeclared effect",
                )
            })?;
        super::binder::validate_factory_configuration(
            attachment.factory.as_ref(),
            config,
            stage_type,
            MiddlewareSurfaceKind::Effect,
            Some(effect),
            ingress,
            attachment.authored_site,
        )?;
    }
    Ok(())
}

/// The topology is a projection of the frozen placement and effective settings.
/// The internal effect execution owner does not replace its authored members.
pub(super) fn topology(
    descriptor: &dyn super::stage_descriptor::StageDescriptor,
    config: &StageConfig,
) -> Result<obzenflow_topology::MiddlewareInfo, MiddlewarePlanError> {
    use obzenflow_adapters::middleware::{BuiltinControlFamily, MiddlewareDeclaration};
    use obzenflow_topology::{
        MiddlewareAttachmentInfo, MiddlewareAuthoredSite, MiddlewareFamily, MiddlewareInfo,
        MiddlewareOperation,
    };
    let mut result = MiddlewareInfo::default();
    let effects = descriptor.effect_declarations();
    let ingress = descriptor.hosted_ingress_key();
    let mut project = |factory: &dyn MiddlewareFactory,
                       kind: MiddlewareSurfaceKind,
                       effect: Option<&EffectDeclaration>,
                       site: MiddlewareAttachmentSite|
     -> Result<(), MiddlewarePlanError> {
        let declaration = factory.declaration();
        let contributions = factory.builtin_control();
        let components = contributions
            .as_ref()
            .map(|contribution| {
                contribution
                    .families()
                    .map(|family| {
                        let component_site = contribution
                            .attachment_sites()
                            .iter()
                            .find(|(member, _)| *member == family)
                            .map(|(_, site)| *site)
                            .unwrap_or(site);
                        (Some(family), component_site)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![(None, site)]);
        for (component, component_site) in components {
            let (family, label, member_declaration) = match component {
                Some(member) => {
                    let family = match member {
                        BuiltinControlFamily::RateLimiter => MiddlewareFamily::RateLimiter,
                        BuiltinControlFamily::CircuitBreaker => MiddlewareFamily::CircuitBreaker,
                        BuiltinControlFamily::Retry => MiddlewareFamily::Retry,
                    };
                    (
                        family,
                        member.label(),
                        MiddlewareDeclaration::control_with_family(
                            member.label(),
                            member.label(),
                            vec![kind],
                        ),
                    )
                }
                None => {
                    let family = if declaration.is_observer() {
                        MiddlewareFamily::Observer
                    } else if declaration.is_rate_limiter() {
                        MiddlewareFamily::RateLimiter
                    } else if declaration.is_circuit_breaker() {
                        MiddlewareFamily::CircuitBreaker
                    } else if declaration.is_retry() {
                        MiddlewareFamily::Retry
                    } else {
                        MiddlewareFamily::Custom {
                            name: declaration.family_label.to_string(),
                        }
                    };
                    (family, declaration.label, declaration.clone())
                }
            };
            let operation = match kind {
                MiddlewareSurfaceKind::SourcePoll => MiddlewareOperation::SourcePoll,
                MiddlewareSurfaceKind::Ingress => MiddlewareOperation::Ingress,
                MiddlewareSurfaceKind::SinkDelivery => MiddlewareOperation::SinkDelivery,
                MiddlewareSurfaceKind::Effect => MiddlewareOperation::Effect {
                    effect_type: effect
                        .expect("effect binding has a subject")
                        .effect_type()
                        .to_string(),
                },
                MiddlewareSurfaceKind::Handler => MiddlewareOperation::Handler,
                MiddlewareSurfaceKind::Stateful => MiddlewareOperation::Stateful,
                MiddlewareSurfaceKind::Join => MiddlewareOperation::Join,
                MiddlewareSurfaceKind::StageLifecycle => MiddlewareOperation::Lifecycle,
            };
            let authored_site = match component_site {
                MiddlewareAttachmentSite::Implementation => MiddlewareAuthoredSite::Implementation,
                MiddlewareAttachmentSite::Effect => MiddlewareAuthoredSite::Effect {
                    effect_type: effect
                        .expect("effect site has a subject")
                        .effect_type()
                        .to_string(),
                },
            };
            let mut configuration = serde_json::Map::new();
            let prefix = component.map(|member| format!("middleware.{}.", member.label()));
            for key in factory.consumed_config_keys() {
                if prefix
                    .as_ref()
                    .is_some_and(|prefix| !key.starts_with(prefix))
                {
                    continue;
                }
                let resolved = match effect {
                    Some(effect) => config.effective_config.effect_value(
                        key,
                        &config.name.as_str().into(),
                        &effect.effect_type().into(),
                    ),
                    None => config.effective_config.get(
                        key,
                        &obzenflow_core::config::ConfigScope::stage(config.name.as_str()),
                    ),
                };
                if let Some(resolved) = resolved {
                    configuration.insert(key.to_string(), serde_json::json!({ "value": resolved.value.to_json(), "source": resolved.meta.source.to_string(), "scope": resolved.meta.scope.to_string() }));
                }
            }
            result.attachments.push(MiddlewareAttachmentInfo {
                key: super::binder::attachment_key(
                    &member_declaration,
                    config,
                    kind,
                    effect,
                    ingress.as_ref(),
                    component_site,
                )?,
                label: label.to_string(),
                family,
                authored_site,
                operation,
                configuration: serde_json::Value::Object(configuration),
            });
        }
        Ok(())
    };
    for factory in descriptor.stage_middleware_factories() {
        let declaration = factory.declaration();
        if declaration.is_control() {
            let operation = if descriptor.stage_type() == StageType::Sink {
                MiddlewareSurfaceKind::SinkDelivery
            } else {
                MiddlewareSurfaceKind::SourcePoll
            };
            let kind = control_surface(factory, operation, ingress.is_some())
                .map_err(|error| MiddlewarePlanError::invalid(descriptor.name(), None, error))?;
            project(
                factory,
                kind,
                None,
                MiddlewareAttachmentSite::Implementation,
            )?;
        } else {
            for surface in obzenflow_runtime::stages::observer::observer_shell_surfaces_for_stage(
                descriptor.stage_type(),
            ) {
                let kind = super::stage_descriptor::middleware_surface_kind(*surface);
                if declaration.supports(kind) {
                    project(
                        factory,
                        kind,
                        None,
                        MiddlewareAttachmentSite::Implementation,
                    )?;
                }
            }
            if declaration.supports(MiddlewareSurfaceKind::Effect) {
                for effect in &effects {
                    project(
                        factory,
                        MiddlewareSurfaceKind::Effect,
                        Some(effect),
                        MiddlewareAttachmentSite::Implementation,
                    )?;
                }
            }
        }
    }
    for attachment in descriptor.effect_policy_attachments() {
        let effect = effects
            .iter()
            .find(|effect| effect.effect_type() == attachment.effect_type)
            .expect("validated effect attachment");
        project(
            attachment.factory.as_ref(),
            MiddlewareSurfaceKind::Effect,
            Some(effect),
            attachment.authored_site,
        )?;
    }
    result
        .attachments
        .sort_by(|left, right| left.key.cmp(&right.key));
    Ok(result)
}
