// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sealed built-in contributions and the bounded effect composition catalogue.

use super::circuit_breaker::{circuit_breaker, CircuitBreaker, Retry};
use super::rate_limiter::RateLimiter;
use super::resilience::EffectPlanFactory;
use crate::middleware::{MiddlewareAttachmentSite, MiddlewareFactory};

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BuiltinControlFamily {
    RateLimiter,
    CircuitBreaker,
    Retry,
}

impl BuiltinControlFamily {
    pub fn label(self) -> &'static str {
        match self {
            Self::RateLimiter => "rate_limiter",
            Self::CircuitBreaker => "circuit_breaker",
            Self::Retry => "retry",
        }
    }
}

#[derive(Clone)]
enum Contribution {
    Limiter(RateLimiter),
    Breaker(CircuitBreaker),
    Retry(Retry),
}

/// Only canonical built-in values can construct these authority-bearing facts.
#[doc(hidden)]
#[derive(Clone)]
pub struct BuiltinControlContribution {
    values: Vec<Contribution>,
    sites: Vec<(BuiltinControlFamily, MiddlewareAttachmentSite)>,
    #[cfg(feature = "test-support")]
    reject_affine_recovery_for_test: bool,
}

impl BuiltinControlContribution {
    pub(in crate::middleware::control) fn limiter(value: RateLimiter) -> Self {
        Self::one(Contribution::Limiter(value))
    }
    pub(in crate::middleware::control) fn breaker(value: CircuitBreaker) -> Self {
        Self::one(Contribution::Breaker(value))
    }
    pub(in crate::middleware::control) fn retry(value: Retry) -> Self {
        Self::one(Contribution::Retry(value))
    }
    fn one(value: Contribution) -> Self {
        Self {
            values: vec![value],
            sites: Vec::new(),
            #[cfg(feature = "test-support")]
            reject_affine_recovery_for_test: false,
        }
    }
    pub(in crate::middleware::control) fn from_plan(plan: &EffectPlanFactory) -> Self {
        let mut values = Vec::new();
        if let Some(value) = &plan.rate_limiter {
            values.push(Contribution::Limiter(value.clone()));
        }
        if let Some(value) = &plan.breaker {
            values.push(Contribution::Breaker(value.clone()));
        }
        if let Some(value) = &plan.retry {
            values.push(Contribution::Retry(value.clone()));
        }
        Self {
            values,
            sites: plan.sites.clone(),
            #[cfg(feature = "test-support")]
            reject_affine_recovery_for_test: plan.reject_affine_recovery_for_test,
        }
    }
    /// Original authored coordinates survive the internal execution composition.
    pub fn attachment_sites(&self) -> &[(BuiltinControlFamily, MiddlewareAttachmentSite)] {
        &self.sites
    }
    pub fn families(&self) -> impl ExactSizeIterator<Item = BuiltinControlFamily> + '_ {
        self.values.iter().map(|value| match value {
            Contribution::Limiter(_) => BuiltinControlFamily::RateLimiter,
            Contribution::Breaker(_) => BuiltinControlFamily::CircuitBreaker,
            Contribution::Retry(_) => BuiltinControlFamily::Retry,
        })
    }
    pub fn is_retry(&self) -> bool {
        self.families()
            .any(|family| family == BuiltinControlFamily::Retry)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ControlCompositionError {
    #[error("two {family} definitions resolve to the same operation at {first:?} and {second:?}; retain one definition")]
    Duplicate {
        family: &'static str,
        first: MiddlewareAttachmentSite,
        second: MiddlewareAttachmentSite,
    },
    #[error("control '{label}' cannot compose with another control on this operation; no composition contract exists")]
    Unsupported { label: &'static str },
    #[error("control '{label}' supplied built-in contributions inconsistent with its sealed declaration")]
    AuthorityMismatch { label: &'static str },
    #[error("effect composition requires at least one control")]
    Empty,
}

#[doc(hidden)]
pub fn compose_effect_controls(
    factories: Vec<(MiddlewareAttachmentSite, Box<dyn MiddlewareFactory>)>,
) -> Result<Box<dyn MiddlewareFactory>, ControlCompositionError> {
    if factories.is_empty() {
        return Err(ControlCompositionError::Empty);
    }
    // A standalone custom control retains its existing checked extension route.
    let mut captured = factories
        .into_iter()
        .map(|(site, factory)| {
            let contribution = factory.builtin_control();
            (site, factory, contribution)
        })
        .collect::<Vec<_>>();
    if captured.len() == 1 && captured[0].2.is_none() {
        return Ok(captured.remove(0).1);
    }
    let mut plan = EffectPlanFactory::empty();
    for (site, factory, contribution) in captured {
        let contribution = contribution.ok_or(ControlCompositionError::Unsupported {
            label: factory.label(),
        })?;
        let declaration = factory.declaration();
        let families = contribution.families().collect::<Vec<_>>();
        let authorised = declaration.is_effect_resilience()
            || matches!(families.as_slice(), [BuiltinControlFamily::RateLimiter] if declaration.is_rate_limiter())
            || matches!(families.as_slice(), [BuiltinControlFamily::CircuitBreaker] if declaration.is_circuit_breaker())
            || matches!(families.as_slice(), [BuiltinControlFamily::Retry] if declaration.is_retry());
        if !authorised {
            return Err(ControlCompositionError::AuthorityMismatch {
                label: factory.label(),
            });
        }
        #[cfg(feature = "test-support")]
        {
            plan.reject_affine_recovery_for_test |= contribution.reject_affine_recovery_for_test;
        }
        for value in contribution.values {
            let family = match &value {
                Contribution::Limiter(_) => BuiltinControlFamily::RateLimiter,
                Contribution::Breaker(_) => BuiltinControlFamily::CircuitBreaker,
                Contribution::Retry(_) => BuiltinControlFamily::Retry,
            };
            let site = contribution
                .sites
                .iter()
                .find(|(existing, _)| *existing == family)
                .map(|(_, original)| *original)
                .unwrap_or(site);
            if let Some((_, first)) = plan.sites.iter().find(|(existing, _)| *existing == family) {
                return Err(ControlCompositionError::Duplicate {
                    family: family.label(),
                    first: *first,
                    second: site,
                });
            }
            plan.sites.push((family, site));
            match value {
                Contribution::Limiter(value) => plan.rate_limiter = Some(value),
                Contribution::Breaker(value) => plan.breaker = Some(value),
                Contribution::Retry(value) => plan.retry = Some(value),
            }
        }
    }
    Ok(Box::new(plan))
}

/// The fixed no-retry preset for generated AI effect boundaries.
#[derive(Clone)]
pub struct AiResilience {
    plan: EffectPlanFactory,
}

pub fn ai_resilience() -> AiResilience {
    let mut plan = EffectPlanFactory::empty();
    plan.breaker = Some(
        circuit_breaker()
            .consecutive_failures(5)
            .open_for(std::time::Duration::from_secs(60))
            .probes(1),
    );
    AiResilience { plan }
}

#[cfg(feature = "test-support")]
pub fn ai_recovery_rejecting_resilience_for_test() -> AiResilience {
    let mut preset = ai_resilience();
    preset.plan.reject_affine_recovery_for_test = true;
    preset
}

macro_rules! delegate_effect_value {
    ($ty:ty, $plan:expr, $contribution:expr, $label:literal) => {
        impl MiddlewareFactory for $ty {
            fn label(&self) -> &'static str { $label }
            fn override_key(&self) -> crate::middleware::MiddlewareOverrideKey { crate::middleware::MiddlewareOverrideKey::of::<Self>($label) }
            fn declaration(&self) -> crate::middleware::MiddlewareDeclaration {
                if $label == "retry" {
                    crate::middleware::MiddlewareDeclaration::retry(self.label(), self.override_key().family_label())
                } else {
                    crate::middleware::MiddlewareDeclaration::effect_resilience(self.label(), self.override_key().family_label())
                }
            }
            fn builtin_control(&self) -> Option<BuiltinControlContribution> { Some(($contribution)(self)) }
            fn dsl_config_defaults(&self) -> Vec<obzenflow_runtime::runtime_config::DslConfigDefault> { ($plan)(self).dsl_config_defaults() }
            fn consumed_config_keys(&self) -> Vec<&'static str> { ($plan)(self).consumed_config_keys() }
            fn validate_configuration(&self, request: crate::middleware::MiddlewareAttachmentRequest<'_>, config: &obzenflow_runtime::pipeline::config::StageConfig, stage_type: obzenflow_core::event::context::StageType) -> crate::middleware::MiddlewareFactoryResult<()> {
                ($plan)(self).validate_configuration(request, config, stage_type)
            }
            fn materialize(&self, _request: crate::middleware::MiddlewareAttachmentRequest<'_>, context: &crate::middleware::MiddlewareMaterializationContext<'_>) -> crate::middleware::MiddlewareFactoryResult<crate::middleware::MiddlewareSurfaceAttachment> {
                Err(crate::middleware::MiddlewareFactoryError::invalid_configuration(self.label(), &context.config.name, std::io::Error::other("effect controls must be resolved through the complete attachment plan before materialisation")))
            }
            fn config_snapshot(&self) -> Option<serde_json::Value> { ($plan)(self).config_snapshot() }
        }
    }
}

delegate_effect_value!(
    Retry,
    |value: &Retry| {
        let mut plan = EffectPlanFactory::empty();
        plan.retry = Some(value.clone());
        plan
    },
    |value: &Retry| BuiltinControlContribution::retry(value.clone()),
    "retry"
);
delegate_effect_value!(
    AiResilience,
    |value: &AiResilience| value.plan.clone(),
    |value: &AiResilience| BuiltinControlContribution::from_plan(&value.plan),
    "ai_resilience"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::rate_limiter::rate_limit;
    use crate::middleware::control::retry;

    #[test]
    fn duplicate_family_is_rejected_across_sites_even_with_equal_settings() {
        let result = compose_effect_controls(vec![
            (
                MiddlewareAttachmentSite::Implementation,
                Box::new(rate_limit(10.0)),
            ),
            (MiddlewareAttachmentSite::Effect, Box::new(rate_limit(10.0))),
        ]);
        assert!(matches!(
            result,
            Err(ControlCompositionError::Duplicate {
                family: "rate_limiter",
                ..
            })
        ));
    }

    #[test]
    fn retry_only_omits_breaker_configuration() {
        let plan =
            compose_effect_controls(vec![(MiddlewareAttachmentSite::Effect, Box::new(retry()))])
                .unwrap();
        assert!(plan
            .dsl_config_defaults()
            .iter()
            .all(|value| value.key_path.starts_with("middleware.retry.")));
        assert!(plan
            .consumed_config_keys()
            .iter()
            .all(|key| key.starts_with("middleware.retry.")));
    }
}
