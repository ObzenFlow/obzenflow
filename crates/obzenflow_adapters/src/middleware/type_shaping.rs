// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Type-shaping middleware declarations for the `output_middleware:` macro
//! lane (FLOWIP-120h).
//!
//! A type-shaping middleware can author branch facts (fallback, rejection)
//! that are not produced by the stage's effect. The declaration carries those
//! branch fact types as nameable type parameters so the DSL macro can
//! validate them against the arrow contract at build time and register them
//! with the effects runtime for guarded-perform coordination.

use crate::middleware::MiddlewareFactory;
use obzenflow_core::event::schema::{TypedFactType, TypedPayload};
use obzenflow_runtime::effects::{SynthesizedOutcomeKind, SynthesizedOutcomeRegistration};
use std::marker::PhantomData;

/// A middleware factory plus the branch fact types it may author.
///
/// `F` is the fallback fact type and `R` the rejection fact type. The type
/// parameters exist so the macro lane can name them; the erased factory and a
/// runtime registration are extracted with [`into_registration_parts`].
///
/// [`into_registration_parts`]: TypeShapingMiddleware::into_registration_parts
pub struct TypeShapingMiddleware<F, R> {
    factory: Box<dyn MiddlewareFactory>,
    source_label: &'static str,
    config_error: Option<String>,
    _branches: PhantomData<fn() -> (F, R)>,
}

impl<F, R> TypeShapingMiddleware<F, R>
where
    F: TypedPayload + 'static,
    R: TypedPayload + 'static,
{
    pub fn new(
        factory: Box<dyn MiddlewareFactory>,
        source_label: &'static str,
        config_error: Option<String>,
    ) -> Self {
        Self {
            factory,
            source_label,
            config_error,
            _branches: PhantomData,
        }
    }

    /// Decompose into the erased factory, the runtime registration, and any
    /// configuration error detected while the branch types were still
    /// nameable. The DSL macro lane pushes the factory into the ordinary
    /// middleware stack, threads the registration to the effects runtime, and
    /// surfaces the error as a flow build failure.
    pub fn into_registration_parts(
        self,
    ) -> (
        Box<dyn MiddlewareFactory>,
        SynthesizedOutcomeRegistration,
        Option<String>,
    ) {
        let registration = SynthesizedOutcomeRegistration {
            effect_type: None,
            fact_types: vec![TypedFactType::of::<F>(), TypedFactType::of::<R>()],
            source_label: self.source_label.to_string(),
            kind: SynthesizedOutcomeKind::BranchShaped,
        };
        (self.factory, registration, self.config_error)
    }
}

/// An outcome-shaped fallback declaration for the `output_middleware:` lane
/// (FLOWIP-120m).
///
/// Where [`TypeShapingMiddleware`] declares branch facts disjoint from the
/// protected effect's set, this entry declares that the middleware may
/// synthesize the effect's own outcome carrier facts (a cached decision, a
/// stubbed authorization). The handler performs the plain effect; every
/// branch resumes it with `E::Outcome`. No type parameters are needed: the
/// fact types were captured when the fallback closure was configured.
pub struct OutcomeShapingMiddleware {
    factory: Box<dyn MiddlewareFactory>,
    registration: SynthesizedOutcomeRegistration,
    config_error: Option<String>,
}

impl OutcomeShapingMiddleware {
    pub fn new(
        factory: Box<dyn MiddlewareFactory>,
        registration: SynthesizedOutcomeRegistration,
        config_error: Option<String>,
    ) -> Self {
        Self {
            factory,
            registration,
            config_error,
        }
    }

    /// Same duck-typed decomposition the macro lane calls on
    /// [`TypeShapingMiddleware`].
    pub fn into_registration_parts(
        self,
    ) -> (
        Box<dyn MiddlewareFactory>,
        SynthesizedOutcomeRegistration,
        Option<String>,
    ) {
        (self.factory, self.registration, self.config_error)
    }
}

/// Decompose one `Effect with [...]` policy entry (FLOWIP-120c H7) into its
/// factory plus any typed-outcome registration, stamped with the effect the
/// entry guards. Plain policy builders carry no registration; typed builders
/// (`build_typed`, outcome-shaped fallbacks) carry theirs to the same entry,
/// so the macro knows by position which effect a typed builder protects.
pub trait IntoEffectPolicyParts {
    fn into_effect_policy_parts(
        self,
        effect_type: &'static str,
    ) -> (
        Box<dyn MiddlewareFactory>,
        Option<SynthesizedOutcomeRegistration>,
        Option<String>,
    );
}

impl IntoEffectPolicyParts for Box<dyn MiddlewareFactory> {
    fn into_effect_policy_parts(
        self,
        _effect_type: &'static str,
    ) -> (
        Box<dyn MiddlewareFactory>,
        Option<SynthesizedOutcomeRegistration>,
        Option<String>,
    ) {
        (self, None, None)
    }
}

impl<F, R> IntoEffectPolicyParts for TypeShapingMiddleware<F, R>
where
    F: TypedPayload + 'static,
    R: TypedPayload + 'static,
{
    fn into_effect_policy_parts(
        self,
        effect_type: &'static str,
    ) -> (
        Box<dyn MiddlewareFactory>,
        Option<SynthesizedOutcomeRegistration>,
        Option<String>,
    ) {
        let (factory, registration, config_error) = self.into_registration_parts();
        let (registration, config_error) =
            stamp_entry_effect(registration, config_error, effect_type);
        (factory, Some(registration), config_error)
    }
}

impl IntoEffectPolicyParts for OutcomeShapingMiddleware {
    fn into_effect_policy_parts(
        self,
        effect_type: &'static str,
    ) -> (
        Box<dyn MiddlewareFactory>,
        Option<SynthesizedOutcomeRegistration>,
        Option<String>,
    ) {
        let (factory, registration, config_error) = self.into_registration_parts();
        let (registration, config_error) =
            stamp_entry_effect(registration, config_error, effect_type);
        (factory, Some(registration), config_error)
    }
}

/// The entry position names the guarded effect; a builder that already names
/// a different effect is a configuration error rather than a silent rebind.
fn stamp_entry_effect(
    mut registration: SynthesizedOutcomeRegistration,
    config_error: Option<String>,
    effect_type: &'static str,
) -> (SynthesizedOutcomeRegistration, Option<String>) {
    match registration.effect_type.as_deref() {
        Some(existing) if existing != effect_type => {
            let mismatch = format!(
                "typed policy builder names effect '{existing}' but is attached to \
                 effect '{effect_type}'; attach it to the effect it guards \
                 (FLOWIP-120c H7)"
            );
            (
                registration,
                Some(config_error.map_or(mismatch.clone(), |e| format!("{e}; {mismatch}"))),
            )
        }
        _ => {
            registration.effect_type = Some(effect_type.to_string());
            (registration, config_error)
        }
    }
}
