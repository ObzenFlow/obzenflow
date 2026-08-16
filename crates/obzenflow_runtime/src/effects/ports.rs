// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Opaque typed registration and history-gated scoped effect ports (FLOWIP-132a).

use super::binding::{
    validate_effect_type, validate_slot_name, BindingCoordinate, BoundEffectPortSlot,
    EffectPortSlotRequirement,
};
use super::{
    BindingAuthorityFault, EffectBinding, EffectDeclaration, EffectPortSlot,
    LogicalEffectBindingName, NamedEffect,
};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock};

type ErasedPort = Arc<dyn Any + Send + Sync>;
type ErasedResolver = Arc<dyn Fn() -> Result<ErasedPort, EffectPortResolutionError> + Send + Sync>;
const MAX_REPORTED_MISSING_SLOTS: usize = 16;

/// Closed provider-side result for bounded local client construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EffectPortResolutionError {
    #[error("credential unavailable")]
    CredentialUnavailable,
    #[error("client construction failed")]
    ClientConstructionFailed,
}

/// Non-suspending, bounded resolver recipe.
pub type EffectPortResolver<P> =
    Arc<dyn Fn() -> Result<Arc<P>, EffectPortResolutionError> + Send + Sync>;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EffectBindingBuildError {
    #[error("effect binding uses an invalid effect identifier")]
    InvalidEffectType,
    #[error("effect binding '{binding}' for '{effect_type}' declares an invalid slot label")]
    InvalidSlot {
        binding: LogicalEffectBindingName,
        effect_type: &'static str,
    },
    #[error("effect binding '{binding}' for '{effect_type}' is missing slots {slots:?}")]
    MissingSlots {
        binding: LogicalEffectBindingName,
        effect_type: &'static str,
        slots: Vec<&'static str>,
    },
    #[error("effect binding '{binding}' for '{effect_type}' received unexpected slot '{slot}'")]
    UnexpectedSlot {
        binding: LogicalEffectBindingName,
        effect_type: &'static str,
        slot: &'static str,
    },
    #[error("effect binding '{binding}' for '{effect_type}' received duplicate slot '{slot}'")]
    DuplicateSlot {
        binding: LogicalEffectBindingName,
        effect_type: &'static str,
        slot: &'static str,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EffectPortRegistrationError {
    #[error("effect registration '{binding}' for '{effect_type}' is already installed")]
    DuplicateRegistration {
        binding: LogicalEffectBindingName,
        effect_type: &'static str,
    },
}

#[derive(Clone)]
enum PortRecipe {
    Eager(ErasedPort),
    Deferred(ErasedResolver),
}

#[derive(Clone)]
enum RunPortEntry {
    Eager(ErasedPort),
    Deferred {
        resolver: ErasedResolver,
        verdict: Arc<OnceLock<Result<ErasedPort, ResolverVerdictError>>>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResolverVerdictError {
    Provider(EffectPortResolutionError),
    Panicked,
    NotMaterialised,
}

struct PendingPort {
    requirement: EffectPortSlotRequirement,
    coordinate: BindingCoordinate,
    recipe: PortRecipe,
}

/// Public type-safe builder shared by integration facades, applications, and fixtures.
pub struct EffectRegistrationBuilder<E: NamedEffect> {
    logical_name: LogicalEffectBindingName,
    evidence: E::BindingEvidence,
    expected: Vec<EffectPortSlotRequirement>,
    pending: Vec<PendingPort>,
    _effect: PhantomData<fn() -> E>,
}

impl<E: NamedEffect> EffectRegistrationBuilder<E> {
    pub fn new(logical_name: LogicalEffectBindingName, evidence: E::BindingEvidence) -> Self {
        Self {
            logical_name,
            evidence,
            expected: E::required_slots().slots,
            pending: Vec::new(),
            _effect: PhantomData,
        }
    }

    pub fn bind_eager<P>(
        mut self,
        slot: EffectPortSlot<P>,
        port: Arc<P>,
    ) -> Result<Self, EffectBindingBuildError>
    where
        P: ?Sized + Send + Sync + 'static,
    {
        let erased = Arc::new(port) as ErasedPort;
        self.push(slot, PortRecipe::Eager(erased))?;
        Ok(self)
    }

    pub fn bind_deferred<P>(
        mut self,
        slot: EffectPortSlot<P>,
        resolver: EffectPortResolver<P>,
    ) -> Result<Self, EffectBindingBuildError>
    where
        P: ?Sized + Send + Sync + 'static,
    {
        let erased: ErasedResolver = Arc::new(move || {
            let port = resolver()?;
            Ok(Arc::new(port) as ErasedPort)
        });
        self.push(slot, PortRecipe::Deferred(erased))?;
        Ok(self)
    }

    fn push<P>(
        &mut self,
        slot: EffectPortSlot<P>,
        recipe: PortRecipe,
    ) -> Result<(), EffectBindingBuildError>
    where
        P: ?Sized + Send + Sync + 'static,
    {
        // Validate the effect identifier before constructing any error that
        // would otherwise project the author's raw `EFFECT_TYPE` string.
        validate_effect_type(E::EFFECT_TYPE)
            .map_err(|_| EffectBindingBuildError::InvalidEffectType)?;
        let requirement = slot.requirement();
        if validate_slot_name(requirement.label).is_err() {
            return Err(EffectBindingBuildError::InvalidSlot {
                binding: self.logical_name.clone(),
                effect_type: E::EFFECT_TYPE,
            });
        }
        if !self.expected.iter().any(|expected| {
            expected.type_id == requirement.type_id && expected.label == requirement.label
        }) {
            return Err(EffectBindingBuildError::UnexpectedSlot {
                binding: self.logical_name.clone(),
                effect_type: E::EFFECT_TYPE,
                slot: requirement.label,
            });
        }
        if self
            .pending
            .iter()
            .any(|pending| pending.requirement.label == requirement.label)
        {
            return Err(EffectBindingBuildError::DuplicateSlot {
                binding: self.logical_name.clone(),
                effect_type: E::EFFECT_TYPE,
                slot: requirement.label,
            });
        }
        self.pending.push(PendingPort {
            requirement,
            coordinate: BindingCoordinate::mint(),
            recipe,
        });
        Ok(())
    }

    pub fn finish(
        mut self,
    ) -> Result<(EffectBinding<E>, EffectRegistration<E>), EffectBindingBuildError> {
        validate_effect_type(E::EFFECT_TYPE)
            .map_err(|_| EffectBindingBuildError::InvalidEffectType)?;

        let mut declared = HashSet::new();
        for slot in &self.expected {
            if validate_slot_name(slot.label).is_err() {
                return Err(EffectBindingBuildError::InvalidSlot {
                    binding: self.logical_name.clone(),
                    effect_type: E::EFFECT_TYPE,
                });
            }
            if !declared.insert(slot.label) {
                return Err(EffectBindingBuildError::DuplicateSlot {
                    binding: self.logical_name.clone(),
                    effect_type: E::EFFECT_TYPE,
                    slot: slot.label,
                });
            }
        }

        let mut missing = self
            .expected
            .iter()
            .filter(|expected| {
                !self.pending.iter().any(|pending| {
                    pending.requirement.type_id == expected.type_id
                        && pending.requirement.label == expected.label
                })
            })
            .map(|slot| slot.label)
            .take(MAX_REPORTED_MISSING_SLOTS)
            .collect::<Vec<_>>();
        missing.sort_unstable();
        if !missing.is_empty() {
            return Err(EffectBindingBuildError::MissingSlots {
                binding: self.logical_name,
                effect_type: E::EFFECT_TYPE,
                slots: missing,
            });
        }

        self.pending
            .sort_by(|left, right| left.requirement.label.cmp(right.requirement.label));
        let bound_slots = self
            .pending
            .iter()
            .map(|pending| BoundEffectPortSlot {
                requirement: pending.requirement.clone(),
                coordinate: pending.coordinate,
            })
            .collect();
        let registration_coordinate = BindingCoordinate::mint();
        let binding = EffectBinding::from_parts(
            self.logical_name.clone(),
            self.evidence,
            registration_coordinate,
            bound_slots,
        );
        let registration = EffectRegistration {
            logical_name: self.logical_name,
            effect_type: E::EFFECT_TYPE,
            coordinate: registration_coordinate,
            entries: self
                .pending
                .into_iter()
                .map(|pending| (pending.coordinate, pending.recipe))
                .collect(),
            _effect: PhantomData,
        };
        Ok((binding, registration))
    }
}

impl<E: NamedEffect> std::fmt::Debug for EffectRegistrationBuilder<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let effect_type = if validate_effect_type(E::EFFECT_TYPE).is_ok() {
            E::EFFECT_TYPE
        } else {
            "invalid_effect_type"
        };
        formatter
            .debug_struct("EffectRegistrationBuilder")
            .field("effect_type", &effect_type)
            .field("logical_name", &self.logical_name)
            .field("bound_slot_count", &self.pending.len())
            .field("evidence", &"<not disclosed>")
            .field("recipes", &"<not disclosed>")
            .finish()
    }
}

/// Opaque consuming registration authority.
pub struct EffectRegistration<E: NamedEffect> {
    logical_name: LogicalEffectBindingName,
    effect_type: &'static str,
    coordinate: BindingCoordinate,
    entries: Vec<(BindingCoordinate, PortRecipe)>,
    _effect: PhantomData<fn() -> E>,
}

impl<E: NamedEffect> std::fmt::Debug for EffectRegistration<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectRegistration")
            .field("effect_type", &self.effect_type)
            .field("logical_name", &self.logical_name)
            .field("authority", &"<not disclosed>")
            .finish()
    }
}

/// Flow-owned registry. Its public mutation surface accepts only completed typed registrations.
#[derive(Clone, Default)]
pub struct EffectPortRegistry {
    recipes: Arc<HashMap<BindingCoordinate, PortRecipe>>,
    installed: Arc<HashSet<(TypeId, String)>>,
    installed_coordinates: Arc<HashSet<BindingCoordinate>>,
    run_entries: Option<Arc<HashMap<BindingCoordinate, RunPortEntry>>>,
}

impl EffectPortRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn install<E: NamedEffect>(
        &mut self,
        registration: EffectRegistration<E>,
    ) -> Result<(), EffectPortRegistrationError> {
        let installation_key = (
            TypeId::of::<E>(),
            registration.logical_name.as_str().to_string(),
        );
        if self.installed.contains(&installation_key) {
            return Err(EffectPortRegistrationError::DuplicateRegistration {
                binding: registration.logical_name,
                effect_type: registration.effect_type,
            });
        }
        Arc::make_mut(&mut self.installed).insert(installation_key);
        Arc::make_mut(&mut self.installed_coordinates).insert(registration.coordinate);
        let recipes = Arc::make_mut(&mut self.recipes);
        for (coordinate, recipe) in registration.entries {
            recipes.insert(coordinate, recipe);
        }
        self.run_entries = None;
        Ok(())
    }

    pub(crate) fn into_run_registry(mut self) -> Self {
        let entries = self
            .recipes
            .iter()
            .map(|(coordinate, recipe)| {
                let entry = match recipe {
                    PortRecipe::Eager(port) => RunPortEntry::Eager(Arc::clone(port)),
                    PortRecipe::Deferred(resolver) => RunPortEntry::Deferred {
                        resolver: Arc::clone(resolver),
                        verdict: Arc::new(OnceLock::new()),
                    },
                };
                (*coordinate, entry)
            })
            .collect();
        self.run_entries = Some(Arc::new(entries));
        self
    }

    /// Verify that a named declaration carries authority minted by a
    /// registration installed in this registry. This is a metadata-only
    /// materialisation preflight: it never invokes a deferred resolver.
    #[doc(hidden)]
    pub fn validate_required_registration(
        &self,
        declaration: &EffectDeclaration,
    ) -> Result<(), BindingAuthorityFault> {
        let Some((logical_name, registration, slots)) = declaration.binding().named_parts() else {
            return Ok(());
        };

        if !self.installed_coordinates.contains(&registration) {
            return Err(BindingAuthorityFault::registration_missing(
                declaration.effect_type(),
                logical_name.clone(),
            ));
        }

        if slots
            .iter()
            .any(|slot| !self.recipes.contains_key(&slot.coordinate))
        {
            return Err(BindingAuthorityFault::registration_missing(
                declaration.effect_type(),
                logical_name.clone(),
            ));
        }

        Ok(())
    }

    pub(super) fn scoped_view(
        &self,
        registration: BindingCoordinate,
        slots: &[BoundEffectPortSlot],
    ) -> Result<EffectPortView, EffectPortViewBuildError> {
        if !self.installed_coordinates.contains(&registration) {
            return Err(EffectPortViewBuildError {
                slot: None,
                kind: EffectPortViewBuildErrorKind::MissingRegistration,
            });
        }
        let mut resolved = HashMap::with_capacity(slots.len());
        for slot in slots {
            let port = self.resolve(slot)?;
            resolved.insert((slot.requirement.type_id, slot.requirement.label), port);
        }
        Ok(EffectPortView { resolved })
    }

    fn resolve(&self, slot: &BoundEffectPortSlot) -> Result<ErasedPort, EffectPortViewBuildError> {
        let Some(entries) = self.run_entries.as_ref() else {
            return match self.recipes.get(&slot.coordinate) {
                Some(PortRecipe::Eager(port)) => Ok(Arc::clone(port)),
                Some(PortRecipe::Deferred(_)) => Err(EffectPortViewBuildError {
                    slot: Some(slot.requirement.label),
                    kind: EffectPortViewBuildErrorKind::Resolver(
                        ResolverVerdictError::NotMaterialised,
                    ),
                }),
                None => Err(EffectPortViewBuildError {
                    slot: Some(slot.requirement.label),
                    kind: EffectPortViewBuildErrorKind::MissingRegistration,
                }),
            };
        };
        let Some(entry) = entries.get(&slot.coordinate) else {
            return Err(EffectPortViewBuildError {
                slot: Some(slot.requirement.label),
                kind: EffectPortViewBuildErrorKind::MissingRegistration,
            });
        };
        match entry {
            RunPortEntry::Eager(port) => Ok(Arc::clone(port)),
            RunPortEntry::Deferred { resolver, verdict } => verdict
                .get_or_init(|| {
                    catch_unwind(AssertUnwindSafe(|| resolver()))
                        .map_err(|_| ResolverVerdictError::Panicked)
                        .and_then(|result| result.map_err(ResolverVerdictError::Provider))
                })
                .as_ref()
                .map(Arc::clone)
                .map_err(|error| EffectPortViewBuildError {
                    slot: Some(slot.requirement.label),
                    kind: EffectPortViewBuildErrorKind::Resolver(*error),
                }),
        }
    }
}

impl std::fmt::Debug for EffectPortRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectPortRegistry")
            .field("registrations", &self.installed.len())
            .field("run_registry", &self.run_entries.is_some())
            .field("authority", &"<not disclosed>")
            .finish()
    }
}

#[derive(Clone, Default)]
pub(super) struct EffectPortView {
    resolved: HashMap<(TypeId, &'static str), ErasedPort>,
}

impl EffectPortView {
    pub(super) fn get<P>(&self, slot: EffectPortSlot<P>) -> Option<Arc<P>>
    where
        P: ?Sized + Send + Sync + 'static,
    {
        self.resolved
            .get(&(TypeId::of::<P>(), slot.label()))?
            .downcast_ref::<Arc<P>>()
            .cloned()
    }
}

impl std::fmt::Debug for EffectPortView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectPortView")
            .field("slot_count", &self.resolved.len())
            .field("ports", &"<not disclosed>")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EffectPortViewBuildErrorKind {
    MissingRegistration,
    Resolver(ResolverVerdictError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EffectPortViewBuildError {
    pub(crate) slot: Option<&'static str>,
    pub(crate) kind: EffectPortViewBuildErrorKind,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::{
        Effect, EffectBindingEvidence, EffectBindingUse, EffectContext, EffectError,
        EffectPortSlotSet, EffectSafety, Named, RecordedReply,
    };
    use async_trait::async_trait;
    use obzenflow_core::{BoundedBindingEvidence, EffectBindingIdentity, TypedPayload};
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Evidence;

    impl EffectBindingEvidence for Evidence {
        const SCHEMA_VERSION: u32 = 1;

        fn canonical_bytes(&self) -> BoundedBindingEvidence {
            BoundedBindingEvidence::try_new(b"fixture".to_vec()).unwrap()
        }
    }

    trait Port: Send + Sync {
        fn value(&self) -> usize;
    }

    struct LivePort;
    impl Port for LivePort {
        fn value(&self) -> usize {
            7
        }
    }

    const PORT: EffectPortSlot<dyn Port> = EffectPortSlot::new("client");
    const FALLBACK: EffectPortSlot<dyn Port> = EffectPortSlot::new("fallback");
    const CONFLICTING_CLIENT: EffectPortSlot<usize> = EffectPortSlot::new("client");

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Outcome;
    impl TypedPayload for Outcome {
        const EVENT_TYPE: &'static str = "binding.fixture.outcome";
    }

    #[derive(Clone, Debug)]
    struct BoundEffect {
        binding: EffectBindingUse<Self>,
    }

    #[async_trait]
    impl Effect for BoundEffect {
        const EFFECT_TYPE: &'static str = "binding.fixture.effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;
        type BindingMode = Named<Evidence>;
        type Outcome = Outcome;
        type OutcomeSemantics = RecordedReply;

        fn label(&self) -> &str {
            "fixture"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Outcome)
        }
    }

    impl NamedEffect for BoundEffect {
        type BindingEvidence = Evidence;

        fn binding_use(&self) -> &EffectBindingUse<Self> {
            &self.binding
        }

        fn required_slots() -> EffectPortSlotSet {
            EffectPortSlotSet::single(PORT)
        }
    }

    #[derive(Clone, Debug)]
    struct InvalidIdentifierEffect {
        binding: EffectBindingUse<Self>,
    }

    #[async_trait]
    impl Effect for InvalidIdentifierEffect {
        const EFFECT_TYPE: &'static str = "https://credential-canary.example";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;
        type BindingMode = Named<Evidence>;
        type Outcome = Outcome;
        type OutcomeSemantics = RecordedReply;

        fn label(&self) -> &str {
            "invalid-identifier-fixture"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Outcome)
        }
    }

    impl NamedEffect for InvalidIdentifierEffect {
        type BindingEvidence = Evidence;

        fn binding_use(&self) -> &EffectBindingUse<Self> {
            &self.binding
        }

        fn required_slots() -> EffectPortSlotSet {
            EffectPortSlotSet::single(PORT)
        }
    }

    #[derive(Clone, Debug)]
    struct TwoSlotEffect {
        binding: EffectBindingUse<Self>,
    }

    #[async_trait]
    impl Effect for TwoSlotEffect {
        const EFFECT_TYPE: &'static str = "binding.fixture.two_slot_effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;
        type BindingMode = Named<Evidence>;
        type Outcome = Outcome;
        type OutcomeSemantics = RecordedReply;

        fn label(&self) -> &str {
            "two-slot-fixture"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Outcome)
        }
    }

    impl NamedEffect for TwoSlotEffect {
        type BindingEvidence = Evidence;

        fn binding_use(&self) -> &EffectBindingUse<Self> {
            &self.binding
        }

        fn required_slots() -> EffectPortSlotSet {
            EffectPortSlotSet::new().with(PORT).with(FALLBACK)
        }
    }

    #[derive(Clone, Debug)]
    struct DuplicateLabelEffect {
        binding: EffectBindingUse<Self>,
    }

    #[async_trait]
    impl Effect for DuplicateLabelEffect {
        const EFFECT_TYPE: &'static str = "binding.fixture.duplicate_label_effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;
        type BindingMode = Named<Evidence>;
        type Outcome = Outcome;
        type OutcomeSemantics = RecordedReply;

        fn label(&self) -> &str {
            "duplicate-label-fixture"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Outcome)
        }
    }

    impl NamedEffect for DuplicateLabelEffect {
        type BindingEvidence = Evidence;

        fn binding_use(&self) -> &EffectBindingUse<Self> {
            &self.binding
        }

        fn required_slots() -> EffectPortSlotSet {
            EffectPortSlotSet::new().with(PORT).with(CONFLICTING_CLIENT)
        }
    }

    fn name() -> LogicalEffectBindingName {
        LogicalEffectBindingName::new("fixture").unwrap()
    }

    #[test]
    fn exact_slot_builder_returns_one_binding_and_consuming_registration() {
        let (binding, registration) =
            EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
                .bind_eager(PORT, Arc::new(LivePort) as Arc<dyn Port>)
                .unwrap()
                .finish()
                .unwrap();
        let mut registry = EffectPortRegistry::new();
        registry.install(registration).unwrap();
        let registry = registry.into_run_registry();
        let declaration = binding.declaration_binding();
        let super::super::binding::EffectDeclarationBinding::Named(named) = declaration else {
            panic!("expected named declaration")
        };
        let view = registry
            .scoped_view(named.registration, &named.slots)
            .unwrap();
        assert_eq!(view.get(PORT).unwrap().value(), 7);
    }

    #[test]
    fn deferred_resolver_body_runs_once_and_caches_its_verdict() {
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver: EffectPortResolver<dyn Port> = Arc::new({
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(EffectPortResolutionError::CredentialUnavailable)
            }
        });
        let (binding, registration) =
            EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
                .bind_deferred(PORT, resolver)
                .unwrap()
                .finish()
                .unwrap();
        let mut registry = EffectPortRegistry::new();
        registry.install(registration).unwrap();
        let registry = registry.into_run_registry();
        let super::super::binding::EffectDeclarationBinding::Named(named) =
            binding.declaration_binding()
        else {
            panic!("expected named declaration")
        };
        assert!(registry
            .scoped_view(named.registration, &named.slots)
            .is_err());
        assert!(registry
            .scoped_view(named.registration, &named.slots)
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn missing_and_duplicate_slots_fail_before_registration_exists() {
        let missing = EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
            .finish()
            .unwrap_err();
        assert!(matches!(
            missing,
            EffectBindingBuildError::MissingSlots { .. }
        ));

        let duplicate = EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
            .bind_eager(PORT, Arc::new(LivePort) as Arc<dyn Port>)
            .unwrap()
            .bind_eager(PORT, Arc::new(LivePort) as Arc<dyn Port>)
            .unwrap_err();
        assert!(matches!(
            duplicate,
            EffectBindingBuildError::DuplicateSlot { .. }
        ));

        let duplicate_label =
            EffectRegistrationBuilder::<DuplicateLabelEffect>::new(name(), Evidence)
                .finish()
                .unwrap_err();
        assert!(matches!(
            duplicate_label,
            EffectBindingBuildError::DuplicateSlot { slot: "client", .. }
        ));
    }

    #[test]
    fn invalid_effect_identifiers_are_rejected_before_any_raw_value_is_projected() {
        let builder = EffectRegistrationBuilder::<InvalidIdentifierEffect>::new(name(), Evidence);
        let builder_debug = format!("{builder:?}");
        assert!(!builder_debug.contains("credential-canary"));
        assert!(builder_debug.contains("invalid_effect_type"));

        let error = builder
            .bind_eager(PORT, Arc::new(LivePort) as Arc<dyn Port>)
            .unwrap_err();
        assert_eq!(error, EffectBindingBuildError::InvalidEffectType);
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(!display.contains("credential-canary"));
        assert!(!debug.contains("credential-canary"));
    }

    #[test]
    fn binding_evidence_digest_matches_the_locked_sha256_vector() {
        let identity = super::super::binding::binding_identity::<BoundEffect>(&Evidence);
        let EffectBindingIdentity::Named { evidence } = identity else {
            panic!("fixture is named")
        };
        assert_eq!(evidence.schema_version, 1);
        assert_eq!(
            evidence.digest,
            "c99d0f995ff4e8a89afcde2bdc08a6b5f5912d5306c87ba3d6a2dde2dc1cd22e"
        );
    }

    #[test]
    fn two_slot_failure_caches_each_coordinate_and_never_returns_a_partial_view() {
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let first: EffectPortResolver<dyn Port> = Arc::new({
            let calls = Arc::clone(&first_calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(LivePort) as Arc<dyn Port>)
            }
        });
        let second: EffectPortResolver<dyn Port> = Arc::new({
            let calls = Arc::clone(&second_calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(EffectPortResolutionError::ClientConstructionFailed)
            }
        });
        let (binding, registration) =
            EffectRegistrationBuilder::<TwoSlotEffect>::new(name(), Evidence)
                .bind_deferred(PORT, first)
                .unwrap()
                .bind_deferred(FALLBACK, second)
                .unwrap()
                .finish()
                .unwrap();
        let mut registry = EffectPortRegistry::new();
        registry.install(registration).unwrap();
        let registry = registry.into_run_registry();
        let super::super::binding::EffectDeclarationBinding::Named(named) =
            binding.declaration_binding()
        else {
            panic!("expected named declaration")
        };

        for _ in 0..2 {
            let error = registry
                .scoped_view(named.registration, &named.slots)
                .expect_err("a failed second slot cannot expose the first slot");
            assert_eq!(error.slot, Some("fallback"));
        }
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_resolution_is_single_flight_per_materialised_run() {
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver: EffectPortResolver<dyn Port> = Arc::new({
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(20));
                Ok(Arc::new(LivePort) as Arc<dyn Port>)
            }
        });
        let (binding, registration) =
            EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
                .bind_deferred(PORT, resolver)
                .unwrap()
                .finish()
                .unwrap();
        let mut registry = EffectPortRegistry::new();
        registry.install(registration).unwrap();
        let registry = registry.into_run_registry();
        let super::super::binding::EffectDeclarationBinding::Named(named) =
            binding.declaration_binding()
        else {
            panic!("expected named declaration")
        };

        let threads = (0..8)
            .map(|_| {
                let registry = registry.clone();
                let slots = Arc::clone(&named.slots);
                let registration = named.registration;
                std::thread::spawn(move || {
                    assert_eq!(
                        registry
                            .scoped_view(registration, &slots)
                            .unwrap()
                            .get(PORT)
                            .unwrap()
                            .value(),
                        7
                    );
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_after_a_resolver_begins_cannot_authorise_a_second_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let resolver: EffectPortResolver<dyn Port> = Arc::new({
            let calls = Arc::clone(&calls);
            let release_rx = Arc::clone(&release_rx);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                started_tx.send(()).expect("test observes resolver start");
                release_rx
                    .lock()
                    .expect("release lock")
                    .recv()
                    .expect("test releases resolver");
                Ok(Arc::new(LivePort) as Arc<dyn Port>)
            }
        });
        let (binding, registration) =
            EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
                .bind_deferred(PORT, resolver)
                .unwrap()
                .finish()
                .unwrap();
        let mut registry = EffectPortRegistry::new();
        registry.install(registration).unwrap();
        let registry = registry.into_run_registry();
        let super::super::binding::EffectDeclarationBinding::Named(named) =
            binding.declaration_binding()
        else {
            panic!("expected named declaration")
        };

        let first_registry = registry.clone();
        let first_slots = Arc::clone(&named.slots);
        let registration = named.registration;
        let first =
            tokio::spawn(async move { first_registry.scoped_view(registration, &first_slots) });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("resolver must begin");
        first.abort();
        release_tx.send(()).expect("release begun resolver");
        let _ = first.await;

        let view = registry
            .scoped_view(named.registration, &named.slots)
            .expect("the completed verdict remains cached after task cancellation");
        assert_eq!(view.get(PORT).unwrap().value(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn resolver_panic_is_cached_once_but_a_new_run_gets_a_fresh_cell() {
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver: EffectPortResolver<dyn Port> = Arc::new({
            let calls = Arc::clone(&calls);
            move || -> Result<Arc<dyn Port>, EffectPortResolutionError> {
                calls.fetch_add(1, Ordering::SeqCst);
                panic!("resolver-panic-canary")
            }
        });
        let (binding, registration) =
            EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
                .bind_deferred(PORT, resolver)
                .unwrap()
                .finish()
                .unwrap();
        let mut registry = EffectPortRegistry::new();
        registry.install(registration).unwrap();
        let super::super::binding::EffectDeclarationBinding::Named(named) =
            binding.declaration_binding()
        else {
            panic!("expected named declaration")
        };

        for run in [
            registry.clone().into_run_registry(),
            registry.into_run_registry(),
        ] {
            for _ in 0..2 {
                let error = run
                    .scoped_view(named.registration, &named.slots)
                    .expect_err("panics become a closed cached verdict");
                assert_eq!(
                    error.kind,
                    EffectPortViewBuildErrorKind::Resolver(ResolverVerdictError::Panicked)
                );
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn authority_debug_surfaces_do_not_disclose_evidence_or_ports() {
        let (binding, registration) =
            EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
                .bind_eager(PORT, Arc::new(LivePort) as Arc<dyn Port>)
                .unwrap()
                .finish()
                .unwrap();
        let binding_debug = format!("{binding:?}");
        let registration_debug = format!("{registration:?}");
        assert!(binding_debug.contains("<not disclosed>"));
        assert!(registration_debug.contains("<not disclosed>"));
        assert!(!registration_debug.contains("LivePort"));
    }
}
