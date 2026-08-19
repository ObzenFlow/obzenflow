// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Opaque typed registration and history-gated scoped effect ports (FLOWIP-132a).

use super::binding::{
    validate_effect_type, validate_slot_name, BindingCoordinate, BoundEffectPortSlot,
    EffectPortSlotRequirement, NoPortMetadata,
};
use super::{
    transactional_effect_port_slot, BindingAuthorityFault, EffectBinding, EffectDeclaration,
    EffectPortSlot, EffectSafety, LogicalEffectBindingName, NamedEffect,
};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, OnceLock};

type ErasedValue = Arc<dyn Any + Send + Sync>;

#[derive(Clone)]
struct ErasedResolvedEffectPort {
    port: ErasedValue,
    metadata: ErasedValue,
}

type ErasedResolver =
    Arc<dyn Fn() -> Result<ErasedResolvedEffectPort, EffectPortResolutionError> + Send + Sync>;
const MAX_REPORTED_MISSING_SLOTS: usize = 16;

/// Closed provider-side result for bounded local client construction.
#[non_exhaustive]
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

/// A callable port and its immutable pre-boundary metadata snapshot.
///
/// The two values form one resolver verdict and cannot be registered under
/// independent lifecycles or coordinates.
pub struct ResolvedEffectPort<P, M>
where
    P: ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    port: Arc<P>,
    metadata: Arc<M>,
}

impl<P, M> ResolvedEffectPort<P, M>
where
    P: ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    /// Bind callable authority to the metadata snapshot observed at construction.
    pub fn new(port: Arc<P>, metadata: Arc<M>) -> Self {
        Self { port, metadata }
    }
}

impl<P, M> std::fmt::Debug for ResolvedEffectPort<P, M>
where
    P: ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedEffectPort")
            .field("port", &"<not disclosed>")
            .field("metadata", &"<not disclosed>")
            .finish()
    }
}

/// Non-suspending resolver that co-produces callable authority and metadata.
pub type EffectPortResolverWithMetadata<P, M> =
    Arc<dyn Fn() -> Result<ResolvedEffectPort<P, M>, EffectPortResolutionError> + Send + Sync>;

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
pub enum EffectRegistrationCollectionError {
    #[error("distinct bindings were declared for '{effect_type} via {binding}'")]
    DistinctBindings {
        binding: LogicalEffectBindingName,
        effect_type: &'static str,
    },
    #[error("binding package for '{effect_type} via {binding}' was already collected")]
    AlreadyCollected {
        binding: LogicalEffectBindingName,
        effect_type: &'static str,
    },
    #[error("declaration for '{effect_type} via {binding}' has no pending binding package")]
    MissingPackage {
        binding: LogicalEffectBindingName,
        effect_type: &'static str,
    },
}

#[derive(Clone)]
enum PortRecipe {
    Eager(ErasedResolvedEffectPort),
    Deferred(ErasedResolver),
}

#[derive(Clone)]
enum RunPortEntry {
    Eager(ErasedResolvedEffectPort),
    Deferred {
        resolver: ErasedResolver,
        verdict: Arc<OnceLock<Result<ErasedResolvedEffectPort, ResolverVerdictError>>>,
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

enum PendingRegistrationState {
    Pending(Vec<(BindingCoordinate, PortRecipe)>),
    Collected,
}

/// Shared, process-local package carried only by authored declarations.
pub(super) struct PendingRegistrationPackage {
    logical_name: LogicalEffectBindingName,
    effect_type: &'static str,
    effect_type_id: TypeId,
    coordinate: BindingCoordinate,
    state: Mutex<PendingRegistrationState>,
}

impl PendingRegistrationPackage {
    fn new<E: NamedEffect>(
        logical_name: LogicalEffectBindingName,
        coordinate: BindingCoordinate,
        entries: Vec<(BindingCoordinate, PortRecipe)>,
    ) -> Self {
        Self {
            logical_name,
            effect_type: E::EFFECT_TYPE,
            effect_type_id: TypeId::of::<E>(),
            coordinate,
            state: Mutex::new(PendingRegistrationState::Pending(entries)),
        }
    }
}

impl std::fmt::Debug for PendingRegistrationPackage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingRegistrationPackage")
            .field("effect_type", &self.effect_type)
            .field("logical_name", &self.logical_name)
            .field("authority", &"<not disclosed>")
            .field("recipes", &"<not disclosed>")
            .finish()
    }
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
        let mut expected = E::required_slots().slots;
        if matches!(E::SAFETY, EffectSafety::Transactional) {
            let executor = transactional_effect_port_slot::<E>().requirement();
            if !expected.iter().any(|slot| {
                slot.port_type_id == executor.port_type_id
                    && slot.metadata_type_id == executor.metadata_type_id
                    && slot.label == executor.label
            }) {
                expected.push(executor);
            }
        }
        Self {
            logical_name,
            evidence,
            expected,
            pending: Vec::new(),
            _effect: PhantomData,
        }
    }

    pub fn bind_eager<P>(
        self,
        slot: EffectPortSlot<P>,
        port: Arc<P>,
    ) -> Result<Self, EffectBindingBuildError>
    where
        P: ?Sized + Send + Sync + 'static,
    {
        self.bind_eager_with_metadata(
            slot,
            ResolvedEffectPort::new(port, Arc::new(NoPortMetadata)),
        )
    }

    pub fn bind_deferred<P>(
        self,
        slot: EffectPortSlot<P>,
        resolver: EffectPortResolver<P>,
    ) -> Result<Self, EffectBindingBuildError>
    where
        P: ?Sized + Send + Sync + 'static,
    {
        let resolver_with_metadata: EffectPortResolverWithMetadata<P, NoPortMetadata> =
            Arc::new(move || {
                let port = resolver()?;
                Ok(ResolvedEffectPort::new(port, Arc::new(NoPortMetadata)))
            });
        self.bind_deferred_with_metadata(slot, resolver_with_metadata)
    }

    /// Bind an already-constructed port and metadata snapshot as one verdict.
    pub fn bind_eager_with_metadata<P, M>(
        mut self,
        slot: EffectPortSlot<P, M>,
        resolved: ResolvedEffectPort<P, M>,
    ) -> Result<Self, EffectBindingBuildError>
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        self.push(slot, PortRecipe::Eager(Self::erase(resolved)))?;
        Ok(self)
    }

    /// Bind one resolver that co-produces a port and metadata snapshot.
    pub fn bind_deferred_with_metadata<P, M>(
        mut self,
        slot: EffectPortSlot<P, M>,
        resolver: EffectPortResolverWithMetadata<P, M>,
    ) -> Result<Self, EffectBindingBuildError>
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        let erased: ErasedResolver = Arc::new(move || resolver().map(Self::erase));
        self.push(slot, PortRecipe::Deferred(erased))?;
        Ok(self)
    }

    fn erase<P, M>(resolved: ResolvedEffectPort<P, M>) -> ErasedResolvedEffectPort
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        ErasedResolvedEffectPort {
            port: Arc::new(resolved.port),
            metadata: Arc::new(resolved.metadata),
        }
    }

    fn push<P, M>(
        &mut self,
        slot: EffectPortSlot<P, M>,
        recipe: PortRecipe,
    ) -> Result<(), EffectBindingBuildError>
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
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
            expected.port_type_id == requirement.port_type_id
                && expected.metadata_type_id == requirement.metadata_type_id
                && expected.label == requirement.label
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

    pub fn finish(mut self) -> Result<EffectBinding<E>, EffectBindingBuildError> {
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
                    pending.requirement.port_type_id == expected.port_type_id
                        && pending.requirement.metadata_type_id == expected.metadata_type_id
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
        let package = Arc::new(PendingRegistrationPackage::new::<E>(
            self.logical_name.clone(),
            registration_coordinate,
            self.pending
                .into_iter()
                .map(|pending| (pending.coordinate, pending.recipe))
                .collect(),
        ));
        Ok(EffectBinding::from_parts(
            self.logical_name,
            self.evidence,
            registration_coordinate,
            bound_slots,
            package,
        ))
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

/// Flow-owned registry collected atomically from authored declarations.
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

    /// Collect the complete declaration set as one atomic ownership transfer.
    #[doc(hidden)]
    pub fn collect_from_declarations<'a>(
        declarations: impl IntoIterator<Item = &'a EffectDeclaration>,
    ) -> Result<Self, EffectRegistrationCollectionError> {
        let mut by_key: HashMap<
            (&'static str, String),
            (BindingCoordinate, Arc<PendingRegistrationPackage>),
        > = HashMap::new();

        for declaration in declarations {
            let Some((logical_name, registration, _)) = declaration.binding().named_parts() else {
                continue;
            };
            let package = declaration.binding().pending_package().ok_or_else(|| {
                EffectRegistrationCollectionError::MissingPackage {
                    binding: logical_name.clone(),
                    effect_type: declaration.effect_type(),
                }
            })?;
            let key = (declaration.effect_type(), logical_name.as_str().to_string());
            if let Some((first_coordinate, _)) = by_key.get(&key) {
                if *first_coordinate != registration {
                    return Err(EffectRegistrationCollectionError::DistinctBindings {
                        binding: logical_name.clone(),
                        effect_type: declaration.effect_type(),
                    });
                }
                continue;
            }
            by_key.insert(key, (registration, Arc::clone(package)));
        }

        let mut packages = by_key.into_values().collect::<Vec<_>>();
        packages.sort_by_key(|(coordinate, _)| *coordinate);

        // The ordered mutex guards are the process-local reservation. Holding the
        // complete set makes the state check and ownership transfer atomic across
        // concurrent builders without a global registry lock.
        let mut guards = Vec::with_capacity(packages.len());
        for (_, package) in &packages {
            guards.push(
                package
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
        for ((_, package), state) in packages.iter().zip(guards.iter()) {
            if matches!(&**state, PendingRegistrationState::Collected) {
                return Err(EffectRegistrationCollectionError::AlreadyCollected {
                    binding: package.logical_name.clone(),
                    effect_type: package.effect_type,
                });
            }
        }

        let mut registry = Self::new();
        let installed = Arc::make_mut(&mut registry.installed);
        let installed_coordinates = Arc::make_mut(&mut registry.installed_coordinates);
        let recipes = Arc::make_mut(&mut registry.recipes);
        for ((_, package), state) in packages.iter().zip(guards.iter_mut()) {
            let PendingRegistrationState::Pending(entries) =
                std::mem::replace(&mut **state, PendingRegistrationState::Collected)
            else {
                unreachable!("all package states were checked while reservations were held")
            };
            installed.insert((
                package.effect_type_id,
                package.logical_name.as_str().to_string(),
            ));
            installed_coordinates.insert(package.coordinate);
            recipes.extend(entries);
        }
        Ok(registry)
    }

    pub(crate) fn into_run_registry(mut self) -> Self {
        let entries = self
            .recipes
            .iter()
            .map(|(coordinate, recipe)| {
                let entry = match recipe {
                    PortRecipe::Eager(port) => RunPortEntry::Eager(port.clone()),
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
    ) -> Result<EffectPortViews, EffectPortViewBuildError> {
        if !self.installed_coordinates.contains(&registration) {
            return Err(EffectPortViewBuildError::MissingRegistration);
        }
        let mut ports = HashMap::with_capacity(slots.len());
        let mut metadata = HashMap::with_capacity(slots.len());
        for slot in slots {
            let resolved = self.resolve(slot)?;
            let key = (
                slot.requirement.port_type_id,
                slot.requirement.metadata_type_id,
                slot.requirement.label,
            );
            ports.insert(key, resolved.port);
            metadata.insert(key, resolved.metadata);
        }
        Ok(EffectPortViews {
            ports: EffectPortView { resolved: ports },
            metadata: EffectPortMetadataView { resolved: metadata },
        })
    }

    fn resolve(
        &self,
        slot: &BoundEffectPortSlot,
    ) -> Result<ErasedResolvedEffectPort, EffectPortViewBuildError> {
        let Some(entries) = self.run_entries.as_ref() else {
            return match self.recipes.get(&slot.coordinate) {
                Some(PortRecipe::Eager(port)) => Ok(port.clone()),
                Some(PortRecipe::Deferred(_)) => Err(EffectPortViewBuildError::Resolver {
                    slot: slot.requirement.label,
                    verdict: ResolverVerdictError::NotMaterialised,
                }),
                None => Err(EffectPortViewBuildError::MissingRegistration),
            };
        };
        let Some(entry) = entries.get(&slot.coordinate) else {
            return Err(EffectPortViewBuildError::MissingRegistration);
        };
        match entry {
            RunPortEntry::Eager(port) => Ok(port.clone()),
            RunPortEntry::Deferred { resolver, verdict } => verdict
                .get_or_init(|| {
                    catch_unwind(AssertUnwindSafe(|| resolver()))
                        .map_err(|_| ResolverVerdictError::Panicked)
                        .and_then(|result| result.map_err(ResolverVerdictError::Provider))
                })
                .as_ref()
                .cloned()
                .map_err(|error| EffectPortViewBuildError::Resolver {
                    slot: slot.requirement.label,
                    verdict: *error,
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
    resolved: HashMap<(TypeId, TypeId, &'static str), ErasedValue>,
}

impl EffectPortView {
    pub(super) fn get<P, M>(&self, slot: EffectPortSlot<P, M>) -> Option<Arc<P>>
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        self.resolved
            .get(&(TypeId::of::<P>(), TypeId::of::<M>(), slot.label()))?
            .downcast_ref::<Arc<P>>()
            .cloned()
    }
}

#[derive(Clone, Default)]
pub(super) struct EffectPortMetadataView {
    resolved: HashMap<(TypeId, TypeId, &'static str), ErasedValue>,
}

impl EffectPortMetadataView {
    pub(super) fn get<P, M>(&self, slot: EffectPortSlot<P, M>) -> Option<Arc<M>>
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        self.resolved
            .get(&(TypeId::of::<P>(), TypeId::of::<M>(), slot.label()))?
            .downcast_ref::<Arc<M>>()
            .cloned()
    }
}

impl std::fmt::Debug for EffectPortMetadataView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectPortMetadataView")
            .field("slot_count", &self.resolved.len())
            .field("metadata", &"<not disclosed>")
            .finish()
    }
}

#[derive(Clone, Default)]
pub(super) struct EffectPortViews {
    pub(super) ports: EffectPortView,
    pub(super) metadata: EffectPortMetadataView,
}

impl EffectPortViews {
    #[cfg(test)]
    fn get<P, M>(&self, slot: EffectPortSlot<P, M>) -> Option<Arc<P>>
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        self.ports.get(slot)
    }
}

impl std::fmt::Debug for EffectPortViews {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectPortViews")
            .field("slot_count", &self.ports.resolved.len())
            .field("authority", &"<not disclosed>")
            .finish()
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
pub(crate) enum EffectPortViewBuildError {
    MissingRegistration,
    Resolver {
        slot: &'static str,
        verdict: ResolverVerdictError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::{
        Effect, EffectBindingEvidence, EffectBindingUse, EffectContext, EffectError,
        EffectPortMetadataContext, EffectPortSlotSet, EffectSafety, Named, RecordedReply,
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

    const AUTHORITY_CANARY: &str = "credential-canary://user:secret@provider.example";

    #[derive(Clone, PartialEq, Eq)]
    struct CanaryEvidence;

    impl EffectBindingEvidence for CanaryEvidence {
        const SCHEMA_VERSION: u32 = 1;

        fn canonical_bytes(&self) -> BoundedBindingEvidence {
            BoundedBindingEvidence::try_new(AUTHORITY_CANARY.as_bytes().to_vec()).unwrap()
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

    #[derive(Debug)]
    struct CanaryLivePort {
        secret: &'static str,
    }

    struct CanaryMetadata {
        secret: &'static str,
    }

    impl Port for CanaryLivePort {
        fn value(&self) -> usize {
            self.secret.len()
        }
    }

    const PORT: EffectPortSlot<dyn Port> = EffectPortSlot::new("client");
    const CANARY_PORT: EffectPortSlot<dyn Port, CanaryMetadata> = EffectPortSlot::new("client");
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
    struct CanaryBoundEffect {
        binding: EffectBindingUse<Self>,
    }

    #[async_trait]
    impl Effect for CanaryBoundEffect {
        const EFFECT_TYPE: &'static str = "binding.fixture.canary_effect";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;
        type BindingMode = Named<CanaryEvidence>;
        type Outcome = Outcome;
        type OutcomeSemantics = RecordedReply;

        fn label(&self) -> &str {
            "canary-fixture"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Outcome)
        }
    }

    impl NamedEffect for CanaryBoundEffect {
        type BindingEvidence = CanaryEvidence;

        fn binding_use(&self) -> &EffectBindingUse<Self> {
            &self.binding
        }

        fn required_slots() -> EffectPortSlotSet {
            EffectPortSlotSet::single(CANARY_PORT)
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

    #[derive(Clone, Debug)]
    struct TransactionalEffectOmittingExecutor {
        binding: EffectBindingUse<Self>,
    }

    #[async_trait]
    impl Effect for TransactionalEffectOmittingExecutor {
        const EFFECT_TYPE: &'static str = "binding.fixture.transactional_without_executor";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Transactional;
        type BindingMode = Named<Evidence>;
        type Outcome = Outcome;
        type OutcomeSemantics = RecordedReply;

        fn label(&self) -> &str {
            "transactional-without-executor"
        }

        fn canonical_input(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(Outcome)
        }
    }

    impl NamedEffect for TransactionalEffectOmittingExecutor {
        type BindingEvidence = Evidence;

        fn binding_use(&self) -> &EffectBindingUse<Self> {
            &self.binding
        }

        fn required_slots() -> EffectPortSlotSet {
            EffectPortSlotSet::new()
        }
    }

    fn name() -> LogicalEffectBindingName {
        LogicalEffectBindingName::new("fixture").unwrap()
    }

    fn collect<E: NamedEffect>(binding: &EffectBinding<E>) -> EffectPortRegistry {
        let declaration = EffectDeclaration::named(binding);
        EffectPortRegistry::collect_from_declarations([&declaration]).unwrap()
    }

    fn bound_binding(logical_name: &str) -> EffectBinding<BoundEffect> {
        EffectRegistrationBuilder::<BoundEffect>::new(
            LogicalEffectBindingName::new(logical_name).unwrap(),
            Evidence,
        )
        .bind_eager(PORT, Arc::new(LivePort) as Arc<dyn Port>)
        .unwrap()
        .finish()
        .unwrap()
    }

    #[test]
    fn exact_slot_builder_returns_one_binding_with_a_collectable_package() {
        let binding = EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
            .bind_eager(PORT, Arc::new(LivePort) as Arc<dyn Port>)
            .unwrap()
            .finish()
            .unwrap();
        let registry = collect(&binding).into_run_registry();
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
    fn failed_multi_package_collection_leaves_every_unclaimed_package_retryable() {
        let already_collected = bound_binding("already_collected");
        let retryable = bound_binding("retryable");
        let _first_registry = collect(&already_collected);

        let already_collected_declaration = EffectDeclaration::named(&already_collected);
        let retryable_declaration = EffectDeclaration::named(&retryable);
        let error = match EffectPortRegistry::collect_from_declarations([
            &retryable_declaration,
            &already_collected_declaration,
        ]) {
            Ok(_) => panic!("a set containing a consumed package must fail atomically"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            EffectRegistrationCollectionError::AlreadyCollected { ref binding, .. }
                if binding.as_str() == "already_collected"
        ));

        let _retry_registry = collect(&retryable);
    }

    #[test]
    fn distinct_same_key_bindings_fail_in_either_order_without_consuming_packages() {
        let first = bound_binding("duplicate");
        let second = bound_binding("duplicate");
        let first_declaration = EffectDeclaration::named(&first);
        let second_declaration = EffectDeclaration::named(&second);

        let forward = EffectPortRegistry::collect_from_declarations([
            &first_declaration,
            &second_declaration,
        ])
        .expect_err("distinct bindings for one key must be rejected");
        let reverse = EffectPortRegistry::collect_from_declarations([
            &second_declaration,
            &first_declaration,
        ])
        .expect_err("declaration order must not select a binding");

        assert_eq!(forward, reverse);
        assert!(matches!(
            forward,
            EffectRegistrationCollectionError::DistinctBindings { ref binding, .. }
                if binding.as_str() == "duplicate"
        ));

        let _first_registry = collect(&first);
        let _second_registry = collect(&second);
    }

    #[test]
    fn overlapping_concurrent_collections_have_one_winner_and_no_partial_loser() {
        let left_only = bound_binding("left_only");
        let shared = bound_binding("shared");
        let right_only = bound_binding("right_only");

        let left_declarations = [
            EffectDeclaration::named(&left_only),
            EffectDeclaration::named(&shared),
        ];
        let right_declarations = [
            EffectDeclaration::named(&shared),
            EffectDeclaration::named(&right_only),
        ];
        let left = std::thread::spawn(move || {
            EffectPortRegistry::collect_from_declarations(left_declarations.iter())
        });
        let right = std::thread::spawn(move || {
            EffectPortRegistry::collect_from_declarations(right_declarations.iter())
        });

        let left = left.join().expect("left collector does not panic");
        let right = right.join().expect("right collector does not panic");
        assert_ne!(left.is_ok(), right.is_ok(), "exactly one overlap may win");
        if left.is_ok() {
            assert!(matches!(
                right,
                Err(EffectRegistrationCollectionError::AlreadyCollected { .. })
            ));
            let _right_only_registry = collect(&right_only);
        } else {
            assert!(matches!(
                left,
                Err(EffectRegistrationCollectionError::AlreadyCollected { .. })
            ));
            let _left_only_registry = collect(&left_only);
        }
    }

    #[test]
    fn runtime_projection_is_inert_and_does_not_consume_the_authored_package() {
        let binding = bound_binding("projection");
        let authored = EffectDeclaration::named(&binding);
        let runtime = authored.runtime_projection();
        let error = match EffectPortRegistry::collect_from_declarations([&runtime]) {
            Ok(_) => panic!("runtime declarations cannot carry authoring packages"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            EffectRegistrationCollectionError::MissingPackage { ref binding, .. }
                if binding.as_str() == "projection"
        ));

        let _registry = EffectPortRegistry::collect_from_declarations([&authored]).unwrap();
    }

    #[test]
    fn transactional_registration_structurally_requires_the_reserved_executor() {
        let error =
            EffectRegistrationBuilder::<TransactionalEffectOmittingExecutor>::new(name(), Evidence)
                .finish()
                .expect_err("transactional bindings cannot omit their reserved executor");

        assert!(matches!(
            error,
            EffectBindingBuildError::MissingSlots { slots, .. }
                if slots == vec!["executor"]
        ));
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
        let binding = EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
            .bind_deferred(PORT, resolver)
            .unwrap()
            .finish()
            .unwrap();
        let registry = collect(&binding).into_run_registry();
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
        let binding = EffectRegistrationBuilder::<TwoSlotEffect>::new(name(), Evidence)
            .bind_deferred(PORT, first)
            .unwrap()
            .bind_deferred(FALLBACK, second)
            .unwrap()
            .finish()
            .unwrap();
        let registry = collect(&binding).into_run_registry();
        let super::super::binding::EffectDeclarationBinding::Named(named) =
            binding.declaration_binding()
        else {
            panic!("expected named declaration")
        };

        for _ in 0..2 {
            let error = registry
                .scoped_view(named.registration, &named.slots)
                .expect_err("a failed second slot cannot expose the first slot");
            assert!(matches!(
                error,
                EffectPortViewBuildError::Resolver {
                    slot: "fallback",
                    verdict: ResolverVerdictError::Provider(
                        EffectPortResolutionError::ClientConstructionFailed
                    ),
                }
            ));
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
        let binding = EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
            .bind_deferred(PORT, resolver)
            .unwrap()
            .finish()
            .unwrap();
        let registry = collect(&binding).into_run_registry();
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
        let binding = EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
            .bind_deferred(PORT, resolver)
            .unwrap()
            .finish()
            .unwrap();
        let registry = collect(&binding);
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
        let binding = EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
            .bind_deferred(PORT, resolver)
            .unwrap()
            .finish()
            .unwrap();
        let registry = collect(&binding);
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
                assert!(matches!(
                    error,
                    EffectPortViewBuildError::Resolver {
                        slot: "client",
                        verdict: ResolverVerdictError::Panicked,
                    }
                ));
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn authority_debug_surfaces_do_not_disclose_evidence_or_ports() {
        let binding = EffectRegistrationBuilder::<BoundEffect>::new(name(), Evidence)
            .bind_eager(PORT, Arc::new(LivePort) as Arc<dyn Port>)
            .unwrap()
            .finish()
            .unwrap();
        let binding_debug = format!("{binding:?}");
        let registration_debug = format!(
            "{:?}",
            binding
                .declaration_binding()
                .pending_package()
                .expect("authored binding carries its pending package")
        );
        assert!(binding_debug.contains("<not disclosed>"));
        assert!(registration_debug.contains("<not disclosed>"));
        assert!(!registration_debug.contains("LivePort"));
    }

    #[test]
    fn authority_canaries_are_absent_from_framework_projections() {
        let resolved = ResolvedEffectPort::new(
            Arc::new(CanaryLivePort {
                secret: AUTHORITY_CANARY,
            }) as Arc<dyn Port>,
            Arc::new(CanaryMetadata {
                secret: AUTHORITY_CANARY,
            }),
        );
        let resolved_debug = format!("{resolved:?}");
        let builder = EffectRegistrationBuilder::<CanaryBoundEffect>::new(
            LogicalEffectBindingName::new("canary").unwrap(),
            CanaryEvidence,
        )
        .bind_eager_with_metadata(CANARY_PORT, resolved)
        .unwrap();
        let builder_debug = format!("{builder:?}");
        let binding = builder.finish().unwrap();
        let registration_debug = format!(
            "{:?}",
            binding
                .declaration_binding()
                .pending_package()
                .expect("authored binding carries its pending package")
        );
        let declaration = EffectDeclaration::named(&binding);
        let declaration_debug = format!("{declaration:?}");
        let durable_identity =
            serde_json::to_string(&declaration.binding_identity()).expect("identity serializes");
        let effect_debug = format!(
            "{:?}",
            CanaryBoundEffect {
                binding: binding.invocation(),
            }
        );
        let binding_debug = format!("{binding:?}");
        let run = collect(&binding).into_run_registry();
        let run_debug = format!("{run:?}");
        let super::super::binding::EffectDeclarationBinding::Named(named) =
            binding.declaration_binding()
        else {
            panic!("expected named declaration")
        };
        let view = run.scoped_view(named.registration, &named.slots).unwrap();
        assert_eq!(
            view.get(CANARY_PORT).unwrap().value(),
            AUTHORITY_CANARY.len()
        );
        assert_eq!(
            view.metadata.get(CANARY_PORT).unwrap().secret.len(),
            AUTHORITY_CANARY.len()
        );
        let view_debug = format!("{view:?}");
        let metadata_context_debug = format!(
            "{:?}",
            EffectPortMetadataContext {
                metadata: view.metadata.clone(),
            }
        );

        let resolver: EffectPortResolverWithMetadata<dyn Port, CanaryMetadata> = Arc::new(|| {
            panic!("{AUTHORITY_CANARY}");
        });
        let panic_binding = EffectRegistrationBuilder::<CanaryBoundEffect>::new(
            LogicalEffectBindingName::new("panic_canary").unwrap(),
            CanaryEvidence,
        )
        .bind_deferred_with_metadata(CANARY_PORT, resolver)
        .unwrap()
        .finish()
        .unwrap();
        let panic_registry = collect(&panic_binding).into_run_registry();
        let super::super::binding::EffectDeclarationBinding::Named(panic_named) =
            panic_binding.declaration_binding()
        else {
            panic!("expected named declaration")
        };
        let panic_error = panic_registry
            .scoped_view(panic_named.registration, &panic_named.slots)
            .expect_err("panic becomes a closed resolver verdict");
        let panic_error_debug = format!("{panic_error:?}");
        let fault = BindingAuthorityFault::resolution_failed(
            CanaryBoundEffect::EFFECT_TYPE,
            panic_binding.logical_name().clone(),
            "client",
        );
        let fatal = fault.stage_fatal();
        let effect_error = EffectError::BindingAuthority {
            fault: fault.clone(),
        };
        let framework_surfaces = [
            builder_debug,
            resolved_debug,
            registration_debug,
            declaration_debug,
            durable_identity,
            effect_debug,
            binding_debug,
            run_debug,
            view_debug,
            metadata_context_debug,
            panic_error_debug,
            fault.to_string(),
            format!("{fault:?}"),
            fatal.detail,
            format!("{:?}", fatal.reason),
            effect_error.to_string(),
            format!("{effect_error:?}"),
        ];
        for projection in framework_surfaces {
            assert!(
                !projection.contains(AUTHORITY_CANARY),
                "framework projection disclosed authority canary: {projection}"
            );
        }
    }

    #[test]
    fn callable_port_and_metadata_share_one_cached_resolver_verdict() {
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver: EffectPortResolverWithMetadata<dyn Port, CanaryMetadata> = Arc::new({
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(ResolvedEffectPort::new(
                    Arc::new(LivePort) as Arc<dyn Port>,
                    Arc::new(CanaryMetadata { secret: "snapshot" }),
                ))
            }
        });
        let binding = EffectRegistrationBuilder::<CanaryBoundEffect>::new(
            LogicalEffectBindingName::new("co_resolved").unwrap(),
            CanaryEvidence,
        )
        .bind_deferred_with_metadata(CANARY_PORT, resolver)
        .unwrap()
        .finish()
        .unwrap();
        let registry = collect(&binding).into_run_registry();
        let super::super::binding::EffectDeclarationBinding::Named(named) =
            binding.declaration_binding()
        else {
            panic!("expected named declaration")
        };

        for _ in 0..2 {
            let views = registry
                .scoped_view(named.registration, &named.slots)
                .unwrap();
            assert_eq!(views.get(CANARY_PORT).unwrap().value(), 7);
            assert_eq!(views.metadata.get(CANARY_PORT).unwrap().secret, "snapshot");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
