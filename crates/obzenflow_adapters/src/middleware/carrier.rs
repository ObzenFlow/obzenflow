// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115b: the public hook-declaration carrier.
//!
//! A middleware factory declares, before runtime erasure, which surface and
//! capability it attaches to, and materializes one typed surface attachment for
//! one concrete protected unit. The DSL binder (`obzenflow_dsl`) is the only
//! layer that sees both these adapter-owned carrier types and the
//! runtime/infra neutral boundary seams; runtime and infra receive only neutral
//! seams.
//!
//! The carrier covers hook-bound control policy and observe-only runtime
//! observer ports. Adapter factories declare intent here; the DSL validates
//! surface/capability compatibility and passes only typed runtime/infra ports
//! across the architectural boundary.

use super::control::policy::{
    EffectPolicy, EffectPolicyAttachment, EventAwareEffectPolicy, SinkPolicy, SourcePolicy,
};
use super::control::provider::PendingControlRegistration;
use obzenflow_core::event::context::StageType;
use obzenflow_core::ingress::{IngressBoundaryMiddleware, IngressKey};
use obzenflow_core::{StageId, StageKey};
use obzenflow_runtime::effects::EffectSafety;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::observer::{
    EffectObserver, HandlerObserver, JoinObserver, OutputCommitObserver, SinkDeliveryObserver,
    SourcePollObserver, StageLifecycleObserver, StatefulObserver,
};
use obzenflow_runtime::stages::source::strategies::CompletionGate;
use ring::digest::{Context, SHA256};
use std::sync::{Arc, Mutex};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Typed identity newtypes (FLOWIP-115b "Typed identity")
// ---------------------------------------------------------------------------

/// Effect-type key for the carrier.
///
/// Effects are keyed by `&'static str` at the runtime effect boundary, so the
/// carrier carries an owned typed value and bridges to the static key when it
/// builds the per-effect attachment. This is a semantic key, not a GUID-style
/// identity, so it deliberately does not use the `Id` suffix.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectTypeKey(pub String);

impl EffectTypeKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for EffectTypeKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for EffectTypeKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// A sink-delivery target declared before consume (replay-stable). Refines the
/// sink-delivery protected unit beyond the default stage-level identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SinkConfiguredTargetKey(pub String);

// ---------------------------------------------------------------------------
// Capability
// ---------------------------------------------------------------------------

/// Middleware capability. The binder validates `surface x capability`: a broad
/// surface vocabulary does not imply every capability is legal everywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareCapability {
    Control,
    Observer,
}

// ---------------------------------------------------------------------------
// Surface vocabulary
// ---------------------------------------------------------------------------

/// The source-poll call site for one source stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePollSurface {
    pub stage_id: StageId,
}

/// The effect call site for one declared effect on a stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectSurface {
    pub stage_id: StageId,
    pub effect_type: EffectTypeKey,
    pub safety: EffectSafety,
}

/// The sink-delivery call site for one sink stage, optionally refined by a
/// configured target declared before consume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkDeliverySurface {
    pub stage_id: StageId,
    pub configured_target: Option<SinkConfiguredTargetKey>,
}

// ---------------------------------------------------------------------------
// Ingress identity (FLOWIP-115d)
// ---------------------------------------------------------------------------

// FLOWIP-114e: the ingress stage key and the hosted-surface key are the core
// newtypes `StageKey` and `IngressKey`. The carrier reuses them rather than
// minting adapter-local string newtypes for the same concepts.

/// Which work-admission endpoint a hosted ingress route belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IngressEndpointKind {
    Events,
    Batch,
}

/// The admission route scope. The default `Admission` scope is shared by the
/// work-admission endpoints of one hosted surface, so a client cannot bypass the
/// limiter by switching submission endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IngressRouteScope {
    Admission,
    Endpoint(IngressEndpointKind),
}

impl IngressRouteScope {
    fn label(&self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::Endpoint(IngressEndpointKind::Events) => "endpoint:events",
            Self::Endpoint(IngressEndpointKind::Batch) => "endpoint:batch",
        }
    }
}

/// The concrete hosted ingress target: a hosted surface plus its admission route
/// scope. Target metadata under the source-stage owner, not a protected-unit
/// owner on its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostedIngressTargetKey {
    pub surface: IngressKey,
    pub scope: IngressRouteScope,
}

/// The source stage that owns a source-backed hosted ingress surface. Ingress
/// identity is source-stage-owned: the owner is the linked source stage, not the
/// raw base path and not a fresh hosted-surface id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceStageIngressOwner {
    pub stage_id: StageId,
    pub stage_key: StageKey,
}

/// The ingress call site for one source-backed hosted ingress target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressSurface {
    pub owner: SourceStageIngressOwner,
    pub target: HostedIngressTargetKey,
}

/// The ingress protected unit: cross-attempt, keyed by the replay-stable source
/// stage key plus the typed hosted ingress target. Request-scoped facts (event
/// id, batch index, counts, retry-after, transport metadata, `IngressAttemptSeq`)
/// are attempt context and never enter this key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IngressUnitId {
    pub source_stage_key: StageKey,
    pub target: HostedIngressTargetKey,
}

/// The call-site shape a middleware attaches to. Broad and non-exhaustive so
/// later slices consume the same foundation. This slice implements
/// `SourcePoll`, `Effect`, and `SinkDelivery`; the remaining variants are
/// reserved (FLOWIP-115d ingress, FLOWIP-115f observers).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MiddlewareSurface {
    SourcePoll(SourcePollSurface),
    Effect(EffectSurface),
    SinkDelivery(SinkDeliverySurface),
    /// FLOWIP-115d: source-backed hosted listener admission (not source polling).
    Ingress(IngressSurface),
    Handler {
        stage_id: StageId,
    },
    Stateful {
        stage_id: StageId,
    },
    Join {
        stage_id: StageId,
    },
    OutputCommit {
        stage_id: StageId,
    },
    StageLifecycle {
        stage_id: StageId,
    },
}

/// A lightweight discriminant of a surface, used in deterministic attachment
/// identity and in `surface x capability` validation without dragging the
/// surface payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MiddlewareSurfaceKind {
    SourcePoll,
    Effect,
    SinkDelivery,
    Ingress,
    Handler,
    Stateful,
    Join,
    OutputCommit,
    StageLifecycle,
}

impl MiddlewareSurface {
    pub fn kind(&self) -> MiddlewareSurfaceKind {
        match self {
            Self::SourcePoll(_) => MiddlewareSurfaceKind::SourcePoll,
            Self::Effect(_) => MiddlewareSurfaceKind::Effect,
            Self::SinkDelivery(_) => MiddlewareSurfaceKind::SinkDelivery,
            Self::Ingress(_) => MiddlewareSurfaceKind::Ingress,
            Self::Handler { .. } => MiddlewareSurfaceKind::Handler,
            Self::Stateful { .. } => MiddlewareSurfaceKind::Stateful,
            Self::Join { .. } => MiddlewareSurfaceKind::Join,
            Self::OutputCommit { .. } => MiddlewareSurfaceKind::OutputCommit,
            Self::StageLifecycle { .. } => MiddlewareSurfaceKind::StageLifecycle,
        }
    }

    /// The stage this surface attaches to, where the surface names one.
    pub fn stage_id(&self) -> Option<StageId> {
        match self {
            Self::SourcePoll(s) => Some(s.stage_id),
            Self::Effect(s) => Some(s.stage_id),
            Self::SinkDelivery(s) => Some(s.stage_id),
            // FLOWIP-115d: source-backed hosted ingress is owned by its linked
            // source stage, so it returns that stage id and keeps the existing
            // stage-keyed validation flow additive.
            Self::Ingress(s) => Some(s.owner.stage_id),
            Self::Handler { stage_id }
            | Self::Stateful { stage_id }
            | Self::Join { stage_id }
            | Self::OutputCommit { stage_id }
            | Self::StageLifecycle { stage_id } => Some(*stage_id),
        }
    }
}

impl MiddlewareSurfaceKind {
    /// Whether a `Control`-capability middleware may attach to this surface in
    /// this slice. Control is legal on the live-I/O boundary surfaces (source
    /// poll, effect, sink delivery) and, since FLOWIP-115d, on hosted ingress;
    /// `OutputCommit` is never a control surface, and the remaining reserved
    /// surfaces are owned by later slices.
    pub fn allows_control(self) -> bool {
        matches!(
            self,
            Self::SourcePoll | Self::Effect | Self::SinkDelivery | Self::Ingress
        )
    }
}

// ---------------------------------------------------------------------------
// Protected unit
// ---------------------------------------------------------------------------

/// The source-poll protected unit. There is one source-poll unit per source
/// stage, so it carries no further discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourcePollUnitId;

/// One declared-effect protected unit, keyed by effect type within the stage.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EffectUnitId {
    pub effect_type: EffectTypeKey,
}

/// Default stage-level sink delivery, or a refined configured target.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SinkDeliveryTarget {
    Stage,
    Configured(SinkConfiguredTargetKey),
}

/// The sink-delivery protected unit, stage-level by default.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SinkDeliveryUnitId {
    pub target: SinkDeliveryTarget,
}

/// The cross-attempt state and accounting key for one attached unit.
///
/// This must not carry per-attempt data (effect cursor, event id, request
/// metadata, batch index, input position); those live in invocation/attempt
/// contexts. Non-exhaustive so later slices add ingress and observer units.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProtectedUnit {
    SourcePoll(SourcePollUnitId),
    Effect(EffectUnitId),
    SinkDelivery(SinkDeliveryUnitId),
    /// FLOWIP-115d: source-backed hosted listener admission.
    Ingress(IngressUnitId),
    /// Reserved (FLOWIP-115f).
    Handler,
    Stateful,
    Join,
    OutputCommit,
    StageLifecycle,
}

/// Identity of one protected unit: the cross-attempt accounting key.
///
/// Keyed by `stage_id` (unique within the flow-scoped binding, the same key the
/// control aggregator uses) plus the unit. There is no separate `flow_id`: the
/// binding site carries only the stage id, and the stage id already identifies
/// the unit within the flow-scoped aggregator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProtectedUnitId {
    pub stage_id: StageId,
    pub unit: ProtectedUnit,
}

/// The grammar position that produced an attachment.
///
/// These variants are archive vocabulary. Their stable labels are written into
/// the attachment-id preimage explicitly; Rust discriminants are never hashed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MiddlewareDeclarationPosition {
    SourceWith,
    IngressWith,
    EffectWith,
    SinkWith,
    Observers,
}

impl MiddlewareDeclarationPosition {
    pub const fn stable_label(self) -> &'static str {
        match self {
            Self::SourceWith => "source_with",
            Self::IngressWith => "ingress_with",
            Self::EffectWith => "effect_with",
            Self::SinkWith => "sink_with",
            Self::Observers => "observers",
        }
    }

    pub const fn syntax_label(self) -> &'static str {
        match self {
            Self::SourceWith => "source with",
            Self::IngressWith => "ingress with",
            Self::EffectWith => "effect with",
            Self::SinkWith => "sink with",
            Self::Observers => "observers:",
        }
    }
}

/// Replay-stable ordinal within one grammar-owned declaration position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MiddlewareDeclarationIndex {
    position: MiddlewareDeclarationPosition,
    ordinal: u64,
}

impl MiddlewareDeclarationIndex {
    const fn new(position: MiddlewareDeclarationPosition, ordinal: u64) -> Self {
        Self { position, ordinal }
    }

    pub fn source_with(ordinal: usize) -> Self {
        Self::new(MiddlewareDeclarationPosition::SourceWith, ordinal as u64)
    }

    pub const fn ingress_with() -> Self {
        Self::new(MiddlewareDeclarationPosition::IngressWith, 0)
    }

    pub const fn effect_with() -> Self {
        Self::new(MiddlewareDeclarationPosition::EffectWith, 0)
    }

    pub fn sink_with(ordinal: usize) -> Self {
        Self::new(MiddlewareDeclarationPosition::SinkWith, ordinal as u64)
    }

    pub fn observers(ordinal: usize) -> Self {
        Self::new(MiddlewareDeclarationPosition::Observers, ordinal as u64)
    }

    pub const fn position(self) -> MiddlewareDeclarationPosition {
        self.position
    }

    pub const fn ordinal(self) -> u64 {
        self.ordinal
    }

    #[cfg(test)]
    fn for_test(position: MiddlewareDeclarationPosition, ordinal: u64) -> Self {
        Self { position, ordinal }
    }
}

// ---------------------------------------------------------------------------
// Attachment identity
// ---------------------------------------------------------------------------

/// Deterministic, structurally-derived attachment identity (FLOWIP-115b).
///
/// Derived from replay-stable binding coordinates, never a fresh
/// materialization-time ULID, so it survives strict replay and archive-drift
/// checks. The hashed coordinate includes declaration schema, label, family,
/// declaration order, concrete surface, and protected unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MiddlewareAttachmentId(obzenflow_core::Ulid);

impl MiddlewareAttachmentId {
    /// Deterministically derive the attachment id from replay-stable binding
    /// coordinates. This is never materialization-time randomness.
    pub fn from_declaration_and_request(
        declaration: &MiddlewareDeclaration,
        request: &MiddlewareAttachmentRequest<'_>,
    ) -> Self {
        let mut context = Context::new(&SHA256);
        push_field(&mut context, "schema", "middleware-attachment:v5");
        push_field(&mut context, "middleware.label", declaration.label);
        push_field(&mut context, "middleware.family", declaration.family_label);
        push_declaration_index(&mut context, request.declaration_index);
        push_surface(&mut context, request.surface);
        push_protected_unit(&mut context, request.protected_unit);
        let hash = context.finish();
        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&hash.as_ref()[..16]);
        Self(obzenflow_core::Ulid(u128::from_be_bytes(id_bytes)))
    }

    pub fn as_ulid(&self) -> obzenflow_core::Ulid {
        self.0
    }
}

fn push_field(context: &mut Context, label: &str, value: &str) {
    context.update(label.as_bytes());
    context.update(b"\0");
    context.update(value.len().to_string().as_bytes());
    context.update(b"\0");
    context.update(value.as_bytes());
    context.update(b"\0");
}

fn push_stage_id(context: &mut Context, label: &str, stage_id: StageId) {
    push_field(context, label, &stage_id.as_ulid().to_string());
}

fn push_declaration_index(context: &mut Context, index: MiddlewareDeclarationIndex) {
    push_field(
        context,
        "declaration.position",
        index.position().stable_label(),
    );
    push_field(context, "declaration.ordinal", &index.ordinal().to_string());
}

fn push_surface(context: &mut Context, surface: &MiddlewareSurface) {
    match surface {
        MiddlewareSurface::SourcePoll(surface) => {
            push_field(context, "surface.kind", "source_poll");
            push_stage_id(context, "surface.stage_id", surface.stage_id);
        }
        MiddlewareSurface::Effect(surface) => {
            push_field(context, "surface.kind", "effect");
            push_stage_id(context, "surface.stage_id", surface.stage_id);
            push_field(context, "surface.effect_type", surface.effect_type.as_str());
        }
        MiddlewareSurface::SinkDelivery(surface) => {
            push_field(context, "surface.kind", "sink_delivery");
            push_stage_id(context, "surface.stage_id", surface.stage_id);
            match &surface.configured_target {
                Some(target) => {
                    push_field(context, "surface.sink_target.kind", "configured");
                    push_field(context, "surface.sink_target", &target.0);
                }
                None => push_field(context, "surface.sink_target.kind", "stage"),
            }
        }
        MiddlewareSurface::Ingress(surface) => {
            push_field(context, "surface.kind", "ingress");
            push_stage_id(context, "surface.stage_id", surface.owner.stage_id);
            push_field(
                context,
                "surface.ingress_stage_key",
                &surface.owner.stage_key.0,
            );
            push_field(
                context,
                "surface.ingress_hosted_surface",
                &surface.target.surface.0,
            );
            push_field(
                context,
                "surface.ingress_route_scope",
                surface.target.scope.label(),
            );
        }
        MiddlewareSurface::Handler { stage_id } => {
            push_field(context, "surface.kind", "handler");
            push_stage_id(context, "surface.stage_id", *stage_id);
        }
        MiddlewareSurface::Stateful { stage_id } => {
            push_field(context, "surface.kind", "stateful");
            push_stage_id(context, "surface.stage_id", *stage_id);
        }
        MiddlewareSurface::Join { stage_id } => {
            push_field(context, "surface.kind", "join");
            push_stage_id(context, "surface.stage_id", *stage_id);
        }
        MiddlewareSurface::OutputCommit { stage_id } => {
            push_field(context, "surface.kind", "output_commit");
            push_stage_id(context, "surface.stage_id", *stage_id);
        }
        MiddlewareSurface::StageLifecycle { stage_id } => {
            push_field(context, "surface.kind", "stage_lifecycle");
            push_stage_id(context, "surface.stage_id", *stage_id);
        }
    }
}

fn push_protected_unit(context: &mut Context, protected_unit: &ProtectedUnitId) {
    push_stage_id(context, "protected_unit.stage_id", protected_unit.stage_id);
    match &protected_unit.unit {
        ProtectedUnit::SourcePoll(_) => push_field(context, "protected_unit.kind", "source_poll"),
        ProtectedUnit::Effect(unit) => {
            push_field(context, "protected_unit.kind", "effect");
            push_field(
                context,
                "protected_unit.effect_type",
                unit.effect_type.as_str(),
            );
        }
        ProtectedUnit::SinkDelivery(unit) => match &unit.target {
            SinkDeliveryTarget::Stage => {
                push_field(context, "protected_unit.kind", "sink_delivery");
                push_field(context, "protected_unit.sink_target.kind", "stage");
            }
            SinkDeliveryTarget::Configured(target) => {
                push_field(context, "protected_unit.kind", "sink_delivery");
                push_field(context, "protected_unit.sink_target.kind", "configured");
                push_field(context, "protected_unit.sink_target", &target.0);
            }
        },
        ProtectedUnit::Ingress(unit) => {
            push_field(context, "protected_unit.kind", "ingress");
            push_field(
                context,
                "protected_unit.ingress_stage_key",
                &unit.source_stage_key.0,
            );
            push_field(
                context,
                "protected_unit.ingress_hosted_surface",
                &unit.target.surface.0,
            );
            push_field(
                context,
                "protected_unit.ingress_route_scope",
                unit.target.scope.label(),
            );
        }
        ProtectedUnit::Handler => push_field(context, "protected_unit.kind", "handler"),
        ProtectedUnit::Stateful => push_field(context, "protected_unit.kind", "stateful"),
        ProtectedUnit::Join => push_field(context, "protected_unit.kind", "join"),
        ProtectedUnit::OutputCommit => {
            push_field(context, "protected_unit.kind", "output_commit");
        }
        ProtectedUnit::StageLifecycle => {
            push_field(context, "protected_unit.kind", "stage_lifecycle");
        }
    }
}

// ---------------------------------------------------------------------------
// Declaration (pre-erasure)
// ---------------------------------------------------------------------------

/// The static declaration a factory returns before any runtime middleware
/// object is created. The DSL reads this to plan the attachment, and the
/// adapter-owned materialisation gateway validates `surface x capability`
/// before invoking the factory.
#[derive(Debug, Clone)]
pub struct MiddlewareDeclaration {
    pub label: &'static str,
    pub family_label: &'static str,
    pub capability: MiddlewareCapability,
    /// The surfaces this factory can attach to. A control middleware may span
    /// several (the rate limiter declares source poll, effect, sink delivery,
    /// and ingress); the grammar position's typed binder supplies the concrete
    /// surface and this carrier validates membership. A declaration always
    /// names at least one surface.
    pub surfaces: Vec<MiddlewareSurfaceKind>,
    materialization_claim: MaterializationClaim,
}

/// Sealed semantic identity shared by structural composition validation,
/// materialisation authority, and returned-attachment validation. Public
/// declaration and attachment constructors can create only `Ordinary`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaterializationClaim {
    Ordinary,
    CircuitBreaker,
    RateLimiter,
    EffectResilience,
}

impl MaterializationClaim {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::CircuitBreaker => "circuit_breaker",
            Self::RateLimiter => "rate_limiter",
            Self::EffectResilience => "effect_resilience",
        }
    }
}

impl MiddlewareDeclaration {
    /// A hook-bound control declaration spanning the given surfaces.
    pub fn control(label: &'static str, surfaces: Vec<MiddlewareSurfaceKind>) -> Self {
        Self::control_with_family(label, label, surfaces)
    }

    /// A hook-bound control declaration with an explicit override family.
    pub fn control_with_family(
        label: &'static str,
        family_label: &'static str,
        surfaces: Vec<MiddlewareSurfaceKind>,
    ) -> Self {
        Self {
            label,
            family_label,
            capability: MiddlewareCapability::Control,
            surfaces,
            materialization_claim: MaterializationClaim::Ordinary,
        }
    }

    pub(crate) fn circuit_breaker(
        label: &'static str,
        family_label: &'static str,
        surfaces: Vec<MiddlewareSurfaceKind>,
    ) -> Self {
        Self {
            label,
            family_label,
            capability: MiddlewareCapability::Control,
            surfaces,
            materialization_claim: MaterializationClaim::CircuitBreaker,
        }
    }

    pub(crate) fn effect_resilience(label: &'static str, family_label: &'static str) -> Self {
        Self {
            label,
            family_label,
            capability: MiddlewareCapability::Control,
            surfaces: vec![MiddlewareSurfaceKind::Effect],
            materialization_claim: MaterializationClaim::EffectResilience,
        }
    }

    pub(crate) fn rate_limiter(
        label: &'static str,
        family_label: &'static str,
        surfaces: Vec<MiddlewareSurfaceKind>,
    ) -> Self {
        Self {
            label,
            family_label,
            capability: MiddlewareCapability::Control,
            surfaces,
            materialization_claim: MaterializationClaim::RateLimiter,
        }
    }

    /// A hook-bound observer declaration spanning the given surfaces.
    pub fn observer(label: &'static str, surfaces: Vec<MiddlewareSurfaceKind>) -> Self {
        Self::observer_with_family(label, label, surfaces)
    }

    /// A hook-bound observer declaration with an explicit override family.
    pub fn observer_with_family(
        label: &'static str,
        family_label: &'static str,
        surfaces: Vec<MiddlewareSurfaceKind>,
    ) -> Self {
        Self {
            label,
            family_label,
            capability: MiddlewareCapability::Observer,
            surfaces,
            materialization_claim: MaterializationClaim::Ordinary,
        }
    }

    /// Whether this factory declares it can attach to `surface`.
    pub fn supports(&self, surface: MiddlewareSurfaceKind) -> bool {
        self.surfaces.contains(&surface)
    }

    /// Whether this is a control-capability declaration.
    pub fn is_control(&self) -> bool {
        matches!(self.capability, MiddlewareCapability::Control)
    }

    pub fn is_observer(&self) -> bool {
        matches!(self.capability, MiddlewareCapability::Observer)
    }

    /// Whether this declaration carries the sealed aggregate effect-resilience
    /// identity. Generated composites use this read-only predicate to require
    /// their fixed effect protocol without exposing construction authority.
    #[doc(hidden)]
    pub fn is_effect_resilience(&self) -> bool {
        self.materialization_claim == MaterializationClaim::EffectResilience
    }

    pub(crate) fn materialization_claim(&self) -> MaterializationClaim {
        self.materialization_claim
    }

    /// Validate the declaration independently of a concrete binding request.
    /// Factories with no surfaces fail at flow build instead of being assigned
    /// an implicit shell meaning.
    pub fn validate_shape(&self) -> Result<(), MiddlewareAttachmentValidationError> {
        if self.surfaces.is_empty() {
            return Err(MiddlewareAttachmentValidationError::EmptyDeclaration {
                label: self.label,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EffectControlCompositionError {
    #[error(
        "effect '{effect_type}' on stage '{stage}' combines EffectResilience with standalone effect control(s): {conflicts:?}"
    )]
    AggregateWithStandalone {
        stage: String,
        effect_type: String,
        conflicts: Vec<&'static str>,
    },
    #[error(
        "effect '{effect_type}' on stage '{stage}' declares EffectResilience more than once: {labels:?}"
    )]
    DuplicateAggregate {
        stage: String,
        effect_type: String,
        labels: Vec<&'static str>,
    },
    #[error(
        "effect '{effect_type}' on stage '{stage}' declares standalone {control} more than once: {labels:?}"
    )]
    DuplicateStandalone {
        stage: String,
        effect_type: String,
        control: &'static str,
        labels: Vec<&'static str>,
    },
}

/// Validate one effect's complete structural control set before any factory is
/// materialised or any breaker/limiter authority is registered.
#[doc(hidden)]
pub fn validate_effect_control_composition(
    stage: &str,
    effect_type: &str,
    declarations: &[MiddlewareDeclaration],
) -> Result<(), EffectControlCompositionError> {
    let effect_controls = declarations.iter().filter(|declaration| {
        declaration.is_control() && declaration.supports(MiddlewareSurfaceKind::Effect)
    });
    let mut aggregates = Vec::new();
    let mut standalone: std::collections::BTreeMap<&'static str, Vec<&'static str>> =
        std::collections::BTreeMap::new();
    for declaration in effect_controls {
        match declaration.materialization_claim {
            MaterializationClaim::EffectResilience => aggregates.push(declaration.label),
            MaterializationClaim::CircuitBreaker => standalone
                .entry("circuit_breaker")
                .or_default()
                .push(declaration.label),
            MaterializationClaim::RateLimiter => standalone
                .entry("rate_limiter")
                .or_default()
                .push(declaration.label),
            MaterializationClaim::Ordinary => {}
        }
    }
    if aggregates.len() > 1 {
        return Err(EffectControlCompositionError::DuplicateAggregate {
            stage: stage.to_string(),
            effect_type: effect_type.to_string(),
            labels: aggregates,
        });
    }
    if !aggregates.is_empty() && !standalone.is_empty() {
        return Err(EffectControlCompositionError::AggregateWithStandalone {
            stage: stage.to_string(),
            effect_type: effect_type.to_string(),
            conflicts: standalone.keys().copied().collect(),
        });
    }
    if let Some((control, labels)) = standalone.into_iter().find(|(_, labels)| labels.len() > 1) {
        return Err(EffectControlCompositionError::DuplicateStandalone {
            stage: stage.to_string(),
            effect_type: effect_type.to_string(),
            control,
            labels,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Attachment validation
// ---------------------------------------------------------------------------

/// A carrier-level validation failure before runtime erasure.
#[derive(Debug, Error)]
pub enum MiddlewareAttachmentValidationError {
    #[error("middleware '{label}' declares no typed surfaces")]
    EmptyDeclaration { label: &'static str },

    #[error("middleware '{label}' does not declare support for surface {surface:?}")]
    UnsupportedSurface {
        label: &'static str,
        surface: MiddlewareSurfaceKind,
    },

    #[error("middleware '{label}' declares capability {capability:?}, which cannot attach to surface {surface:?}")]
    UnsupportedCapability {
        label: &'static str,
        capability: MiddlewareCapability,
        surface: MiddlewareSurfaceKind,
    },

    #[error(
        "middleware '{label}' returned {returned_capability:?}/{returned_surface:?} for a {requested_capability:?}/{requested_surface:?} request"
    )]
    ReturnedAttachmentMismatch {
        label: &'static str,
        requested_capability: MiddlewareCapability,
        requested_surface: MiddlewareSurfaceKind,
        returned_capability: MiddlewareCapability,
        returned_surface: MiddlewareSurfaceKind,
    },

    #[error(
        "middleware '{label}' declared authority '{declared}', but returned attachment authority '{returned}'"
    )]
    ReturnedAuthorityMismatch {
        label: &'static str,
        declared: &'static str,
        returned: &'static str,
    },

    #[error("surface {surface:?} is bound to stage {surface_stage}, but protected unit is bound to stage {unit_stage}")]
    StageMismatch {
        surface: MiddlewareSurfaceKind,
        surface_stage: StageId,
        unit_stage: StageId,
    },

    #[error("surface {surface:?} cannot protect unit {unit:?}")]
    ProtectedUnitMismatch {
        surface: MiddlewareSurfaceKind,
        unit: ProtectedUnit,
    },

    #[error(
        "declaration position {position:?} at ordinal {ordinal} cannot carry {capability:?} middleware on {surface:?}"
    )]
    DeclarationPositionMismatch {
        position: MiddlewareDeclarationPosition,
        ordinal: u64,
        capability: MiddlewareCapability,
        surface: MiddlewareSurfaceKind,
    },
}

/// Validate the pre-erasure declaration against one concrete binding request
/// and return its deterministic attachment id.
pub fn validate_attachment_request(
    declaration: &MiddlewareDeclaration,
    request: &MiddlewareAttachmentRequest<'_>,
) -> Result<MiddlewareAttachmentId, MiddlewareAttachmentValidationError> {
    declaration.validate_shape()?;
    let surface = request.surface.kind();
    if !declaration.supports(surface) {
        return Err(MiddlewareAttachmentValidationError::UnsupportedSurface {
            label: declaration.label,
            surface,
        });
    }

    let allowed = match declaration.capability {
        MiddlewareCapability::Control => surface.allows_control(),
        // Observer capability is universal across surfaces: a surface is
        // observer-capable iff it has an observer port (an `ObserverSurface`
        // impl). The set is derived from the observer wiring table, so the gate
        // cannot drift from the ports that actually exist.
        MiddlewareCapability::Observer => {
            crate::middleware::observer::OBSERVER_SURFACE_KINDS.contains(&surface)
        }
    };
    if !allowed {
        return Err(MiddlewareAttachmentValidationError::UnsupportedCapability {
            label: declaration.label,
            capability: declaration.capability,
            surface,
        });
    }

    // FLOWIP-115s: the grammar position is an authority-bearing coordinate,
    // not a descriptive tag inferred from capability or surface after the
    // fact. Validate the complete position/capability/surface matrix before an
    // identity can be minted. Bare singleton positions additionally reserve
    // ordinal zero until their owning follow-up proposal changes the grammar.
    let position = request.declaration_index.position();
    let ordinal = request.declaration_index.ordinal();
    let position_matches = match position {
        MiddlewareDeclarationPosition::SourceWith => {
            declaration.capability == MiddlewareCapability::Control
                && surface == MiddlewareSurfaceKind::SourcePoll
        }
        MiddlewareDeclarationPosition::IngressWith => {
            ordinal == 0
                && declaration.capability == MiddlewareCapability::Control
                && surface == MiddlewareSurfaceKind::Ingress
        }
        MiddlewareDeclarationPosition::EffectWith => {
            ordinal == 0
                && declaration.capability == MiddlewareCapability::Control
                && surface == MiddlewareSurfaceKind::Effect
        }
        MiddlewareDeclarationPosition::SinkWith => {
            declaration.capability == MiddlewareCapability::Control
                && surface == MiddlewareSurfaceKind::SinkDelivery
        }
        MiddlewareDeclarationPosition::Observers => {
            declaration.capability == MiddlewareCapability::Observer
                && crate::middleware::observer::OBSERVER_SURFACE_KINDS.contains(&surface)
        }
    };
    if !position_matches {
        return Err(
            MiddlewareAttachmentValidationError::DeclarationPositionMismatch {
                position,
                ordinal,
                capability: declaration.capability,
                surface,
            },
        );
    }

    let surface_stage = request
        .surface
        .stage_id()
        .expect("every materializable middleware surface carries a stage id");

    if surface_stage != request.protected_unit.stage_id {
        return Err(MiddlewareAttachmentValidationError::StageMismatch {
            surface,
            surface_stage,
            unit_stage: request.protected_unit.stage_id,
        });
    }

    let matches_unit = match (request.surface, &request.protected_unit.unit) {
        (MiddlewareSurface::SourcePoll(_), ProtectedUnit::SourcePoll(_)) => true,
        (MiddlewareSurface::Effect(surface), ProtectedUnit::Effect(unit)) => {
            surface.effect_type == unit.effect_type
        }
        (MiddlewareSurface::SinkDelivery(surface), ProtectedUnit::SinkDelivery(unit)) => {
            match (&surface.configured_target, &unit.target) {
                (None, SinkDeliveryTarget::Stage) => true,
                (Some(surface_target), SinkDeliveryTarget::Configured(unit_target)) => {
                    surface_target == unit_target
                }
                _ => false,
            }
        }
        (MiddlewareSurface::Ingress(surface), ProtectedUnit::Ingress(unit)) => {
            // FLOWIP-115d: the ingress surface and protected unit agree when the
            // replay-stable source stage key and the typed hosted target match.
            // Non-source hosted ingress has no source stage owner and is rejected
            // here because no non-source consumer ships in this slice.
            surface.owner.stage_key == unit.source_stage_key && surface.target == unit.target
        }
        (MiddlewareSurface::Handler { .. }, ProtectedUnit::Handler) => true,
        (MiddlewareSurface::Stateful { .. }, ProtectedUnit::Stateful) => true,
        (MiddlewareSurface::Join { .. }, ProtectedUnit::Join) => true,
        (MiddlewareSurface::OutputCommit { .. }, ProtectedUnit::OutputCommit) => true,
        (MiddlewareSurface::StageLifecycle { .. }, ProtectedUnit::StageLifecycle) => true,
        _ => false,
    };

    if !matches_unit {
        return Err(MiddlewareAttachmentValidationError::ProtectedUnitMismatch {
            surface,
            unit: request.protected_unit.unit.clone(),
        });
    }

    Ok(MiddlewareAttachmentId::from_declaration_and_request(
        declaration,
        request,
    ))
}

// ---------------------------------------------------------------------------
// Materialization request + context
// ---------------------------------------------------------------------------

/// The concrete attachment the DSL binder asks the factory to materialize: one
/// surface, one protected unit, and one declaration position.
#[derive(Clone, Copy)]
pub struct MiddlewareAttachmentRequest<'a> {
    pub surface: &'a MiddlewareSurface,
    pub protected_unit: &'a ProtectedUnitId,
    pub declaration_index: MiddlewareDeclarationIndex,
}

#[derive(Debug, Error)]
pub enum MiddlewareAuthorityError {
    #[error(
        "middleware '{middleware}' declared authority '{declared}' for {surface:?}, but materialisation attempted privileged authority '{attempted}'"
    )]
    Escalation {
        middleware: &'static str,
        surface: MiddlewareSurfaceKind,
        declared: &'static str,
        attempted: &'static str,
    },

    #[error(
        "middleware '{middleware}' attempted control registration for stage {attempted_stage} and effect {attempted_effect:?}, but the checked request is bound to stage {expected_stage} and effect {expected_effect:?}"
    )]
    RegistrationTargetMismatch {
        middleware: &'static str,
        expected_stage: StageId,
        expected_effect: Option<String>,
        attempted_stage: StageId,
        attempted_effect: Option<String>,
    },

    #[error(
        "middleware '{middleware}' attempted privileged materialisation for attachment {attempted_attachment}, but the checked invocation is bound to attachment {expected_attachment}"
    )]
    DelegatedRequestMismatch {
        middleware: &'static str,
        expected_attachment: obzenflow_core::Ulid,
        attempted_attachment: obzenflow_core::Ulid,
    },

    #[error(
        "middleware '{middleware}' returned an attachment from a different materialisation invocation"
    )]
    ReturnedInvocationMismatch { middleware: &'static str },
}

/// Invocation-local construction inputs for one checked materialisation.
/// Shared control-plane mutations are staged here and remain invisible until
/// the adapter-owned gateway validates the returned semantic claim and commits
/// the complete batch.
pub struct MiddlewareMaterializationContext<'a> {
    pub config: &'a StageConfig,
    /// The type of the stage being attached to. Source-poll materialization uses
    /// this to choose the FLOWIP-114m charge position (infinite sources charge
    /// pre-poll, finite sources charge after a clean non-empty delivery); the
    /// effect and sink-delivery surfaces ignore it.
    pub stage_type: StageType,
    middleware: &'static str,
    surface: MiddlewareSurfaceKind,
    bound_surface: MiddlewareSurface,
    expected_claim: MaterializationClaim,
    stage_id: StageId,
    effect_type: Option<EffectTypeKey>,
    attachment_id: MiddlewareAttachmentId,
    invocation_token: Arc<()>,
    pending: Mutex<Vec<PendingControlRegistration>>,
}

impl<'a> MiddlewareMaterializationContext<'a> {
    pub(crate) fn new(
        config: &'a StageConfig,
        stage_type: StageType,
        declaration: &MiddlewareDeclaration,
        request: &MiddlewareAttachmentRequest<'_>,
    ) -> Self {
        let effect_type = match &request.protected_unit.unit {
            ProtectedUnit::Effect(unit) => Some(unit.effect_type.clone()),
            _ => None,
        };
        Self {
            config,
            stage_type,
            middleware: declaration.label,
            surface: request.surface.kind(),
            bound_surface: request.surface.clone(),
            expected_claim: declaration.materialization_claim(),
            stage_id: request.protected_unit.stage_id,
            effect_type,
            attachment_id: MiddlewareAttachmentId::from_declaration_and_request(
                declaration,
                request,
            ),
            invocation_token: Arc::new(()),
            pending: Mutex::new(Vec::new()),
        }
    }

    fn ensure_claim(&self, claimant: MaterializationClaim) -> Result<(), MiddlewareAuthorityError> {
        if claimant == self.expected_claim {
            return Ok(());
        }
        Err(MiddlewareAuthorityError::Escalation {
            middleware: self.middleware,
            surface: self.surface,
            declared: self.expected_claim.label(),
            attempted: claimant.label(),
        })
    }

    pub(crate) fn authorize_materialization(
        &self,
        claimant: MaterializationClaim,
        declaration: &MiddlewareDeclaration,
        request: &MiddlewareAttachmentRequest<'_>,
    ) -> Result<(), MiddlewareAuthorityError> {
        self.ensure_claim(claimant)?;
        let attempted = MiddlewareAttachmentId::from_declaration_and_request(declaration, request);
        if request.surface != &self.bound_surface || attempted != self.attachment_id {
            return Err(MiddlewareAuthorityError::DelegatedRequestMismatch {
                middleware: self.middleware,
                expected_attachment: self.attachment_id.as_ulid(),
                attempted_attachment: attempted.as_ulid(),
            });
        }
        Ok(())
    }

    fn issue_attachment_token(
        &self,
        claimant: MaterializationClaim,
    ) -> Result<Arc<()>, MiddlewareAuthorityError> {
        self.ensure_claim(claimant)?;
        Ok(self.invocation_token.clone())
    }

    pub(crate) fn validate_returned_attachment(
        &self,
        attachment: &MiddlewareSurfaceAttachment,
    ) -> Result<(), MiddlewareAuthorityError> {
        let valid = match (&attachment.invocation_token, attachment.claim) {
            (None, MaterializationClaim::Ordinary) => true,
            (Some(token), claim) if claim != MaterializationClaim::Ordinary => {
                Arc::ptr_eq(token, &self.invocation_token)
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(MiddlewareAuthorityError::ReturnedInvocationMismatch {
                middleware: self.middleware,
            })
        }
    }

    pub(crate) fn stage_control_registration(
        &self,
        claimant: MaterializationClaim,
        registration: PendingControlRegistration,
    ) -> Result<(), MiddlewareAuthorityError> {
        self.ensure_claim(claimant)?;
        let (attempted_stage, attempted_effect) = registration.target();
        if attempted_stage != self.stage_id || attempted_effect != self.effect_type.as_ref() {
            return Err(MiddlewareAuthorityError::RegistrationTargetMismatch {
                middleware: self.middleware,
                expected_stage: self.stage_id,
                expected_effect: self
                    .effect_type
                    .as_ref()
                    .map(|effect| effect.as_str().to_string()),
                attempted_stage,
                attempted_effect: attempted_effect.map(|effect| effect.as_str().to_string()),
            });
        }
        self.pending
            .lock()
            .expect("middleware pending-registration lock poisoned")
            .push(registration);
        Ok(())
    }

    pub(crate) fn take_pending(&self) -> Vec<PendingControlRegistration> {
        std::mem::take(
            &mut *self
                .pending
                .lock()
                .expect("middleware pending-registration lock poisoned"),
        )
    }

    /// Resolve the policy-neutral immutable config view for exactly the
    /// protected unit being materialised. Configuration cannot create a unit;
    /// callers can only request a view for an already validated attachment.
    pub fn config_view(&self) -> obzenflow_runtime::runtime_config::ExactConfigView<'_> {
        use obzenflow_core::event::EffectType;
        use obzenflow_runtime::runtime_config::ResolutionPoint;

        let point = match &self.effect_type {
            Some(effect_type) => ResolutionPoint::Effect {
                stage: StageKey::from(self.config.name.as_str()),
                effect_type: EffectType::from(effect_type.as_str()),
            },
            None => ResolutionPoint::Stage(StageKey::from(self.config.name.as_str())),
        };
        self.config.effective_config.exact_view(point)
    }
}

// ---------------------------------------------------------------------------
// Surface attachment
// ---------------------------------------------------------------------------

/// One source-poll attachment: the composable source policy plus an optional
/// read-only completion-gate companion (the only companion this slice allows on
/// a source-poll attachment).
///
/// The attachment carries the adapter-owned `SourcePolicy`, not a pre-composed
/// boundary, because source composition happens at the policy level: the binder
/// collects every source policy declared for a stage (in resolved order) and
/// builds one `PerSourcePolicyBoundary` that implements the neutral runtime
/// `SourceBoundary` seam. A pre-composed per-middleware boundary could not
/// compose with other source policies (a rate limiter, or a second
/// control middleware) without breaking the single-context admit-forward /
/// observe-reverse semantics.
pub struct SourcePollAttachment {
    pub policy: Arc<dyn SourcePolicy>,
    pub completion_gate: Option<Arc<dyn CompletionGate>>,
}

/// The single typed attachment a factory materializes for one surface.
///
/// Adapter-owned: the DSL binder collects the per-surface policies, composes
/// them into the neutral runtime/core boundary, and passes only that neutral
/// seam to runtime/infra. Non-exhaustive; this slice implements `SourcePoll`,
/// `Effect`, and `SinkDelivery`, and FLOWIP-115d adds `Ingress`, whose neutral
/// port is the core-owned `IngressBoundaryMiddleware` that infra calls.
pub(crate) enum MiddlewareSurfaceAttachmentKind {
    SourcePoll(SourcePollAttachment),
    Effect(EffectPolicyAttachment),
    SinkDelivery(Arc<dyn SinkPolicy>),
    Ingress(Arc<dyn IngressBoundaryMiddleware>),
    SourcePollObserver(Arc<dyn SourcePollObserver>),
    EffectObserver(Arc<dyn EffectObserver>),
    SinkDeliveryObserver(Arc<dyn SinkDeliveryObserver>),
    HandlerObserver(Arc<dyn HandlerObserver>),
    StatefulObserver(Arc<dyn StatefulObserver>),
    JoinObserver(Arc<dyn JoinObserver>),
    OutputCommitObserver(Arc<dyn OutputCommitObserver>),
    StageLifecycleObserver(Arc<dyn StageLifecycleObserver>),
}

/// One typed attachment plus its sealed semantic claim. Public constructors
/// create ordinary custom attachments; built-in claims can only be attached by
/// adapter-owned constructors.
pub struct MiddlewareSurfaceAttachment {
    kind: MiddlewareSurfaceAttachmentKind,
    claim: MaterializationClaim,
    invocation_token: Option<Arc<()>>,
}

impl MiddlewareSurfaceAttachment {
    fn ordinary(kind: MiddlewareSurfaceAttachmentKind) -> Self {
        Self {
            kind,
            claim: MaterializationClaim::Ordinary,
            invocation_token: None,
        }
    }

    pub(crate) fn claimed(
        kind: MiddlewareSurfaceAttachmentKind,
        claim: MaterializationClaim,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> Result<Self, MiddlewareAuthorityError> {
        Ok(Self {
            kind,
            claim,
            invocation_token: Some(context.issue_attachment_token(claim)?),
        })
    }

    pub fn source_poll(attachment: SourcePollAttachment) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::SourcePoll(attachment))
    }

    pub fn effect(policy: Arc<dyn EffectPolicy>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::Effect(
            EffectPolicyAttachment::neutral(policy),
        ))
    }

    pub fn event_aware_effect(policy: Arc<dyn EventAwareEffectPolicy>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::Effect(
            EffectPolicyAttachment::event_aware(policy),
        ))
    }

    pub fn sink_delivery(policy: Arc<dyn SinkPolicy>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::SinkDelivery(policy))
    }

    pub fn ingress(boundary: Arc<dyn IngressBoundaryMiddleware>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::Ingress(boundary))
    }

    pub fn source_poll_observer(observer: Arc<dyn SourcePollObserver>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::SourcePollObserver(
            observer,
        ))
    }

    pub fn effect_observer(observer: Arc<dyn EffectObserver>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::EffectObserver(observer))
    }

    pub fn sink_delivery_observer(observer: Arc<dyn SinkDeliveryObserver>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::SinkDeliveryObserver(
            observer,
        ))
    }

    pub fn handler_observer(observer: Arc<dyn HandlerObserver>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::HandlerObserver(observer))
    }

    pub fn stateful_observer(observer: Arc<dyn StatefulObserver>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::StatefulObserver(observer))
    }

    pub fn join_observer(observer: Arc<dyn JoinObserver>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::JoinObserver(observer))
    }

    pub fn output_commit_observer(observer: Arc<dyn OutputCommitObserver>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::OutputCommitObserver(
            observer,
        ))
    }

    pub fn stage_lifecycle_observer(observer: Arc<dyn StageLifecycleObserver>) -> Self {
        Self::ordinary(MiddlewareSurfaceAttachmentKind::StageLifecycleObserver(
            observer,
        ))
    }

    pub(crate) fn into_kind(self) -> MiddlewareSurfaceAttachmentKind {
        self.kind
    }

    fn capability_and_surface(&self) -> (MiddlewareCapability, MiddlewareSurfaceKind) {
        capability_and_surface(&self.kind)
    }
}

/// A materialised attachment whose request, returned semantic claim, invocation
/// token, and staged authority have passed the adapter-owned gateway. Only this
/// checked carrier exposes payload extraction to the DSL.
pub struct CheckedMiddlewareSurfaceAttachment {
    kind: MiddlewareSurfaceAttachmentKind,
}

impl CheckedMiddlewareSurfaceAttachment {
    pub(crate) fn from_validated(attachment: MiddlewareSurfaceAttachment) -> Self {
        Self {
            kind: attachment.into_kind(),
        }
    }

    pub fn into_source_poll(self) -> Option<SourcePollAttachment> {
        match self.kind {
            MiddlewareSurfaceAttachmentKind::SourcePoll(attachment) => Some(attachment),
            _ => None,
        }
    }

    pub fn into_effect(self) -> Option<EffectPolicyAttachment> {
        match self.kind {
            MiddlewareSurfaceAttachmentKind::Effect(attachment) => Some(attachment),
            _ => None,
        }
    }

    pub fn into_sink_delivery(self) -> Option<Arc<dyn SinkPolicy>> {
        match self.kind {
            MiddlewareSurfaceAttachmentKind::SinkDelivery(policy) => Some(policy),
            _ => None,
        }
    }

    pub fn into_ingress(self) -> Option<Arc<dyn IngressBoundaryMiddleware>> {
        match self.kind {
            MiddlewareSurfaceAttachmentKind::Ingress(boundary) => Some(boundary),
            _ => None,
        }
    }

    pub(crate) fn into_kind(self) -> MiddlewareSurfaceAttachmentKind {
        self.kind
    }
}

fn capability_and_surface(
    kind: &MiddlewareSurfaceAttachmentKind,
) -> (MiddlewareCapability, MiddlewareSurfaceKind) {
    match kind {
        MiddlewareSurfaceAttachmentKind::SourcePoll(_) => (
            MiddlewareCapability::Control,
            MiddlewareSurfaceKind::SourcePoll,
        ),
        MiddlewareSurfaceAttachmentKind::Effect(_) => {
            (MiddlewareCapability::Control, MiddlewareSurfaceKind::Effect)
        }
        MiddlewareSurfaceAttachmentKind::SinkDelivery(_) => (
            MiddlewareCapability::Control,
            MiddlewareSurfaceKind::SinkDelivery,
        ),
        MiddlewareSurfaceAttachmentKind::Ingress(_) => (
            MiddlewareCapability::Control,
            MiddlewareSurfaceKind::Ingress,
        ),
        MiddlewareSurfaceAttachmentKind::SourcePollObserver(_) => (
            MiddlewareCapability::Observer,
            MiddlewareSurfaceKind::SourcePoll,
        ),
        MiddlewareSurfaceAttachmentKind::EffectObserver(_) => (
            MiddlewareCapability::Observer,
            MiddlewareSurfaceKind::Effect,
        ),
        MiddlewareSurfaceAttachmentKind::SinkDeliveryObserver(_) => (
            MiddlewareCapability::Observer,
            MiddlewareSurfaceKind::SinkDelivery,
        ),
        MiddlewareSurfaceAttachmentKind::HandlerObserver(_) => (
            MiddlewareCapability::Observer,
            MiddlewareSurfaceKind::Handler,
        ),
        MiddlewareSurfaceAttachmentKind::StatefulObserver(_) => (
            MiddlewareCapability::Observer,
            MiddlewareSurfaceKind::Stateful,
        ),
        MiddlewareSurfaceAttachmentKind::JoinObserver(_) => {
            (MiddlewareCapability::Observer, MiddlewareSurfaceKind::Join)
        }
        MiddlewareSurfaceAttachmentKind::OutputCommitObserver(_) => (
            MiddlewareCapability::Observer,
            MiddlewareSurfaceKind::OutputCommit,
        ),
        MiddlewareSurfaceAttachmentKind::StageLifecycleObserver(_) => (
            MiddlewareCapability::Observer,
            MiddlewareSurfaceKind::StageLifecycle,
        ),
    }
}

/// Validate the value returned by a factory against the already-validated
/// request. Declaration validation alone cannot stop a nominal observer from
/// returning a control attachment or the wrong observer surface.
pub(crate) fn validate_materialized_attachment(
    declaration: &MiddlewareDeclaration,
    request: &MiddlewareAttachmentRequest<'_>,
    attachment: &MiddlewareSurfaceAttachment,
) -> Result<(), MiddlewareAttachmentValidationError> {
    validate_attachment_request(declaration, request)?;
    let requested_surface = request.surface.kind();
    let (returned_capability, returned_surface) = attachment.capability_and_surface();
    if declaration.capability != returned_capability || requested_surface != returned_surface {
        return Err(
            MiddlewareAttachmentValidationError::ReturnedAttachmentMismatch {
                label: declaration.label,
                requested_capability: declaration.capability,
                requested_surface,
                returned_capability,
                returned_surface,
            },
        );
    }
    if declaration.materialization_claim != attachment.claim {
        return Err(
            MiddlewareAttachmentValidationError::ReturnedAuthorityMismatch {
                label: declaration.label,
                declared: declaration.materialization_claim.label(),
                returned: attachment.claim.label(),
            },
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_declarations_reject_an_empty_surface_set() {
        let declaration = MiddlewareDeclaration::observer("empty", Vec::new());
        assert!(matches!(
            declaration.validate_shape(),
            Err(MiddlewareAttachmentValidationError::EmptyDeclaration { label: "empty" })
        ));
    }

    /// A source-owned ingress surface plus its matching protected unit
    /// (FLOWIP-115d), for the canonical piggy-bank `accounts` shape.
    fn ingress_fixture(stage_id: StageId) -> (MiddlewareSurface, ProtectedUnitId) {
        let stage_key = StageKey("accounts".to_string());
        let target = HostedIngressTargetKey {
            surface: IngressKey("/api/bank/accounts".to_string()),
            scope: IngressRouteScope::Admission,
        };
        let surface = MiddlewareSurface::Ingress(IngressSurface {
            owner: SourceStageIngressOwner {
                stage_id,
                stage_key: stage_key.clone(),
            },
            target: target.clone(),
        });
        let unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::Ingress(IngressUnitId {
                source_stage_key: stage_key,
                target,
            }),
        };
        (surface, unit)
    }

    #[test]
    fn validates_source_owned_ingress_and_rejects_target_mismatch() {
        let stage_id = StageId::new();
        let declaration = MiddlewareDeclaration::control_with_family(
            "rate_limiter",
            "rate_limiter",
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Ingress,
            ],
        );
        let (surface, unit) = ingress_fixture(stage_id);
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &unit,
            declaration_index: MiddlewareDeclarationIndex::ingress_with(),
        };

        // A source-owned ingress attachment validates and derives a stable id.
        let id = validate_attachment_request(&declaration, &request).unwrap();
        assert_eq!(
            id,
            validate_attachment_request(&declaration, &request).unwrap()
        );

        // A surface naming a different hosted target than the unit fails as a
        // protected-unit mismatch (the source stage still agrees).
        let other_target = MiddlewareSurface::Ingress(IngressSurface {
            owner: SourceStageIngressOwner {
                stage_id,
                stage_key: StageKey("accounts".to_string()),
            },
            target: HostedIngressTargetKey {
                surface: IngressKey("/api/bank/tx".to_string()),
                scope: IngressRouteScope::Admission,
            },
        });
        let mismatch_request = MiddlewareAttachmentRequest {
            surface: &other_target,
            ..request
        };
        assert!(matches!(
            validate_attachment_request(&declaration, &mismatch_request),
            Err(MiddlewareAttachmentValidationError::ProtectedUnitMismatch { .. })
        ));
    }

    #[test]
    fn rejects_ingress_without_declared_support_or_control_capability() {
        let stage_id = StageId::new();
        let (surface, unit) = ingress_fixture(stage_id);
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &unit,
            declaration_index: MiddlewareDeclarationIndex::ingress_with(),
        };

        // A control declaration that does not list Ingress is UnsupportedSurface.
        let no_ingress = MiddlewareDeclaration::control_with_family(
            "rate_limiter",
            "rate_limiter",
            vec![MiddlewareSurfaceKind::SourcePoll],
        );
        assert!(matches!(
            validate_attachment_request(&no_ingress, &request),
            Err(MiddlewareAttachmentValidationError::UnsupportedSurface { .. })
        ));

        // Ingress observation is carrier-reserved but not a public 115f
        // observer port until infra ships the matching dispatcher.
        let observer =
            MiddlewareDeclaration::observer("observer", vec![MiddlewareSurfaceKind::Ingress]);
        assert!(matches!(
            validate_attachment_request(&observer, &request),
            Err(MiddlewareAttachmentValidationError::UnsupportedCapability { .. })
        ));
    }

    #[test]
    fn output_commit_allows_observer_but_rejects_control() {
        let stage_id = StageId::new();
        let surface = MiddlewareSurface::OutputCommit { stage_id };
        let protected_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::OutputCommit,
        };
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            declaration_index: MiddlewareDeclarationIndex::observers(0),
        };

        let observer =
            MiddlewareDeclaration::observer("timing", vec![MiddlewareSurfaceKind::OutputCommit]);
        validate_attachment_request(&observer, &request)
            .expect("output commit observer should validate");

        let control = MiddlewareDeclaration::control(
            "bad_control",
            vec![MiddlewareSurfaceKind::OutputCommit],
        );
        assert!(matches!(
            validate_attachment_request(&control, &request),
            Err(MiddlewareAttachmentValidationError::UnsupportedCapability { .. })
        ));
    }

    #[test]
    fn validates_source_poll_and_derives_stable_attachment_id() {
        let stage_id = StageId::new_const(1);
        let declaration = MiddlewareDeclaration::control_with_family(
            "shared_label",
            "circuit_breaker",
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Effect,
            ],
        );
        let surface = MiddlewareSurface::SourcePoll(SourcePollSurface { stage_id });
        let protected_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
        };
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            declaration_index: MiddlewareDeclarationIndex::source_with(0),
        };

        let first = validate_attachment_request(&declaration, &request).unwrap();
        let second = validate_attachment_request(&declaration, &request).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.as_ulid(), second.as_ulid());
        assert_eq!(
            first.as_ulid().to_string(),
            "4PSQFY5Q4PTGAJB1RCWBE4XY6A",
            "the middleware-attachment:v5 coordinate is a versioned archive boundary"
        );

        let other_index_request = MiddlewareAttachmentRequest {
            declaration_index: MiddlewareDeclarationIndex::source_with(1),
            ..request
        };
        assert_ne!(
            first,
            validate_attachment_request(&declaration, &other_index_request).unwrap()
        );

        let other_family = MiddlewareDeclaration::control_with_family(
            "shared_label",
            "rate_limiter",
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Effect,
            ],
        );
        assert_ne!(
            first,
            validate_attachment_request(&other_family, &request).unwrap()
        );

        let effect_surface = MiddlewareSurface::Effect(EffectSurface {
            stage_id,
            effect_type: EffectTypeKey::from("http"),
            safety: EffectSafety::Idempotent,
        });
        let effect_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::Effect(EffectUnitId {
                effect_type: EffectTypeKey::from("http"),
            }),
        };
        let effect_request = MiddlewareAttachmentRequest {
            surface: &effect_surface,
            protected_unit: &effect_unit,
            declaration_index: MiddlewareDeclarationIndex::effect_with(),
        };
        assert_ne!(
            first,
            validate_attachment_request(&declaration, &effect_request).unwrap()
        );

        let other_stage_id = StageId::new_const(2);
        let other_stage_surface = MiddlewareSurface::SourcePoll(SourcePollSurface {
            stage_id: other_stage_id,
        });
        let other_stage_unit = ProtectedUnitId {
            stage_id: other_stage_id,
            unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
        };
        let other_stage_request = MiddlewareAttachmentRequest {
            surface: &other_stage_surface,
            protected_unit: &other_stage_unit,
            declaration_index: MiddlewareDeclarationIndex::source_with(0),
        };
        assert_ne!(
            first,
            validate_attachment_request(&declaration, &other_stage_request).unwrap()
        );

        assert_eq!(
            MiddlewareDeclarationPosition::SourceWith.stable_label(),
            "source_with"
        );
    }

    fn surface_fixture(
        kind: MiddlewareSurfaceKind,
        stage_id: StageId,
    ) -> (MiddlewareSurface, ProtectedUnitId) {
        match kind {
            MiddlewareSurfaceKind::SourcePoll => (
                MiddlewareSurface::SourcePoll(SourcePollSurface { stage_id }),
                ProtectedUnitId {
                    stage_id,
                    unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
                },
            ),
            MiddlewareSurfaceKind::Effect => (
                MiddlewareSurface::Effect(EffectSurface {
                    stage_id,
                    effect_type: EffectTypeKey::from("matrix.effect"),
                    safety: EffectSafety::Idempotent,
                }),
                ProtectedUnitId {
                    stage_id,
                    unit: ProtectedUnit::Effect(EffectUnitId {
                        effect_type: EffectTypeKey::from("matrix.effect"),
                    }),
                },
            ),
            MiddlewareSurfaceKind::SinkDelivery => (
                MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
                    stage_id,
                    configured_target: None,
                }),
                ProtectedUnitId {
                    stage_id,
                    unit: ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
                        target: SinkDeliveryTarget::Stage,
                    }),
                },
            ),
            MiddlewareSurfaceKind::Ingress => ingress_fixture(stage_id),
            MiddlewareSurfaceKind::Handler => (
                MiddlewareSurface::Handler { stage_id },
                ProtectedUnitId {
                    stage_id,
                    unit: ProtectedUnit::Handler,
                },
            ),
            MiddlewareSurfaceKind::Stateful => (
                MiddlewareSurface::Stateful { stage_id },
                ProtectedUnitId {
                    stage_id,
                    unit: ProtectedUnit::Stateful,
                },
            ),
            MiddlewareSurfaceKind::Join => (
                MiddlewareSurface::Join { stage_id },
                ProtectedUnitId {
                    stage_id,
                    unit: ProtectedUnit::Join,
                },
            ),
            MiddlewareSurfaceKind::OutputCommit => (
                MiddlewareSurface::OutputCommit { stage_id },
                ProtectedUnitId {
                    stage_id,
                    unit: ProtectedUnit::OutputCommit,
                },
            ),
            MiddlewareSurfaceKind::StageLifecycle => (
                MiddlewareSurface::StageLifecycle { stage_id },
                ProtectedUnitId {
                    stage_id,
                    unit: ProtectedUnit::StageLifecycle,
                },
            ),
        }
    }

    #[test]
    fn five_position_labels_are_exact_and_pairwise_distinct_in_the_v5_preimage() {
        let positions = [
            MiddlewareDeclarationPosition::SourceWith,
            MiddlewareDeclarationPosition::IngressWith,
            MiddlewareDeclarationPosition::EffectWith,
            MiddlewareDeclarationPosition::SinkWith,
            MiddlewareDeclarationPosition::Observers,
        ];
        assert_eq!(
            positions.map(MiddlewareDeclarationPosition::stable_label),
            [
                "source_with",
                "ingress_with",
                "effect_with",
                "sink_with",
                "observers",
            ]
        );

        let stage_id = StageId::new_const(11);
        let (surface, protected_unit) =
            surface_fixture(MiddlewareSurfaceKind::SourcePoll, stage_id);
        let declaration = MiddlewareDeclaration::control(
            "position-separation",
            vec![MiddlewareSurfaceKind::SourcePoll],
        );
        let ids = positions
            .into_iter()
            .map(|position| {
                let request = MiddlewareAttachmentRequest {
                    surface: &surface,
                    protected_unit: &protected_unit,
                    declaration_index: MiddlewareDeclarationIndex::for_test(position, 0),
                };
                MiddlewareAttachmentId::from_declaration_and_request(&declaration, &request)
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            ids.len(),
            5,
            "every stable position label separates identity"
        );
    }

    #[test]
    fn position_capability_surface_matrix_fails_closed() {
        let positions = [
            MiddlewareDeclarationPosition::SourceWith,
            MiddlewareDeclarationPosition::IngressWith,
            MiddlewareDeclarationPosition::EffectWith,
            MiddlewareDeclarationPosition::SinkWith,
            MiddlewareDeclarationPosition::Observers,
        ];
        let surfaces = [
            MiddlewareSurfaceKind::SourcePoll,
            MiddlewareSurfaceKind::Effect,
            MiddlewareSurfaceKind::SinkDelivery,
            MiddlewareSurfaceKind::Ingress,
            MiddlewareSurfaceKind::Handler,
            MiddlewareSurfaceKind::Stateful,
            MiddlewareSurfaceKind::Join,
            MiddlewareSurfaceKind::OutputCommit,
            MiddlewareSurfaceKind::StageLifecycle,
        ];

        for position in positions {
            for capability in [
                MiddlewareCapability::Control,
                MiddlewareCapability::Observer,
            ] {
                for surface_kind in surfaces {
                    let stage_id = StageId::new_const(12);
                    let (surface, protected_unit) = surface_fixture(surface_kind, stage_id);
                    let declaration = match capability {
                        MiddlewareCapability::Control => {
                            MiddlewareDeclaration::control("matrix", vec![surface_kind])
                        }
                        MiddlewareCapability::Observer => {
                            MiddlewareDeclaration::observer("matrix", vec![surface_kind])
                        }
                    };
                    let request = MiddlewareAttachmentRequest {
                        surface: &surface,
                        protected_unit: &protected_unit,
                        declaration_index: MiddlewareDeclarationIndex::for_test(position, 0),
                    };
                    let accepted = validate_attachment_request(&declaration, &request).is_ok();
                    let expected = match position {
                        MiddlewareDeclarationPosition::SourceWith => {
                            capability == MiddlewareCapability::Control
                                && surface_kind == MiddlewareSurfaceKind::SourcePoll
                        }
                        MiddlewareDeclarationPosition::IngressWith => {
                            capability == MiddlewareCapability::Control
                                && surface_kind == MiddlewareSurfaceKind::Ingress
                        }
                        MiddlewareDeclarationPosition::EffectWith => {
                            capability == MiddlewareCapability::Control
                                && surface_kind == MiddlewareSurfaceKind::Effect
                        }
                        MiddlewareDeclarationPosition::SinkWith => {
                            capability == MiddlewareCapability::Control
                                && surface_kind == MiddlewareSurfaceKind::SinkDelivery
                        }
                        MiddlewareDeclarationPosition::Observers => {
                            capability == MiddlewareCapability::Observer
                                && crate::middleware::observer::OBSERVER_SURFACE_KINDS
                                    .contains(&surface_kind)
                        }
                    };
                    assert_eq!(
                        accepted, expected,
                        "matrix mismatch for {position:?}/{capability:?}/{surface_kind:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn singleton_positions_reject_non_zero_ordinals_before_identity_is_minted() {
        for (position, surface_kind) in [
            (
                MiddlewareDeclarationPosition::IngressWith,
                MiddlewareSurfaceKind::Ingress,
            ),
            (
                MiddlewareDeclarationPosition::EffectWith,
                MiddlewareSurfaceKind::Effect,
            ),
        ] {
            let stage_id = StageId::new_const(13);
            let (surface, protected_unit) = surface_fixture(surface_kind, stage_id);
            let declaration = MiddlewareDeclaration::control("singleton", vec![surface_kind]);
            let request = MiddlewareAttachmentRequest {
                surface: &surface,
                protected_unit: &protected_unit,
                declaration_index: MiddlewareDeclarationIndex::for_test(position, 1),
            };
            assert!(matches!(
                validate_attachment_request(&declaration, &request),
                Err(
                    MiddlewareAttachmentValidationError::DeclarationPositionMismatch {
                        ordinal: 1,
                        ..
                    }
                )
            ));
        }
    }

    #[test]
    fn observer_fan_out_reuses_one_lane_coordinate_while_surfaces_separate_ids() {
        let stage_id = StageId::new_const(14);
        let declaration = MiddlewareDeclaration::observer(
            "fan-out",
            vec![
                MiddlewareSurfaceKind::Handler,
                MiddlewareSurfaceKind::StageLifecycle,
            ],
        );
        let declaration_index = MiddlewareDeclarationIndex::observers(3);
        let ids = [
            MiddlewareSurfaceKind::Handler,
            MiddlewareSurfaceKind::StageLifecycle,
        ]
        .map(|surface_kind| {
            let (surface, protected_unit) = surface_fixture(surface_kind, stage_id);
            let request = MiddlewareAttachmentRequest {
                surface: &surface,
                protected_unit: &protected_unit,
                declaration_index,
            };
            validate_attachment_request(&declaration, &request).unwrap()
        });

        assert_eq!(
            declaration_index.position(),
            MiddlewareDeclarationPosition::Observers
        );
        assert_eq!(declaration_index.ordinal(), 3);
        assert_ne!(
            ids[0], ids[1],
            "surface remains a separate identity coordinate"
        );
    }

    #[test]
    fn retained_attachment_discriminants_separate_effect_sink_and_ingress_units() {
        let stage_id = StageId::new_const(3);

        let effect_declaration = MiddlewareDeclaration::control_with_family(
            "effect_control",
            "effect_control",
            vec![MiddlewareSurfaceKind::Effect],
        );
        let http_surface = MiddlewareSurface::Effect(EffectSurface {
            stage_id,
            effect_type: EffectTypeKey::from("http"),
            safety: EffectSafety::Idempotent,
        });
        let http_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::Effect(EffectUnitId {
                effect_type: EffectTypeKey::from("http"),
            }),
        };
        let sql_surface = MiddlewareSurface::Effect(EffectSurface {
            stage_id,
            effect_type: EffectTypeKey::from("sql"),
            safety: EffectSafety::Idempotent,
        });
        let sql_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::Effect(EffectUnitId {
                effect_type: EffectTypeKey::from("sql"),
            }),
        };
        let http_request = MiddlewareAttachmentRequest {
            surface: &http_surface,
            protected_unit: &http_unit,
            declaration_index: MiddlewareDeclarationIndex::effect_with(),
        };
        let sql_request = MiddlewareAttachmentRequest {
            surface: &sql_surface,
            protected_unit: &sql_unit,
            declaration_index: MiddlewareDeclarationIndex::effect_with(),
        };
        assert_ne!(
            validate_attachment_request(&effect_declaration, &http_request).unwrap(),
            validate_attachment_request(&effect_declaration, &sql_request).unwrap()
        );

        let sink_declaration = MiddlewareDeclaration::control_with_family(
            "sink_control",
            "sink_control",
            vec![MiddlewareSurfaceKind::SinkDelivery],
        );
        let stage_sink_surface = MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
            stage_id,
            configured_target: None,
        });
        let stage_sink_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
                target: SinkDeliveryTarget::Stage,
            }),
        };
        let configured_target = SinkConfiguredTargetKey("primary".to_string());
        let configured_sink_surface = MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
            stage_id,
            configured_target: Some(configured_target.clone()),
        });
        let configured_sink_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
                target: SinkDeliveryTarget::Configured(configured_target),
            }),
        };
        let stage_sink_request = MiddlewareAttachmentRequest {
            surface: &stage_sink_surface,
            protected_unit: &stage_sink_unit,
            declaration_index: MiddlewareDeclarationIndex::sink_with(0),
        };
        let configured_sink_request = MiddlewareAttachmentRequest {
            surface: &configured_sink_surface,
            protected_unit: &configured_sink_unit,
            declaration_index: MiddlewareDeclarationIndex::sink_with(0),
        };
        assert_ne!(
            validate_attachment_request(&sink_declaration, &stage_sink_request).unwrap(),
            validate_attachment_request(&sink_declaration, &configured_sink_request).unwrap()
        );

        let ingress_declaration = MiddlewareDeclaration::control_with_family(
            "ingress_control",
            "ingress_control",
            vec![MiddlewareSurfaceKind::Ingress],
        );
        let stage_key = StageKey("ingress".to_string());
        let hosted_surface = IngressKey("/events".to_string());
        let admission_target = HostedIngressTargetKey {
            surface: hosted_surface.clone(),
            scope: IngressRouteScope::Admission,
        };
        let endpoint_target = HostedIngressTargetKey {
            surface: hosted_surface,
            scope: IngressRouteScope::Endpoint(IngressEndpointKind::Events),
        };
        let admission_surface = MiddlewareSurface::Ingress(IngressSurface {
            owner: SourceStageIngressOwner {
                stage_id,
                stage_key: stage_key.clone(),
            },
            target: admission_target.clone(),
        });
        let admission_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::Ingress(IngressUnitId {
                source_stage_key: stage_key.clone(),
                target: admission_target,
            }),
        };
        let endpoint_surface = MiddlewareSurface::Ingress(IngressSurface {
            owner: SourceStageIngressOwner {
                stage_id,
                stage_key: stage_key.clone(),
            },
            target: endpoint_target.clone(),
        });
        let endpoint_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::Ingress(IngressUnitId {
                source_stage_key: stage_key,
                target: endpoint_target,
            }),
        };
        let admission_request = MiddlewareAttachmentRequest {
            surface: &admission_surface,
            protected_unit: &admission_unit,
            declaration_index: MiddlewareDeclarationIndex::ingress_with(),
        };
        let endpoint_request = MiddlewareAttachmentRequest {
            surface: &endpoint_surface,
            protected_unit: &endpoint_unit,
            declaration_index: MiddlewareDeclarationIndex::ingress_with(),
        };
        assert_ne!(
            validate_attachment_request(&ingress_declaration, &admission_request).unwrap(),
            validate_attachment_request(&ingress_declaration, &endpoint_request).unwrap()
        );
    }

    #[test]
    fn rejects_surface_protected_unit_mismatch() {
        let stage_id = StageId::new();
        let declaration = MiddlewareDeclaration::control(
            "circuit_breaker",
            vec![MiddlewareSurfaceKind::SourcePoll],
        );
        let surface = MiddlewareSurface::SourcePoll(SourcePollSurface { stage_id });
        let protected_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::Effect(EffectUnitId {
                effect_type: EffectTypeKey::from("http"),
            }),
        };
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            declaration_index: MiddlewareDeclarationIndex::source_with(0),
        };

        let err = validate_attachment_request(&declaration, &request).unwrap_err();
        assert!(matches!(
            err,
            MiddlewareAttachmentValidationError::ProtectedUnitMismatch { .. }
        ));
    }

    #[test]
    fn resilience_aggregate_rejects_standalone_effect_limiter_in_both_orders() {
        use crate::middleware::{CircuitBreaker, EffectResilience, RateLimiterBuilder};

        let breaker_only_aggregate = EffectResilience::with_breaker(
            CircuitBreaker::builder()
                .consecutive_failures(2)
                .build()
                .expect("breaker-only aggregate configuration"),
        )
        .build()
        .expect("breaker-only aggregate factory");
        let standalone_limiter = RateLimiterBuilder::new(10.0).build();
        let aggregate = breaker_only_aggregate.declaration();
        let limiter = standalone_limiter.declaration();

        for declarations in [
            vec![aggregate.clone(), limiter.clone()],
            vec![limiter.clone(), aggregate.clone()],
        ] {
            let error = validate_effect_control_composition(
                "payments",
                "payments.authorize",
                &declarations,
            )
            .unwrap_err();
            assert!(matches!(
                error,
                EffectControlCompositionError::AggregateWithStandalone { conflicts, .. }
                    if conflicts == vec!["rate_limiter"]
            ));
        }
    }

    #[test]
    fn observers_and_ordinary_effect_controls_can_coexist_with_resilience() {
        let declarations = vec![
            MiddlewareDeclaration::effect_resilience("effect_resilience", "effect_resilience"),
            MiddlewareDeclaration::observer("effect_observer", vec![MiddlewareSurfaceKind::Effect]),
            MiddlewareDeclaration::control("custom_control", vec![MiddlewareSurfaceKind::Effect]),
        ];
        validate_effect_control_composition("payments", "payments.authorize", &declarations)
            .expect("only standalone built-in effect controls conflict with the aggregate");
    }
}
