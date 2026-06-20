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
//! This slice implements the `SourcePoll` and `Effect` surfaces consumed by the
//! circuit breaker. `SinkDelivery` is added by the sink-boundary phase once the
//! runtime `SinkDeliveryBoundary` seam exists; `Ingress` (FLOWIP-115d) and the
//! observer surfaces (FLOWIP-115f) are reserved in the non-exhaustive
//! vocabulary so later slices fill them additively.

use super::control::ControlMiddlewareAggregator;
use super::policy::{EffectPolicyAttachment, SinkPolicy, SourcePolicy};
use obzenflow_core::event::context::StageType;
use obzenflow_core::ingress::{IngressBoundaryMiddleware, IngressKey};
use obzenflow_core::{StageId, StageKey};
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::source::strategies::CompletionGate;
use ring::digest::{Context, SHA256};
use std::sync::Arc;
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
    Structural,
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
    /// Reserved observer surfaces for FLOWIP-115f.
    Handler,
    Stateful,
    Join,
    OutputCommit,
    StageLifecycle,
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
            Self::Handler => MiddlewareSurfaceKind::Handler,
            Self::Stateful => MiddlewareSurfaceKind::Stateful,
            Self::Join => MiddlewareSurfaceKind::Join,
            Self::OutputCommit => MiddlewareSurfaceKind::OutputCommit,
            Self::StageLifecycle => MiddlewareSurfaceKind::StageLifecycle,
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
            _ => None,
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

// ---------------------------------------------------------------------------
// Origin
// ---------------------------------------------------------------------------

/// Where the middleware declaration came from: flow-level configuration,
/// stage-level configuration, or a stage override.
///
/// Adapter-owned audit/validation metadata. The DSL binder maps its own
/// resolution provenance (`obzenflow_dsl::MiddlewareSource`) into this before
/// calling adapter APIs, so adapter APIs do not depend on DSL resolution types.
/// It is not the protected-unit key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MiddlewareOrigin {
    Flow,
    Stage,
    StageOverride {
        family_label: String,
        flow_label: String,
        stage_label: String,
    },
}

/// Which ordered declaration lane produced this attachment.
///
/// Adapter-owned so the binder can pass ordering metadata without exposing DSL
/// resolution types. The lane keeps explicit effect-policy declarations distinct
/// from the resolved stage/flow middleware list when both attach the same
/// family to the same protected unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MiddlewareDeclarationScope {
    ResolvedMiddleware,
    EffectPolicy,
}

/// Replay-stable declaration position within an adapter-owned declaration lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MiddlewareDeclarationIndex {
    pub scope: MiddlewareDeclarationScope,
    pub index: u64,
}

impl MiddlewareDeclarationIndex {
    pub fn resolved(index: usize) -> Self {
        Self {
            scope: MiddlewareDeclarationScope::ResolvedMiddleware,
            index: index as u64,
        }
    }

    pub fn effect_policy(index: usize) -> Self {
        Self {
            scope: MiddlewareDeclarationScope::EffectPolicy,
            index: index as u64,
        }
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
/// origin, declaration order, concrete surface, and protected unit.
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
        push_field(&mut context, "schema", "middleware-attachment:v3");
        push_field(&mut context, "middleware.label", declaration.label);
        push_field(&mut context, "middleware.family", declaration.family_label);
        push_origin(&mut context, request.origin);
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

fn push_origin(context: &mut Context, origin: &MiddlewareOrigin) {
    match origin {
        MiddlewareOrigin::Flow => push_field(context, "origin.kind", "flow"),
        MiddlewareOrigin::Stage => push_field(context, "origin.kind", "stage"),
        MiddlewareOrigin::StageOverride {
            family_label,
            flow_label,
            stage_label,
        } => {
            push_field(context, "origin.kind", "stage_override");
            push_field(context, "origin.family", family_label);
            push_field(context, "origin.flow_label", flow_label);
            push_field(context, "origin.stage_label", stage_label);
        }
    }
}

fn push_declaration_index(context: &mut Context, index: MiddlewareDeclarationIndex) {
    let scope = match index.scope {
        MiddlewareDeclarationScope::ResolvedMiddleware => "resolved_middleware",
        MiddlewareDeclarationScope::EffectPolicy => "effect_policy",
    };
    push_field(context, "declaration.scope", scope);
    push_field(context, "declaration.index", &index.index.to_string());
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
        MiddlewareSurface::Handler => push_field(context, "surface.kind", "handler"),
        MiddlewareSurface::Stateful => push_field(context, "surface.kind", "stateful"),
        MiddlewareSurface::Join => push_field(context, "surface.kind", "join"),
        MiddlewareSurface::OutputCommit => push_field(context, "surface.kind", "output_commit"),
        MiddlewareSurface::StageLifecycle => {
            push_field(context, "surface.kind", "stage_lifecycle");
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
    }
}

// ---------------------------------------------------------------------------
// Declaration (pre-erasure)
// ---------------------------------------------------------------------------

/// The static declaration a factory returns before any runtime middleware
/// object is created. The binder reads this to validate `surface x capability`
/// and to plan the attachment without constructing a `Box<dyn Middleware>`.
#[derive(Debug, Clone)]
pub struct MiddlewareDeclaration {
    pub label: &'static str,
    pub family_label: &'static str,
    pub capability: MiddlewareCapability,
    /// The surfaces this factory can attach to. A control middleware may span
    /// several (the circuit breaker declares source poll, effect, and sink
    /// delivery); the binder picks the concrete surface per call site and
    /// validates membership. An empty set marks legacy shell middleware with no
    /// hook surface, created via `create()`.
    pub surfaces: Vec<MiddlewareSurfaceKind>,
}

impl MiddlewareDeclaration {
    /// Declaration for legacy shell middleware: no hook surface, observer
    /// capability (the safe default matching `MiddlewareKind::Observation`).
    pub fn legacy_shell(label: &'static str, family_label: &'static str) -> Self {
        Self {
            label,
            family_label,
            capability: MiddlewareCapability::Observer,
            surfaces: Vec::new(),
        }
    }

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
        }
    }

    pub fn is_legacy_shell(&self) -> bool {
        self.surfaces.is_empty()
    }

    /// Whether this factory declares it can attach to `surface`.
    pub fn supports(&self, surface: MiddlewareSurfaceKind) -> bool {
        self.surfaces.contains(&surface)
    }

    /// Whether this is a control-capability declaration.
    pub fn is_control(&self) -> bool {
        matches!(self.capability, MiddlewareCapability::Control)
    }
}

// ---------------------------------------------------------------------------
// Attachment validation
// ---------------------------------------------------------------------------

/// A carrier-level validation failure before runtime erasure.
#[derive(Debug, Error)]
pub enum MiddlewareAttachmentValidationError {
    #[error(
        "middleware '{label}' is legacy shell middleware with no hook surface; \
         materialization requires a declared hook surface"
    )]
    LegacyShell { label: &'static str },

    #[error("middleware '{label}' does not declare support for surface {surface:?}")]
    UnsupportedSurface {
        label: &'static str,
        surface: MiddlewareSurfaceKind,
    },

    #[error("middleware '{label}' declares capability {capability:?}, but only control-capability materialization is implemented for surface {surface:?}")]
    UnsupportedCapability {
        label: &'static str,
        capability: MiddlewareCapability,
        surface: MiddlewareSurfaceKind,
    },

    #[error("surface {surface:?} is bound to stage {surface_stage}, but protected unit is bound to stage {unit_stage}")]
    StageMismatch {
        surface: MiddlewareSurfaceKind,
        surface_stage: StageId,
        unit_stage: StageId,
    },

    #[error("surface {surface:?} is reserved and cannot be materialized by this slice")]
    ReservedSurface { surface: MiddlewareSurfaceKind },

    #[error("surface {surface:?} cannot protect unit {unit:?}")]
    ProtectedUnitMismatch {
        surface: MiddlewareSurfaceKind,
        unit: ProtectedUnit,
    },
}

/// Validate the pre-erasure declaration against one concrete binding request
/// and return its deterministic attachment id.
pub fn validate_attachment_request(
    declaration: &MiddlewareDeclaration,
    request: &MiddlewareAttachmentRequest<'_>,
) -> Result<MiddlewareAttachmentId, MiddlewareAttachmentValidationError> {
    if declaration.is_legacy_shell() {
        return Err(MiddlewareAttachmentValidationError::LegacyShell {
            label: declaration.label,
        });
    }

    let surface = request.surface.kind();
    if !declaration.supports(surface) {
        return Err(MiddlewareAttachmentValidationError::UnsupportedSurface {
            label: declaration.label,
            surface,
        });
    }

    if !matches!(declaration.capability, MiddlewareCapability::Control) || !surface.allows_control()
    {
        return Err(MiddlewareAttachmentValidationError::UnsupportedCapability {
            label: declaration.label,
            capability: declaration.capability,
            surface,
        });
    }

    let Some(surface_stage) = request.surface.stage_id() else {
        return Err(MiddlewareAttachmentValidationError::ReservedSurface { surface });
    };

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
/// surface, one protected unit, and the resolved origin.
#[derive(Clone, Copy)]
pub struct MiddlewareAttachmentRequest<'a> {
    pub surface: &'a MiddlewareSurface,
    pub protected_unit: &'a ProtectedUnitId,
    pub origin: &'a MiddlewareOrigin,
    pub declaration_index: MiddlewareDeclarationIndex,
}

/// Runtime construction inputs the factory needs to materialize, mirroring the
/// inputs of `create`: the stage config and the flow-scoped control aggregator.
pub struct MiddlewareMaterializationContext<'a> {
    pub config: &'a StageConfig,
    pub control_middleware: &'a Arc<ControlMiddlewareAggregator>,
    /// The type of the stage being attached to. Source-poll materialization uses
    /// this to choose the FLOWIP-114m charge position (infinite sources charge
    /// pre-poll, finite sources charge after a clean non-empty delivery); the
    /// effect and sink-delivery surfaces ignore it.
    pub stage_type: StageType,
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
/// compose with other source policies (a still-legacy rate limiter, or a second
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
#[non_exhaustive]
pub enum MiddlewareSurfaceAttachment {
    SourcePoll(SourcePollAttachment),
    Effect(EffectPolicyAttachment),
    SinkDelivery(Arc<dyn SinkPolicy>),
    Ingress(Arc<dyn IngressBoundaryMiddleware>),
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::resolved(0),
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
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::resolved(0),
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

        // An observer-capability declaration cannot attach control to Ingress.
        let observer = MiddlewareDeclaration {
            label: "observer",
            family_label: "observer",
            capability: MiddlewareCapability::Observer,
            surfaces: vec![MiddlewareSurfaceKind::Ingress],
        };
        assert!(matches!(
            validate_attachment_request(&observer, &request),
            Err(MiddlewareAttachmentValidationError::UnsupportedCapability { .. })
        ));
    }

    #[test]
    fn validates_source_poll_and_derives_stable_attachment_id() {
        let stage_id = StageId::new();
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
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::resolved(0),
        };

        let first = validate_attachment_request(&declaration, &request).unwrap();
        let second = validate_attachment_request(&declaration, &request).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.as_ulid(), second.as_ulid());

        let other_origin = MiddlewareOrigin::Flow;
        let other_origin_request = MiddlewareAttachmentRequest {
            origin: &other_origin,
            ..request
        };
        assert_ne!(
            first,
            validate_attachment_request(&declaration, &other_origin_request).unwrap()
        );

        let other_index_request = MiddlewareAttachmentRequest {
            declaration_index: MiddlewareDeclarationIndex::resolved(1),
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
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::resolved(0),
        };
        assert_ne!(
            first,
            validate_attachment_request(&declaration, &effect_request).unwrap()
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
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::resolved(0),
        };

        let err = validate_attachment_request(&declaration, &request).unwrap_err();
        assert!(matches!(
            err,
            MiddlewareAttachmentValidationError::ProtectedUnitMismatch { .. }
        ));
    }
}
