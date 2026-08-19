// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed effect-binding declarations and invocation projections (FLOWIP-132a).

use super::ports::PendingRegistrationPackage;
use super::Effect;
use obzenflow_core::{BindingEvidenceDigest, BoundedBindingEvidence, EffectBindingIdentity};
use ring::digest::{digest, SHA256};
use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const BINDING_EVIDENCE_DOMAIN: &[u8] = b"obzenflow.binding-evidence:v1";

/// Credential-free, deterministic evidence for one named effect binding.
pub trait EffectBindingEvidence: Clone + Eq + Send + Sync + 'static {
    const SCHEMA_VERSION: u32;

    fn canonical_bytes(&self) -> BoundedBindingEvidence;
}

mod private {
    pub trait Sealed {}
}

/// Closed strategy selected by an [`Effect`] for binding authority.
pub trait EffectBindingMode<E: Effect>: private::Sealed + Send + Sync + 'static {
    #[doc(hidden)]
    fn invocation_binding(effect: &E) -> EffectInvocationBinding;
}

/// An effect with no named binding contract.
#[derive(Debug, Clone, Copy, Default)]
pub struct Portless;

/// An effect whose declaration requires typed evidence and lexical `via`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Named<B: EffectBindingEvidence>(PhantomData<fn() -> B>);

impl private::Sealed for Portless {}
impl<B: EffectBindingEvidence> private::Sealed for Named<B> {}

/// Observable logical name chosen by the trusted composition root.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalEffectBindingName(String);

impl LogicalEffectBindingName {
    pub fn new(value: impl Into<String>) -> Result<Self, BindingIdentifierError> {
        let value = value.into();
        if !is_public_identifier(&value) {
            return Err(BindingIdentifierError::InvalidLogicalBindingName);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for LogicalEffectBindingName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("LogicalEffectBindingName")
            .field(&self.0)
            .finish()
    }
}

impl std::fmt::Display for LogicalEffectBindingName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<&str> for LogicalEffectBindingName {
    type Error = BindingIdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BindingIdentifierError {
    #[error("logical effect binding names must match [A-Za-z_][A-Za-z0-9_]{{0,63}}")]
    InvalidLogicalBindingName,
    #[error("effect port slot names must match [A-Za-z_][A-Za-z0-9_]{{0,63}}")]
    InvalidSlotName,
    #[error("effect identifiers must contain dot-separated public identifier segments and be at most 255 ASCII bytes")]
    InvalidEffectType,
}

pub(crate) fn validate_effect_type(value: &str) -> Result<(), BindingIdentifierError> {
    if value.is_empty()
        || value.len() > 255
        || !value.is_ascii()
        || value
            .split('.')
            .any(|segment| !is_public_identifier(segment))
    {
        return Err(BindingIdentifierError::InvalidEffectType);
    }
    Ok(())
}

pub(crate) fn validate_slot_name(value: &str) -> Result<(), BindingIdentifierError> {
    if is_public_identifier(value) {
        Ok(())
    } else {
        Err(BindingIdentifierError::InvalidSlotName)
    }
}

fn is_public_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 || !value.is_ascii() {
        return false;
    }
    matches!(bytes[0], b'A'..=b'Z' | b'a'..=b'z' | b'_')
        && bytes[1..]
            .iter()
            .all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

const fn is_public_identifier_const(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return false;
    }
    let first = bytes[0];
    if !matches!(first, b'A'..=b'Z' | b'a'..=b'z' | b'_') {
        return false;
    }
    let mut index = 1;
    while index < bytes.len() {
        let byte = bytes[index];
        if !matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_') {
            return false;
        }
        index += 1;
    }
    true
}

/// Validated public label used by framework target-invariant diagnostics.
///
/// Values can only be obtained from an [`EffectPortSlot`], whose constructor
/// enforces the locked identifier grammar. The inner string is deliberately
/// not publicly constructible.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectPortSlotLabel(&'static str);

impl EffectPortSlotLabel {
    pub fn as_str(self) -> &'static str {
        self.0
    }
}

impl std::fmt::Debug for EffectPortSlotLabel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("EffectPortSlotLabel")
            .field(&self.0)
            .finish()
    }
}

impl std::fmt::Display for EffectPortSlotLabel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Marker for an effect port with no pre-boundary metadata projection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoPortMetadata;

/// One typed, effect-local slot declaration.
///
/// `P` is callable authority available only to the protected effect
/// operation. `M` is the immutable metadata snapshot available to
/// pre-boundary validation and defaults to no metadata.
pub struct EffectPortSlot<
    P: ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static = NoPortMetadata,
> {
    label: &'static str,
    _port: PhantomData<fn() -> P>,
    _metadata: PhantomData<fn() -> M>,
}

impl<P, M> EffectPortSlot<P, M>
where
    P: ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    pub const fn new(label: &'static str) -> Self {
        assert!(
            is_public_identifier_const(label),
            "effect port slot names must match [A-Za-z_][A-Za-z0-9_]{{0,63}}"
        );
        Self {
            label,
            _port: PhantomData,
            _metadata: PhantomData,
        }
    }

    pub const fn label(self) -> &'static str {
        self.label
    }

    pub(crate) const fn diagnostic_label(self) -> EffectPortSlotLabel {
        EffectPortSlotLabel(self.label)
    }

    pub(super) fn requirement(self) -> EffectPortSlotRequirement {
        EffectPortSlotRequirement {
            port_type_id: TypeId::of::<P>(),
            metadata_type_id: TypeId::of::<M>(),
            label: self.label,
        }
    }
}

impl<P, M> Clone for EffectPortSlot<P, M>
where
    P: ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<P, M> Copy for EffectPortSlot<P, M>
where
    P: ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
}

impl<P, M> std::fmt::Debug for EffectPortSlot<P, M>
where
    P: ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectPortSlot")
            .field("label", &self.label)
            .finish()
    }
}

#[derive(Clone, Default)]
pub struct EffectPortSlotSet {
    pub(super) slots: Vec<EffectPortSlotRequirement>,
}

impl EffectPortSlotSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn single<P, M>(slot: EffectPortSlot<P, M>) -> Self
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        Self::new().with(slot)
    }

    pub fn with<P, M>(mut self, slot: EffectPortSlot<P, M>) -> Self
    where
        P: ?Sized + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        self.slots.push(slot.requirement());
        self
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

impl std::fmt::Debug for EffectPortSlotSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_list()
            .entries(self.slots.iter().map(|slot| slot.label))
            .finish()
    }
}

#[derive(Clone)]
pub(super) struct EffectPortSlotRequirement {
    pub(super) port_type_id: TypeId,
    pub(super) metadata_type_id: TypeId,
    pub(super) label: &'static str,
}

#[derive(Clone)]
pub(super) struct BindingFamily(Arc<()>);

impl BindingFamily {
    fn mint() -> Self {
        Self(Arc::new(()))
    }

    pub(super) fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct BindingCoordinate(u64);

impl BindingCoordinate {
    pub(super) fn mint() -> Self {
        static NEXT_COORDINATE: AtomicU64 = AtomicU64::new(1);
        Self(NEXT_COORDINATE.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Clone)]
pub(super) struct BoundEffectPortSlot {
    pub(super) requirement: EffectPortSlotRequirement,
    pub(super) coordinate: BindingCoordinate,
}

/// Typed declaration value selected lexically by `via`.
#[derive(Clone)]
pub struct EffectBinding<E: NamedEffect> {
    logical_name: LogicalEffectBindingName,
    projection: EffectBindingUse<E>,
    registration: BindingCoordinate,
    slots: Arc<Vec<BoundEffectPortSlot>>,
    package: Arc<PendingRegistrationPackage>,
    _effect: PhantomData<fn() -> E>,
}

impl<E: NamedEffect> EffectBinding<E> {
    pub(super) fn from_parts(
        logical_name: LogicalEffectBindingName,
        evidence: E::BindingEvidence,
        registration: BindingCoordinate,
        slots: Vec<BoundEffectPortSlot>,
        package: Arc<PendingRegistrationPackage>,
    ) -> Self {
        let projection = EffectBindingUse {
            family: BindingFamily::mint(),
            evidence,
            _effect: PhantomData,
        };
        Self {
            logical_name,
            projection,
            registration,
            slots: Arc::new(slots),
            package,
            _effect: PhantomData,
        }
    }

    pub fn logical_name(&self) -> &LogicalEffectBindingName {
        &self.logical_name
    }

    pub fn evidence(&self) -> &E::BindingEvidence {
        self.projection.evidence()
    }

    pub fn invocation(&self) -> EffectBindingUse<E> {
        self.projection.clone()
    }

    #[cfg(test)]
    pub(crate) fn invocation_with_evidence_for_test(
        &self,
        evidence: E::BindingEvidence,
    ) -> EffectBindingUse<E> {
        EffectBindingUse {
            family: self.projection.family.clone(),
            evidence,
            _effect: PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn shares_construction_family(&self, other: &Self) -> bool {
        self.projection.family.same_as(&other.projection.family)
    }

    pub(crate) fn declaration_binding(&self) -> EffectDeclarationBinding {
        EffectDeclarationBinding::Named(NamedEffectDeclarationBinding {
            logical_name: self.logical_name.clone(),
            family: self.projection.family.clone(),
            identity: binding_identity::<E>(self.projection.evidence()),
            projection: Arc::new(self.projection.clone()),
            registration: self.registration,
            slots: Arc::clone(&self.slots),
            package: Some(Arc::clone(&self.package)),
        })
    }
}

impl<E: NamedEffect> std::fmt::Debug for EffectBinding<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectBinding")
            .field("effect_type", &E::EFFECT_TYPE)
            .field("logical_name", &self.logical_name)
            .field(
                "slots",
                &self
                    .slots
                    .iter()
                    .map(|slot| slot.requirement.label)
                    .collect::<Vec<_>>(),
            )
            .field("evidence", &"<not disclosed>")
            .field("family", &"<not disclosed>")
            .finish()
    }
}

/// Invocation-only projection retained by a named effect value.
#[derive(Clone)]
pub struct EffectBindingUse<E: NamedEffect> {
    family: BindingFamily,
    evidence: E::BindingEvidence,
    _effect: PhantomData<fn() -> E>,
}

impl<E: NamedEffect> EffectBindingUse<E> {
    pub fn evidence(&self) -> &E::BindingEvidence {
        &self.evidence
    }

    fn erased(&self) -> EffectInvocationBinding {
        EffectInvocationBinding {
            kind: EffectInvocationBindingKind::Named {
                family: self.family.clone(),
                identity: binding_identity::<E>(&self.evidence),
            },
        }
    }
}

impl<E: NamedEffect> std::fmt::Debug for EffectBindingUse<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectBindingUse")
            .field("effect_type", &E::EFFECT_TYPE)
            .field("evidence", &"<not disclosed>")
            .field("family", &"<not disclosed>")
            .finish()
    }
}

/// Effect contract implemented only by named effects.
pub trait NamedEffect: Effect<BindingMode = Named<Self::BindingEvidence>> {
    type BindingEvidence: EffectBindingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self>;

    fn required_slots() -> EffectPortSlotSet;
}

#[doc(hidden)]
#[derive(Clone)]
pub struct EffectInvocationBinding {
    pub(super) kind: EffectInvocationBindingKind,
}

#[derive(Clone)]
pub(super) enum EffectInvocationBindingKind {
    Portless,
    Named {
        family: BindingFamily,
        identity: EffectBindingIdentity,
    },
}

impl std::fmt::Debug for EffectInvocationBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            EffectInvocationBindingKind::Portless => {
                formatter.write_str("EffectInvocationBinding::Portless")
            }
            EffectInvocationBindingKind::Named { .. } => formatter
                .debug_struct("EffectInvocationBinding::Named")
                .field("authority", &"<not disclosed>")
                .finish(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum EffectDeclarationBinding {
    Portless,
    Named(NamedEffectDeclarationBinding),
}

#[derive(Clone)]
pub(crate) struct NamedEffectDeclarationBinding {
    pub(crate) logical_name: LogicalEffectBindingName,
    pub(super) family: BindingFamily,
    pub(crate) identity: EffectBindingIdentity,
    projection: Arc<dyn Any + Send + Sync>,
    pub(super) registration: BindingCoordinate,
    pub(super) slots: Arc<Vec<BoundEffectPortSlot>>,
    pub(super) package: Option<Arc<PendingRegistrationPackage>>,
}

impl std::fmt::Debug for EffectDeclarationBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Portless => formatter.write_str("EffectDeclarationBinding::Portless"),
            Self::Named(named) => formatter
                .debug_struct("EffectDeclarationBinding::Named")
                .field("logical_name", &named.logical_name)
                .field("identity", &named.identity)
                .field(
                    "slots",
                    &named
                        .slots
                        .iter()
                        .map(|slot| slot.requirement.label)
                        .collect::<Vec<_>>(),
                )
                .field("family", &"<not disclosed>")
                .finish(),
        }
    }
}

impl<E> EffectBindingMode<E> for Portless
where
    E: Effect<BindingMode = Portless>,
{
    fn invocation_binding(_effect: &E) -> EffectInvocationBinding {
        EffectInvocationBinding {
            kind: EffectInvocationBindingKind::Portless,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BindingMatchError {
    Mode,
    Family,
    Evidence,
}

impl EffectDeclarationBinding {
    pub(super) fn runtime_projection(&self) -> Self {
        let mut projection = self.clone();
        if let Self::Named(named) = &mut projection {
            named.package = None;
        }
        projection
    }

    pub(super) fn typed_projection<E: NamedEffect>(&self) -> Option<EffectBindingUse<E>> {
        let Self::Named(named) = self else {
            return None;
        };
        named
            .projection
            .downcast_ref::<EffectBindingUse<E>>()
            .cloned()
    }

    pub(super) fn pending_package(&self) -> Option<&Arc<PendingRegistrationPackage>> {
        match self {
            Self::Portless => None,
            Self::Named(named) => named.package.as_ref(),
        }
    }

    pub(crate) fn identity(&self) -> EffectBindingIdentity {
        match self {
            Self::Portless => EffectBindingIdentity::Portless,
            Self::Named(named) => named.identity.clone(),
        }
    }

    pub(crate) fn logical_name(&self) -> Option<&LogicalEffectBindingName> {
        match self {
            Self::Portless => None,
            Self::Named(named) => Some(&named.logical_name),
        }
    }

    pub(super) fn slots(&self) -> &[BoundEffectPortSlot] {
        match self {
            Self::Portless => &[],
            Self::Named(named) => named.slots.as_slice(),
        }
    }

    pub(super) fn named_parts(
        &self,
    ) -> Option<(
        &LogicalEffectBindingName,
        BindingCoordinate,
        &[BoundEffectPortSlot],
    )> {
        match self {
            Self::Portless => None,
            Self::Named(named) => Some((
                &named.logical_name,
                named.registration,
                named.slots.as_slice(),
            )),
        }
    }

    pub(crate) fn declared_slot_label(&self, label: &str) -> Option<&'static str> {
        self.slots()
            .iter()
            .find(|slot| slot.requirement.label == label)
            .map(|slot| slot.requirement.label)
    }

    pub(crate) fn compare_invocation(
        &self,
        invocation: &EffectInvocationBinding,
    ) -> Result<(), BindingMatchError> {
        match (self, &invocation.kind) {
            (Self::Portless, EffectInvocationBindingKind::Portless) => Ok(()),
            (Self::Named(declaration), EffectInvocationBindingKind::Named { family, identity }) => {
                if !declaration.family.same_as(family) {
                    return Err(BindingMatchError::Family);
                }
                if &declaration.identity != identity {
                    return Err(BindingMatchError::Evidence);
                }
                Ok(())
            }
            _ => Err(BindingMatchError::Mode),
        }
    }
}

impl<E, B> EffectBindingMode<E> for Named<B>
where
    B: EffectBindingEvidence,
    E: NamedEffect<BindingEvidence = B>,
{
    fn invocation_binding(effect: &E) -> EffectInvocationBinding {
        effect.binding_use().erased()
    }
}

pub(crate) fn binding_identity<E: NamedEffect>(
    evidence: &E::BindingEvidence,
) -> EffectBindingIdentity {
    let mut framed = Vec::new();
    push_frame(&mut framed, BINDING_EVIDENCE_DOMAIN);
    push_frame(&mut framed, E::EFFECT_TYPE.as_bytes());
    push_frame(
        &mut framed,
        &E::BindingEvidence::SCHEMA_VERSION.to_be_bytes(),
    );
    let canonical = evidence.canonical_bytes();
    push_frame(&mut framed, canonical.as_bytes());
    let hashed = digest(&SHA256, &framed);
    EffectBindingIdentity::Named {
        evidence: BindingEvidenceDigest::new(
            E::BindingEvidence::SCHEMA_VERSION,
            hex_digest(hashed.as_ref()),
        ),
    }
}

fn push_frame(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_identifier_grammar_is_closed_and_bounded() {
        assert!(is_public_identifier("chat_1"));
        assert!(!is_public_identifier(""));
        assert!(!is_public_identifier("1chat"));
        assert!(!is_public_identifier("chat-secret"));
        assert!(!is_public_identifier(&"a".repeat(65)));
    }

    #[test]
    fn effect_identifier_requires_valid_dot_segments() {
        assert!(validate_effect_type("obzenflow.ai.chat_completion").is_ok());
        assert!(validate_effect_type("obzenflow..chat").is_err());
        assert!(validate_effect_type("obzenflow.chat-secret").is_err());
    }
}
