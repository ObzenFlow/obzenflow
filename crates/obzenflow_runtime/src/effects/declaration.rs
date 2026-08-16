// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::binding::EffectDeclarationBinding;
use super::*;
use obzenflow_core::StageFactSet;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectSafety {
    Idempotent,
    NonIdempotentRequiresKey,
    /// May be repeated only as a new durable attempt after an in-doubt cut.
    /// One runtime-minted affine capability authorises each attempt.
    NonIdempotentAtLeastOnce,
    Transactional,
}

/// Replay/resume safety of a configured sink (FLOWIP-120n F16).
/// Read only by the archive sink gate; live behaviour never consults it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkRedeliverySafety {
    /// Repeating a recorded write is acceptable for this configured sink.
    SafeToRepeat,
    /// Repeating a recorded write can create externally visible duplicates.
    /// Archive execution refuses without `allow_duplicate_sink_delivery`.
    DuplicateSensitive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyKeyPolicy {
    NotRequired,
    Required,
    AtLeastOnceAcknowledged,
}

/// Event-sourcing meaning of a successful effect outcome (FLOWIP-120j).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectOutcomeKind {
    /// The outcome lowers to named business facts in the stage output
    /// contract.
    DomainFacts,
    /// The outcome is framework-owned replay material returned to the
    /// handler, never a stage output fact.
    RecordedReply,
}

/// Successful domain-effect outcomes lower to their named fact set.
#[derive(Debug, Clone, Copy)]
pub struct DomainFacts;

/// Successful integration replies are recorded by the framework without
/// entering the stage's public fact set.
#[derive(Debug, Clone, Copy)]
pub struct RecordedReply;

mod sealed_outcome_semantics {
    pub trait Sealed {}

    impl Sealed for super::DomainFacts {}
    impl Sealed for super::RecordedReply {}
}

/// Runtime preparation of a successful effect result.
///
/// This is public only because it appears in the sealed
/// [`EffectOutcomeSemantics`] contract. Effect authors select `DomainFacts`
/// or `RecordedReply`; they do not construct this value.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum PreparedEffectSuccess {
    DomainFacts(Vec<TypedFact>),
    RecordedReply(Value),
}

/// Sealed semantics selected by an [`Effect`] for its successful outcome.
///
/// The associated public fact set drives both compile-time containment and
/// value-level build validation. The preparation and replay methods keep the
/// mode-specific marshalling behind the same runtime path.
pub trait EffectOutcomeSemantics<Outcome>:
    sealed_outcome_semantics::Sealed + Send + Sync + 'static
{
    type PublicFacts: StageFactSet;

    const KIND: EffectOutcomeKind;

    #[doc(hidden)]
    fn prepare_success(output: &Outcome) -> Result<PreparedEffectSuccess, EffectError>;

    #[doc(hidden)]
    fn decode_success(records: &[&EffectRecord]) -> Result<Outcome, EffectError>;
}

impl<Outcome> EffectOutcomeSemantics<Outcome> for DomainFacts
where
    Outcome: EffectOutcomeFacts + Clone + Send + Sync + 'static,
{
    type PublicFacts = Outcome;

    const KIND: EffectOutcomeKind = EffectOutcomeKind::DomainFacts;

    fn prepare_success(output: &Outcome) -> Result<PreparedEffectSuccess, EffectError> {
        let facts = output.clone().into_facts().map_err(effect_fact_set_error)?;
        if facts.is_empty() {
            return Err(EffectError::Execution(
                "domain effect success must author at least one fact".to_string(),
            ));
        }
        Ok(PreparedEffectSuccess::DomainFacts(facts))
    }

    fn decode_success(records: &[&EffectRecord]) -> Result<Outcome, EffectError> {
        decode_effect_outcome_group(records)
    }
}

impl<Outcome> EffectOutcomeSemantics<Outcome> for RecordedReply
where
    Outcome: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type PublicFacts = obzenflow_core::stage_fact_set![];

    const KIND: EffectOutcomeKind = EffectOutcomeKind::RecordedReply;

    fn prepare_success(output: &Outcome) -> Result<PreparedEffectSuccess, EffectError> {
        serde_json::to_value(output)
            .map(PreparedEffectSuccess::RecordedReply)
            .map_err(|error| EffectError::Serialization(error.to_string()))
    }

    fn decode_success(records: &[&EffectRecord]) -> Result<Outcome, EffectError> {
        validate_effect_outcome_group(records)?;
        let [record] = records else {
            return Err(EffectError::EffectProvenanceMismatch(
                "recorded-reply effect outcome must contain exactly one framework record"
                    .to_string(),
            ));
        };
        match &record.outcome {
            EffectOutcomePayload::Succeeded { output } => serde_json::from_value(output.clone())
                .map_err(|error| EffectError::Serialization(error.to_string())),
            EffectOutcomePayload::Failed { .. } => recorded_failure_from_outcome(&record.outcome),
            EffectOutcomePayload::SucceededFact { .. } => {
                Err(EffectError::EffectProvenanceMismatch(
                    "recorded-reply effect outcome used a user-owned domain-fact record"
                        .to_string(),
                ))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct EffectDeclaration {
    effect_type: &'static str,
    safety: EffectSafety,
    idempotency_key_policy: IdempotencyKeyPolicy,
    pub(crate) binding: EffectDeclarationBinding,
    /// Event-sourcing meaning selected by the effect type.
    outcome_kind: EffectOutcomeKind,
    /// Public facts this effect may author. Recorded replies project an empty
    /// set and therefore never enter a stage output contract.
    public_outcome_fact_types: Vec<TypedFactType>,
}

impl EffectDeclaration {
    pub fn of<E>() -> Self
    where
        E: Effect<BindingMode = Portless>,
    {
        Self::for_binding::<E>(EffectDeclarationBinding::Portless)
    }

    /// Declare a named effect using the lexical binding selected by `via`.
    pub fn named<E>(binding: &EffectBinding<E>) -> Self
    where
        E: NamedEffect,
    {
        Self::for_binding::<E>(binding.declaration_binding())
    }

    fn for_binding<E>(binding: EffectDeclarationBinding) -> Self
    where
        E: Effect,
    {
        assert!(
            super::binding::validate_effect_type(E::EFFECT_TYPE).is_ok(),
            "effect identifiers must contain dot-separated public identifier segments and be at most 255 ASCII bytes"
        );
        let idempotency_key_policy = match E::SAFETY {
            EffectSafety::Idempotent | EffectSafety::Transactional => {
                IdempotencyKeyPolicy::NotRequired
            }
            EffectSafety::NonIdempotentRequiresKey => IdempotencyKeyPolicy::Required,
            EffectSafety::NonIdempotentAtLeastOnce => IdempotencyKeyPolicy::NotRequired,
        };

        Self {
            effect_type: E::EFFECT_TYPE,
            safety: E::SAFETY,
            idempotency_key_policy,
            binding,
            outcome_kind: <E::OutcomeSemantics as EffectOutcomeSemantics<E::Outcome>>::KIND,
            public_outcome_fact_types: <<E::OutcomeSemantics as EffectOutcomeSemantics<
                E::Outcome,
            >>::PublicFacts as StageFactSet>::member_fact_types(
            ),
        }
    }

    pub fn effect_type(&self) -> &'static str {
        self.effect_type
    }

    pub fn safety(&self) -> EffectSafety {
        self.safety
    }

    pub fn idempotency_key_policy(&self) -> IdempotencyKeyPolicy {
        self.idempotency_key_policy
    }

    pub fn outcome_kind(&self) -> EffectOutcomeKind {
        self.outcome_kind
    }

    pub fn public_outcome_fact_types(&self) -> &[TypedFactType] {
        &self.public_outcome_fact_types
    }

    /// Validate the framework-visible effect identifier before a stage can be materialised.
    pub fn validate_public_identifiers(&self) -> Result<(), BindingIdentifierError> {
        super::binding::validate_effect_type(self.effect_type)
    }

    /// Explicit acknowledgement for an effect whose only safe recovery is a
    /// new at-least-once attempt after durable in-doubt evidence.
    pub fn at_least_once<E>() -> Self
    where
        E: Effect<BindingMode = Portless>,
    {
        let mut declaration = Self::of::<E>();
        declaration.safety = EffectSafety::NonIdempotentAtLeastOnce;
        declaration.idempotency_key_policy = IdempotencyKeyPolicy::AtLeastOnceAcknowledged;
        declaration
    }

    /// Explicit at-least-once acknowledgement for a named effect.
    pub fn named_at_least_once<E>(binding: &EffectBinding<E>) -> Self
    where
        E: NamedEffect,
    {
        let mut declaration = Self::named(binding);
        declaration.safety = EffectSafety::NonIdempotentAtLeastOnce;
        declaration.idempotency_key_policy = IdempotencyKeyPolicy::AtLeastOnceAcknowledged;
        declaration
    }

    /// Select transactional execution through the named effect's reserved typed slot.
    pub fn transactional<E>(binding: &EffectBinding<E>) -> Self
    where
        E: NamedEffect,
    {
        let mut declaration = Self::named(binding);
        declaration.safety = EffectSafety::Transactional;
        declaration.idempotency_key_policy = IdempotencyKeyPolicy::NotRequired;
        declaration
    }

    pub(crate) fn binding_identity(&self) -> obzenflow_core::EffectBindingIdentity {
        self.binding.identity()
    }

    pub(crate) fn binding(&self) -> &EffectDeclarationBinding {
        &self.binding
    }
}

#[async_trait]
pub trait Effect: Clone + std::fmt::Debug + Send + Sync + 'static {
    const EFFECT_TYPE: &'static str;
    const SCHEMA_VERSION: u32;
    const SAFETY: EffectSafety;

    /// Closed binding strategy for this effect contract.
    type BindingMode: EffectBindingMode<Self>;

    /// The typed value returned to the handler after live execution or replay.
    type Outcome: Clone + Send + Sync + 'static;

    /// Whether `Outcome` lowers to public domain facts or to a
    /// framework-owned recorded reply.
    type OutcomeSemantics: EffectOutcomeSemantics<Self::Outcome>;

    fn label(&self) -> &str;

    fn canonical_input(&self) -> Value;

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError>;

    /// Validate metadata on already-resolved required ports before boundary
    /// admission. Implementations must not perform I/O or resolve another
    /// port from this hook.
    fn validate_port_bindings(&self, _ctx: &EffectContext) -> Result<(), EffectError> {
        Ok(())
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        None
    }
}

#[async_trait]
pub trait TransactionalEffectPort<E: Effect>: Send + Sync {
    async fn execute_and_commit(
        &self,
        effect: E,
        ctx: &mut EffectContext,
        commit: EffectCommitHandle<E::Outcome, E::OutcomeSemantics>,
    ) -> Result<E::Outcome, EffectError>;
}

/// Reserved effect-local slot used by `transactional(E) via binding`.
pub const fn transactional_effect_port_slot<E: Effect>(
) -> EffectPortSlot<dyn TransactionalEffectPort<E>> {
    EffectPortSlot::new("executor")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    struct InvalidIdentifierEffect;

    #[async_trait]
    impl Effect for InvalidIdentifierEffect {
        const EFFECT_TYPE: &'static str = "https://credential-canary.example";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;
        type BindingMode = Portless;
        type Outcome = ();
        type OutcomeSemantics = RecordedReply;

        fn label(&self) -> &str {
            "invalid_identifier"
        }

        fn canonical_input(&self) -> Value {
            Value::Null
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(())
        }
    }

    #[test]
    fn invalid_effect_identifier_cannot_become_a_declaration_or_diagnostic_value() {
        let panic = std::panic::catch_unwind(EffectDeclaration::of::<InvalidIdentifierEffect>)
            .expect_err("invalid identifiers must fail at construction");
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .expect("constructor panic has a static curated message");
        assert!(!message.contains("credential-canary"));
        assert!(message.contains("effect identifiers must contain dot-separated"));
    }
}
