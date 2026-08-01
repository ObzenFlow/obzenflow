// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

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

/// Declared replay/resume safety of a sink's delivery path (FLOWIP-120n F16).
/// Read only by the resume sink gate; live behaviour never consults it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkDeliverySafety {
    /// Deterministic, local, or destination-idempotent delivery: re-consuming
    /// the recorded prefix is absorbed. Resume proceeds.
    IdempotentProjection,
    /// Non-idempotent external write: catch-up re-delivery duplicates.
    /// Resume refuses without `allow_duplicate_sink_delivery`.
    NonIdempotentExternal,
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
    pub effect_type: &'static str,
    pub safety: EffectSafety,
    pub idempotency_key_policy: IdempotencyKeyPolicy,
    pub required_ports: Vec<EffectPortRequirement>,
    pub transactional_executor: Option<&'static str>,
    /// Event-sourcing meaning selected by the effect type.
    pub outcome_kind: EffectOutcomeKind,
    /// Public facts this effect may author. Recorded replies project an empty
    /// set and therefore never enter a stage output contract.
    pub public_outcome_fact_types: Vec<TypedFactType>,
}

impl EffectDeclaration {
    pub fn of<E>() -> Self
    where
        E: Effect,
    {
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
            required_ports: E::required_ports(),
            transactional_executor: None,
            outcome_kind: <E::OutcomeSemantics as EffectOutcomeSemantics<E::Outcome>>::KIND,
            public_outcome_fact_types: <<E::OutcomeSemantics as EffectOutcomeSemantics<
                E::Outcome,
            >>::PublicFacts as StageFactSet>::member_fact_types(
            ),
        }
    }

    /// Explicit acknowledgement for an effect whose only safe recovery is a
    /// new at-least-once attempt after durable in-doubt evidence.
    pub fn at_least_once<E>() -> Self
    where
        E: Effect,
    {
        let mut declaration = Self::of::<E>();
        declaration.safety = EffectSafety::NonIdempotentAtLeastOnce;
        declaration.idempotency_key_policy = IdempotencyKeyPolicy::AtLeastOnceAcknowledged;
        declaration
    }

    pub fn transactional_effect<E>(executor: &'static str) -> Self
    where
        E: Effect,
    {
        let mut required_ports = E::required_ports();
        required_ports.push(EffectPortRequirement::of::<dyn TransactionalEffectPort<E>>(
            executor,
        ));

        Self {
            effect_type: E::EFFECT_TYPE,
            safety: EffectSafety::Transactional,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports,
            transactional_executor: Some(executor),
            outcome_kind: <E::OutcomeSemantics as EffectOutcomeSemantics<E::Outcome>>::KIND,
            public_outcome_fact_types: <<E::OutcomeSemantics as EffectOutcomeSemantics<
                E::Outcome,
            >>::PublicFacts as StageFactSet>::member_fact_types(
            ),
        }
    }

    pub fn require_port<T>(mut self, name: impl Into<String>) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.required_ports
            .push(EffectPortRequirement::of::<T>(name));
        self
    }
}

#[async_trait]
pub trait Effect: Clone + std::fmt::Debug + Send + Sync + 'static {
    const EFFECT_TYPE: &'static str;
    const SCHEMA_VERSION: u32;
    const SAFETY: EffectSafety;

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

    fn required_ports() -> Vec<EffectPortRequirement> {
        Vec::new()
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
