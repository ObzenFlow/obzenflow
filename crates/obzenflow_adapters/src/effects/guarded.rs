// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The guarded effect wrapper and lifted breaker-outcome carrier (FLOWIP-120h).
//!
//! `Guarded<E, F, R>` lifts an effect's output into
//! `CircuitBreakerOutcome<E::Outcome, F, R>` so circuit-breaker branches are
//! explicit in the type at the call site, through the unchanged `fx.perform`
//! verb. The wrapper delegates every piece of effect identity to the inner
//! effect, so the guarded call records and replays under exactly the inner
//! effect's descriptor and cursor, and the carrier is transient control-flow
//! machinery: what persists are the flat branch facts, never a wrapper event.

use async_trait::async_trait;
use obzenflow_core::event::schema::{
    TypedFact, TypedFactSet, TypedFactSetError, TypedFactType, TypedPayload,
};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectPortRequirement, EffectSafety, IdempotencyKey,
};
use serde_json::Value;
use std::marker::PhantomData;

/// Transient lifted carrier for a breaker-guarded effect outcome.
///
/// Never persisted and never an arrow member: the journal holds the branch
/// facts (`P`'s fact set, the fallback fact `F`, or the rejection fact `R`),
/// and `try_from_facts` reconstructs the branch from the recorded group's
/// event types alone, live and replay alike. Reasons live in the branch fact
/// payloads, not in carrier fields. Deliberately exhaustive: handlers must
/// match every branch explicitly, which is the point of the lifted type.
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitBreakerOutcome<P, F, R> {
    /// The protected effect ran and produced its normal outcome.
    Primary(P),
    /// The breaker prevented the call and synthesized the fallback fact.
    Fallback(F),
    /// The breaker prevented the call and synthesized the rejection fact.
    Rejected(R),
}

impl<P, F, R> TypedFactSet for CircuitBreakerOutcome<P, F, R>
where
    P: TypedFactSet + Clone + Send + Sync + 'static,
    F: TypedPayload + Clone + Send + Sync + 'static,
    R: TypedPayload + Clone + Send + Sync + 'static,
{
    fn fact_types() -> Vec<TypedFactType> {
        let mut types = P::fact_types();
        types.push(TypedFactType::of::<F>());
        types.push(TypedFactType::of::<R>());
        types
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        match self {
            Self::Primary(primary) => primary.into_facts(),
            Self::Fallback(fallback) => Ok(vec![TypedFact::from_payload(fallback)?]),
            Self::Rejected(rejected) => Ok(vec![TypedFact::from_payload(rejected)?]),
        }
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        // The event type is the branch discriminator (flat-fact lock): branch
        // facts are single-fact groups disjoint from the effect's own set.
        if let [fact] = facts {
            if R::event_type_matches(fact.event_type.as_str()) {
                let rejected = serde_json::from_value(fact.payload.clone()).map_err(|e| {
                    TypedFactSetError::DeserializationFailed {
                        event_type: fact.event_type.clone(),
                        error: e.to_string(),
                    }
                })?;
                return Ok(Self::Rejected(rejected));
            }
            if F::event_type_matches(fact.event_type.as_str()) {
                let fallback = serde_json::from_value(fact.payload.clone()).map_err(|e| {
                    TypedFactSetError::DeserializationFailed {
                        event_type: fact.event_type.clone(),
                        error: e.to_string(),
                    }
                })?;
                return Ok(Self::Fallback(fallback));
            }
        }
        Ok(Self::Primary(P::try_from_facts(facts)?))
    }

    fn synthesized_fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<F>(), TypedFactType::of::<R>()]
    }
}

/// Effect wrapper lifting the output into [`CircuitBreakerOutcome`].
///
/// All effect identity delegates to the inner effect (`EFFECT_TYPE`,
/// `SCHEMA_VERSION`, `SAFETY`, `label`, `canonical_input`, the idempotency
/// key, and the required ports), so wrapping changes only the carrier type,
/// never the recorded descriptor, hash, or cursor, and the stage's
/// `effects: [...]` declaration is untouched.
pub struct Guarded<E, F, R> {
    inner: E,
    _branches: PhantomData<fn() -> (F, R)>,
}

impl<E, F, R> Guarded<E, F, R> {
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            _branches: PhantomData,
        }
    }
}

// Manual impls: deriving would wrongly bound F and R, which are phantom.
impl<E: Clone, F, R> Clone for Guarded<E, F, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _branches: PhantomData,
        }
    }
}

impl<E: std::fmt::Debug, F, R> std::fmt::Debug for Guarded<E, F, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guarded")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl<E, F, R> Effect for Guarded<E, F, R>
where
    E: Effect,
    F: TypedPayload + Clone + Send + Sync + 'static,
    R: TypedPayload + Clone + Send + Sync + 'static,
{
    const EFFECT_TYPE: &'static str = E::EFFECT_TYPE;
    const SCHEMA_VERSION: u32 = E::SCHEMA_VERSION;
    const SAFETY: EffectSafety = E::SAFETY;

    type Outcome = CircuitBreakerOutcome<E::Outcome, F, R>;

    fn label(&self) -> &str {
        self.inner.label()
    }

    fn canonical_input(&self) -> Value {
        self.inner.canonical_input()
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        self.inner.idempotency_key()
    }

    fn required_ports() -> Vec<EffectPortRequirement> {
        E::required_ports()
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.inner
            .execute(ctx)
            .await
            .map(CircuitBreakerOutcome::Primary)
    }
}

/// Extension-method sugar: `fx.perform(effect.guarded::<F, R>())`.
pub trait GuardedEffectExt: Effect + Sized {
    fn guarded<F, R>(self) -> Guarded<Self, F, R>
    where
        F: TypedPayload + Clone + Send + Sync + 'static,
        R: TypedPayload + Clone + Send + Sync + 'static,
    {
        Guarded::new(self)
    }
}

impl<E: Effect> GuardedEffectExt for E {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct PrimaryFact {
        value: u32,
    }

    impl TypedPayload for PrimaryFact {
        const EVENT_TYPE: &'static str = "guarded.primary";
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct FallbackFact {
        value: u32,
    }

    impl TypedPayload for FallbackFact {
        const EVENT_TYPE: &'static str = "guarded.fallback";
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct RejectedFact {
        reason: String,
    }

    impl TypedPayload for RejectedFact {
        const EVENT_TYPE: &'static str = "guarded.rejected";
    }

    #[derive(Debug, Clone)]
    struct DemoEffect {
        value: u32,
    }

    #[async_trait]
    impl Effect for DemoEffect {
        const EFFECT_TYPE: &'static str = "test.demo";
        const SCHEMA_VERSION: u32 = 3;
        const SAFETY: EffectSafety = EffectSafety::Idempotent;

        type Outcome = PrimaryFact;

        fn label(&self) -> &str {
            "demo"
        }

        fn canonical_input(&self) -> Value {
            json!({ "value": self.value })
        }

        async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
            Ok(PrimaryFact { value: self.value })
        }
    }

    type DemoOutcome = CircuitBreakerOutcome<PrimaryFact, FallbackFact, RejectedFact>;
    type GuardedDemo = Guarded<DemoEffect, FallbackFact, RejectedFact>;

    #[test]
    fn carrier_round_trips_every_branch_by_event_type() {
        for outcome in [
            DemoOutcome::Primary(PrimaryFact { value: 1 }),
            DemoOutcome::Fallback(FallbackFact { value: 2 }),
            DemoOutcome::Rejected(RejectedFact {
                reason: "rejected_circuit_open".to_string(),
            }),
        ] {
            let facts = outcome.clone().into_facts().expect("branch serializes");
            let reconstructed =
                DemoOutcome::try_from_facts(&facts).expect("branch reconstructs from facts");
            assert_eq!(reconstructed, outcome);
        }
    }

    #[test]
    fn carrier_declares_branch_facts_as_synthesized() {
        let synthesized = DemoOutcome::synthesized_fact_types();
        assert_eq!(synthesized.len(), 2);
        assert!(synthesized
            .iter()
            .any(|t| FallbackFact::event_type_matches(t.event_type.as_str())));
        assert!(synthesized
            .iter()
            .any(|t| RejectedFact::event_type_matches(t.event_type.as_str())));

        assert!(PrimaryFact::synthesized_fact_types().is_empty());
    }

    #[test]
    fn guarded_wrapper_delegates_every_descriptor_input() {
        // `descriptor_for_effect` is a pure function of these values plus the
        // stage logic version, so their equality proves descriptor, hash, and
        // cursor identity are unchanged by wrapping.
        assert_eq!(GuardedDemo::EFFECT_TYPE, DemoEffect::EFFECT_TYPE);
        assert_eq!(GuardedDemo::SCHEMA_VERSION, DemoEffect::SCHEMA_VERSION);
        assert!(matches!(GuardedDemo::SAFETY, EffectSafety::Idempotent));

        let inner = DemoEffect { value: 9 };
        let guarded = inner.clone().guarded::<FallbackFact, RejectedFact>();
        assert_eq!(guarded.label(), inner.label());
        assert_eq!(guarded.canonical_input(), inner.canonical_input());
        assert!(guarded.idempotency_key().is_none());
    }
}
