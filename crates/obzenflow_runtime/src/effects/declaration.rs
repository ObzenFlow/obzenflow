// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectSafety {
    Idempotent,
    NonIdempotentRequiresKey,
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
}

#[derive(Debug, Clone)]
pub struct EffectDeclaration {
    pub effect_type: &'static str,
    pub safety: EffectSafety,
    pub idempotency_key_policy: IdempotencyKeyPolicy,
    pub required_ports: Vec<EffectPortRequirement>,
    pub transactional_executor: Option<&'static str>,
    /// Fact types this effect's `Outcome` may produce, read off the type via
    /// `TypedFactSet::fact_types()` (FLOWIP-120h). Build validation checks
    /// these against the stage output contract unconditionally (FLOWIP-120m).
    pub outcome_fact_types: Vec<TypedFactType>,
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
        };

        Self {
            effect_type: E::EFFECT_TYPE,
            safety: E::SAFETY,
            idempotency_key_policy,
            required_ports: E::required_ports(),
            transactional_executor: None,
            outcome_fact_types: E::Outcome::fact_types(),
        }
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
            outcome_fact_types: E::Outcome::fact_types(),
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

    /// The closed set of successful facts this effect may produce, as a
    /// transient outcome carrier (FLOWIP-120m). A scalar `TypedPayload` is a
    /// valid carrier for single-fact effects.
    type Outcome: EffectOutcomeFacts + Clone + Send + Sync + 'static;

    fn label(&self) -> &str;

    fn canonical_input(&self) -> Value;

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError>;

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
        commit: EffectCommitHandle<E::Outcome>,
    ) -> Result<E::Outcome, EffectError>;
}
