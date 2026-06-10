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
    /// Fact types this effect's `Output` may produce, read off the type via
    /// `TypedFactSet::fact_types()` (FLOWIP-120h). Empty for the string-only
    /// constructors, which predate typed outputs and carry no type info.
    pub output_fact_types: Vec<TypedFactType>,
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
            output_fact_types: E::Output::fact_types(),
        }
    }

    pub fn idempotent(effect_type: &'static str) -> Self {
        Self {
            effect_type,
            safety: EffectSafety::Idempotent,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports: Vec::new(),
            transactional_executor: None,
            output_fact_types: Vec::new(),
        }
    }

    pub fn non_idempotent_with_key(effect_type: &'static str) -> Self {
        Self {
            effect_type,
            safety: EffectSafety::NonIdempotentRequiresKey,
            idempotency_key_policy: IdempotencyKeyPolicy::Required,
            required_ports: Vec::new(),
            transactional_executor: None,
            output_fact_types: Vec::new(),
        }
    }

    pub fn transactional(effect_type: &'static str, executor: &'static str) -> Self {
        Self {
            effect_type,
            safety: EffectSafety::Transactional,
            idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
            required_ports: Vec::new(),
            transactional_executor: Some(executor),
            output_fact_types: Vec::new(),
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
            output_fact_types: E::Output::fact_types(),
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

    type Output: TypedFactSet + Clone + Send + Sync + 'static;

    fn label(&self) -> &str;

    fn canonical_input(&self) -> Value;

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Output, EffectError>;

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
        commit: EffectCommitHandle<E::Output>,
    ) -> Result<E::Output, EffectError>;
}
