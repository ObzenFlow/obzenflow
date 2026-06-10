// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

pub struct EffectBoundaryContext {
    inner: Box<dyn Any + Send>,
}

impl EffectBoundaryContext {
    pub fn new<T>(inner: T) -> Self
    where
        T: Any + Send,
    {
        Self {
            inner: Box::new(inner),
        }
    }

    pub fn downcast<T>(self) -> Result<T, Self>
    where
        T: Any + Send,
    {
        match self.inner.downcast::<T>() {
            Ok(value) => Ok(*value),
            Err(inner) => Err(Self { inner }),
        }
    }
}

/// Structured, policy-neutral reason carried by a boundary abort so the
/// rejection is recorded under the effect cursor and replays deterministically.
#[derive(Debug, Clone)]
pub struct EffectAbortReason {
    pub cause: EffectFailureCause,
    pub message: String,
    pub retry: RetryDisposition,
}

pub enum EffectBoundaryAction {
    Continue,
    Skip {
        results: Vec<ChainEvent>,
        /// Label of the middleware that synthesized the results, recorded as
        /// the outcome group's `EffectFactOrigin` (FLOWIP-120h).
        source: Option<String>,
    },
    Abort(EffectAbortReason),
}

pub struct EffectBoundaryStart {
    pub action: EffectBoundaryAction,
    pub context: EffectBoundaryContext,
    pub control_events: Vec<ChainEvent>,
}

pub trait EffectBoundaryMiddleware: Send + Sync {
    fn before_effect(&self, event: &ChainEvent) -> EffectBoundaryStart;

    fn after_effect(
        &self,
        context: EffectBoundaryContext,
        event: &ChainEvent,
        outputs: &[ChainEvent],
    ) -> Vec<ChainEvent>;
}
