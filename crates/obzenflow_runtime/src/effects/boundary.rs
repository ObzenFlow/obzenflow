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

pub enum EffectBoundaryAction {
    Continue,
    Skip(Vec<ChainEvent>),
    Abort,
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
