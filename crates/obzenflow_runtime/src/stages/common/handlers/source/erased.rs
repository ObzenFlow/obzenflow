// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sealed source execution surface (FLOWIP-134g).

use super::traits::SourceError;
use crate::stages::common::handler_error::StageFatal;
use async_trait::async_trait;
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::ingress::HostedIngressBindingSlot;
use obzenflow_core::{ChainEvent, EventId};
use std::time::Duration;

/// Successful completion of one erased source poll.
#[doc(hidden)]
#[derive(Debug)]
pub enum ErasedSourceCompletion {
    Batch(Vec<ChainEvent>),
    Eof,
}

/// Mutually exclusive outcome of one erased source poll.
#[doc(hidden)]
#[derive(Debug)]
pub enum ErasedSourceOutcome {
    Completed(ErasedSourceCompletion),
    HandlerError(SourceError),
    Fatal(StageFatal),
}

#[doc(hidden)]
#[derive(Debug)]
pub struct ErasedSourceInvocation {
    outcome: ErasedSourceOutcome,
}

impl ErasedSourceInvocation {
    pub(crate) fn completed(completion: ErasedSourceCompletion) -> Self {
        Self {
            outcome: ErasedSourceOutcome::Completed(completion),
        }
    }

    pub(crate) fn handler_error(error: SourceError) -> Self {
        Self {
            outcome: ErasedSourceOutcome::HandlerError(error),
        }
    }

    pub(crate) fn fatal(fatal: StageFatal) -> Self {
        Self {
            outcome: ErasedSourceOutcome::Fatal(fatal),
        }
    }

    pub fn into_outcome(self) -> ErasedSourceOutcome {
        self.outcome
    }
}

mod sealed {
    pub trait AsyncFinite {}
    pub trait AsyncInfinite {}
}

/// Runtime-erased asynchronous finite source interface.
#[doc(hidden)]
#[async_trait]
pub trait UnifiedAsyncFiniteSourceHandler: sealed::AsyncFinite + Send + Sync {
    fn has_pending_records(&self) -> bool {
        false
    }
    fn replayed_facts(&self) -> u64 {
        0
    }
    fn idle_backoff_cap(&self) -> Duration {
        Duration::from_millis(50)
    }
    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::AsyncFiniteSource
    }
    fn read_declaration(
        &self,
    ) -> Result<crate::effects::EffectDeclaration, crate::effects::StageEffectsError>;
    fn install_read_context(&mut self, context: crate::effects::EffectInvocationContext);
    fn poll_cancelled(&mut self) {}
    fn poll_timeout(&self) -> Option<Duration>;
    async fn next_invocation(&mut self) -> ErasedSourceInvocation;
}

/// Runtime-erased asynchronous infinite source interface.
#[doc(hidden)]
#[async_trait]
pub trait UnifiedAsyncInfiniteSourceHandler: sealed::AsyncInfinite + Send + Sync {
    fn has_pending_records(&self) -> bool {
        false
    }
    fn idle_backoff_cap(&self) -> Duration {
        Duration::from_millis(50)
    }
    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::AsyncInfiniteSource
    }
    fn read_declaration(
        &self,
    ) -> Result<crate::effects::EffectDeclaration, crate::effects::StageEffectsError>;
    fn install_read_context(&mut self, context: crate::effects::EffectInvocationContext);
    fn poll_cancelled(&mut self) {}
    async fn cross_to_live(
        &mut self,
    ) -> Result<bool, crate::stages::common::handler_error::StageFatal> {
        Ok(false)
    }
    fn replayed_facts(&self) -> u64 {
        0
    }
    fn poll_timeout(&self) -> Option<Duration>;
    /// The binding slot of Runtime-owned hosted ingress (FLOWIP-122i B3).
    fn hosted_ingress_slot(&self) -> Option<HostedIngressBindingSlot> {
        None
    }
    async fn next_invocation(&mut self) -> ErasedSourceInvocation;
    /// Rows the journal committed, so hosted receipts can resolve (FLOWIP-084l).
    fn settle_committed(&mut self, _committed: &[EventId]) {}
}

pub(crate) use sealed::{AsyncFinite as SealAsyncFinite, AsyncInfinite as SealAsyncInfinite};
