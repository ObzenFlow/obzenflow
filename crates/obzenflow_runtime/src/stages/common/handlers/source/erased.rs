// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sealed source execution surface (FLOWIP-134g).

use super::traits::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler, SourceError,
};
use crate::stages::common::handler_error::StageFatal;
use async_trait::async_trait;
use obzenflow_core::ingress::HostedIngressBindingSlot;
use obzenflow_core::{ChainEvent, WriterId};
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

/// One source poll plus its closed, runtime-authored operational outbox.
#[doc(hidden)]
#[derive(Debug)]
pub struct ErasedSourceInvocation {
    outcome: ErasedSourceOutcome,
    observations: Vec<ChainEvent>,
}

impl ErasedSourceInvocation {
    pub(crate) fn completed(
        completion: ErasedSourceCompletion,
        observations: Vec<ChainEvent>,
    ) -> Self {
        Self {
            outcome: ErasedSourceOutcome::Completed(completion),
            observations,
        }
    }

    pub(crate) fn handler_error(error: SourceError, observations: Vec<ChainEvent>) -> Self {
        Self {
            outcome: ErasedSourceOutcome::HandlerError(error),
            observations,
        }
    }

    pub(crate) fn fatal(fatal: StageFatal) -> Self {
        Self {
            outcome: ErasedSourceOutcome::Fatal(fatal),
            observations: Vec::new(),
        }
    }

    pub fn into_parts(self) -> (ErasedSourceOutcome, Vec<ChainEvent>) {
        (self.outcome, self.observations)
    }
}

mod sealed {
    pub trait Finite {}
    pub trait AsyncFinite {}
    pub trait Infinite {}
    pub trait AsyncInfinite {}
}

/// Runtime-erased finite source interface.
#[doc(hidden)]
pub trait UnifiedFiniteSourceHandler: sealed::Finite + Send + Sync {
    fn install_writer_id(&mut self, writer_id: WriterId);
    fn next_invocation(&mut self) -> ErasedSourceInvocation;
}

/// Runtime-erased asynchronous finite source interface.
#[doc(hidden)]
#[async_trait]
pub trait UnifiedAsyncFiniteSourceHandler: sealed::AsyncFinite + Send + Sync {
    fn install_writer_id(&mut self, writer_id: WriterId);
    fn poll_timeout(&self) -> Option<Duration>;
    async fn next_invocation(&mut self) -> ErasedSourceInvocation;
    async fn drain(&mut self) -> Result<(), SourceError>;
}

/// Runtime-erased infinite source interface.
#[doc(hidden)]
pub trait UnifiedInfiniteSourceHandler: sealed::Infinite + Send + Sync {
    fn install_writer_id(&mut self, writer_id: WriterId);
    fn next_invocation(&mut self) -> ErasedSourceInvocation;
}

/// Runtime-erased asynchronous infinite source interface.
#[doc(hidden)]
#[async_trait]
pub trait UnifiedAsyncInfiniteSourceHandler: sealed::AsyncInfinite + Send + Sync {
    fn install_writer_id(&mut self, writer_id: WriterId);
    fn poll_timeout(&self) -> Option<Duration>;
    fn hosted_ingress_slot(&self) -> Option<HostedIngressBindingSlot>;
    async fn next_invocation(&mut self) -> ErasedSourceInvocation;
    async fn drain(&mut self) -> Result<(), SourceError>;
}

impl<T: FiniteSourceHandler + Send + Sync> sealed::Finite for T {}

impl<T: FiniteSourceHandler + Send + Sync> UnifiedFiniteSourceHandler for T {
    fn install_writer_id(&mut self, writer_id: WriterId) {
        FiniteSourceHandler::bind_writer_id(self, writer_id);
    }

    fn next_invocation(&mut self) -> ErasedSourceInvocation {
        match FiniteSourceHandler::next(self) {
            Ok(Some(events)) => {
                ErasedSourceInvocation::completed(ErasedSourceCompletion::Batch(events), Vec::new())
            }
            Ok(None) => ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof, Vec::new()),
            Err(error) => ErasedSourceInvocation::handler_error(error, Vec::new()),
        }
    }
}

impl<T: AsyncFiniteSourceHandler + Send + Sync> sealed::AsyncFinite for T {}

#[async_trait]
impl<T: AsyncFiniteSourceHandler + Send + Sync> UnifiedAsyncFiniteSourceHandler for T {
    fn install_writer_id(&mut self, writer_id: WriterId) {
        AsyncFiniteSourceHandler::bind_writer_id(self, writer_id);
    }

    fn poll_timeout(&self) -> Option<Duration> {
        AsyncFiniteSourceHandler::poll_timeout(self)
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        match AsyncFiniteSourceHandler::next(self).await {
            Ok(Some(events)) => {
                ErasedSourceInvocation::completed(ErasedSourceCompletion::Batch(events), Vec::new())
            }
            Ok(None) => ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof, Vec::new()),
            Err(error) => ErasedSourceInvocation::handler_error(error, Vec::new()),
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        AsyncFiniteSourceHandler::drain(self).await
    }
}

impl<T: InfiniteSourceHandler + Send + Sync> sealed::Infinite for T {}

impl<T: InfiniteSourceHandler + Send + Sync> UnifiedInfiniteSourceHandler for T {
    fn install_writer_id(&mut self, writer_id: WriterId) {
        InfiniteSourceHandler::bind_writer_id(self, writer_id);
    }

    fn next_invocation(&mut self) -> ErasedSourceInvocation {
        match InfiniteSourceHandler::next(self) {
            Ok(events) => {
                ErasedSourceInvocation::completed(ErasedSourceCompletion::Batch(events), Vec::new())
            }
            Err(error) => ErasedSourceInvocation::handler_error(error, Vec::new()),
        }
    }
}

impl<T: AsyncInfiniteSourceHandler + Send + Sync> sealed::AsyncInfinite for T {}

#[async_trait]
impl<T: AsyncInfiniteSourceHandler + Send + Sync> UnifiedAsyncInfiniteSourceHandler for T {
    fn install_writer_id(&mut self, writer_id: WriterId) {
        AsyncInfiniteSourceHandler::bind_writer_id(self, writer_id);
    }

    fn poll_timeout(&self) -> Option<Duration> {
        AsyncInfiniteSourceHandler::poll_timeout(self)
    }

    fn hosted_ingress_slot(&self) -> Option<HostedIngressBindingSlot> {
        AsyncInfiniteSourceHandler::hosted_ingress_slot(self)
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        match AsyncInfiniteSourceHandler::next(self).await {
            Ok(events) => {
                ErasedSourceInvocation::completed(ErasedSourceCompletion::Batch(events), Vec::new())
            }
            Err(error) => ErasedSourceInvocation::handler_error(error, Vec::new()),
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        AsyncInfiniteSourceHandler::drain(self).await
    }
}

pub(super) use sealed::{AsyncFinite as SealAsyncFinite, AsyncInfinite as SealAsyncInfinite};
pub(super) use sealed::{Finite as SealFinite, Infinite as SealInfinite};
