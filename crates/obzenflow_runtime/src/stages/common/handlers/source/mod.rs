// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source handler components

mod erased;
mod hosted;
pub(crate) mod placeholder;
pub(crate) mod prepared;
pub(crate) mod read;
pub(crate) mod record_de;
pub use read::{Continuous, Finite, ReadLifetime, SourceRead};
#[doc(hidden)]
pub mod traits;
pub(crate) mod typed;

#[doc(hidden)]
pub use erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, ErasedSourceOutcome,
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler,
};
#[cfg(test)]
pub(crate) use erased::{SealAsyncFinite, SealAsyncInfinite};
pub use hosted::HostedSource;
#[doc(hidden)]
pub use obzenflow_core::source::{ResumeRefusal, SourceContinuation};
pub use record_de::RecordDeError;
pub use traits::SourceError;
#[doc(hidden)]
pub use typed::{FiniteReadAdapter, InfiniteReadAdapter, SyncFiniteReads, SyncInfiniteReads};
pub use typed::{
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler,
};
