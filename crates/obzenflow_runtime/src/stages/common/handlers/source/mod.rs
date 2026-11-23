// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source handler components

mod erased;
mod hosted;
pub(crate) mod placeholder;
pub(crate) mod prepared;
pub(crate) mod read;
mod record_de;
pub use read::{ContinuousRead, FiniteRead, SourceRead};
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
pub use hosted::{HostedSource, HostedSubmission, IngressReceiptSender};
#[doc(hidden)]
pub use obzenflow_core::source::{ResumeRefusal, SourceContinuation};
pub use record_de::RecordDeError;
pub use traits::SourceError;
pub use typed::{
    prove_journal_form, IngressDecodeError, IngressDecoder, IngressRecord,
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler,
};
#[doc(hidden)]
pub use typed::{FiniteReadAdapter, InfiniteReadAdapter, SyncFiniteReads, SyncInfiniteReads};
