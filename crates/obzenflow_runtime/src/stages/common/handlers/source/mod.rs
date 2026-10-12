// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source handler components

pub(crate) mod continuation;
mod erased;
mod hosted;
mod positioned;
pub(crate) mod prepared;
mod record_de;
#[doc(hidden)]
pub mod traits;
pub(crate) mod typed;

#[doc(hidden)]
pub use continuation::{sources_refusing_resume, RefusedBy, ResumeRefusal, SourceContinuation};
pub use continuation::{
    AsyncFiniteContinuation, AsyncInfiniteContinuation, AtMostOnce, FiniteContinuation,
    InfiniteContinuation, LatestState, LiveGuarantee, Reattach, Refuse, Seek,
};
#[doc(hidden)]
pub use erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, ErasedSourceOutcome, SourceResumeError,
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler, UnifiedFiniteSourceHandler,
    UnifiedInfiniteSourceHandler,
};
pub use hosted::{HostedIngressSource, HostedSubmission, IngressReceiptSender};
pub use positioned::{PositionedFiniteReader, SeekableSource, SourceRecord};
pub use record_de::RecordDeError;
pub use traits::SourceError;
pub use typed::{
    prove_journal_form, IngressDecodeError, IngressDecoder, IngressRecord, SourceObservationSink,
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler,
};
