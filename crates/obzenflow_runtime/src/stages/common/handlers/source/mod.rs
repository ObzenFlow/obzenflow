// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source handler components

pub(crate) mod continuation;
mod erased;
mod hosted;
pub(crate) mod placeholder;
mod port;
mod positioned;
pub(crate) mod prepared;
pub(crate) mod read;
mod record_de;
pub use read::{ContinuousRead, FiniteRead, SourceRead};
#[doc(hidden)]
pub mod traits;
pub(crate) mod typed;

pub use continuation::{
    AsyncFiniteContinuation, AsyncInfiniteContinuation, AtMostOnce, FiniteContinuation,
    InfiniteContinuation, LatestState, LiveGuarantee, Reattach, Refuse, Seek,
};
#[doc(hidden)]
pub use continuation::{RefusedBy, ResumeRefusal, SourceContinuation};
#[doc(hidden)]
pub use erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, ErasedSourceOutcome, SourceResumeError,
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler, UnifiedFiniteSourceHandler,
    UnifiedInfiniteSourceHandler,
};
pub use hosted::{HostedSource, HostedSubmission, IngressReceiptSender};
pub use port::{AsyncFiniteSource, AsyncInfiniteSource, FiniteSource, InfiniteSource};
#[doc(hidden)]
pub use port::{
    BoxesReader, OwnedHandler, OwnedSource, PortAsyncFiniteReader, PortAsyncInfiniteReader,
    PortContinuation, PortFiniteReader, PortInfiniteReader, PortPositionedReader, SourceRowInput,
};
pub use positioned::{PositionedFiniteReader, SeekableSource, SourceRecord};
pub use record_de::RecordDeError;
pub use traits::SourceError;
pub use typed::{
    prove_journal_form, IngressDecodeError, IngressDecoder, IngressRecord, SourceObservationSink,
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler,
};
#[doc(hidden)]
pub use typed::{FiniteReadAdapter, InfiniteReadAdapter, SyncFiniteReads, SyncInfiniteReads};
