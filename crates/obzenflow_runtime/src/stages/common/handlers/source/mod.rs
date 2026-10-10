// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source handler components

mod erased;
pub(crate) mod prepared;
mod record_de;
#[doc(hidden)]
pub mod traits;
pub(crate) mod typed;

#[doc(hidden)]
pub use erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, ErasedSourceOutcome,
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler, UnifiedFiniteSourceHandler,
    UnifiedInfiniteSourceHandler,
};
pub use record_de::RecordDeError;
pub use traits::SourceError;
pub use typed::{
    HostedIngressSource, IngressDecodeError, IngressDecoder, IngressRecord, SourceObservationSink,
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler,
};
