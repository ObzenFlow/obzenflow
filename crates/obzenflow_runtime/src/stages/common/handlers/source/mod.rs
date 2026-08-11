// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source handler components

mod erased;
#[doc(hidden)]
pub mod traits;
pub(crate) mod typed;

#[doc(hidden)]
pub use erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, ErasedSourceOutcome,
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler, UnifiedFiniteSourceHandler,
    UnifiedInfiniteSourceHandler,
};
pub use traits::SourceError;
pub use typed::{
    HostedIngressSource, SourceObservationSink, TypedAsyncFiniteSourceHandler,
    TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
