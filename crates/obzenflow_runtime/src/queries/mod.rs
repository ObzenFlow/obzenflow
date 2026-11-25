// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Queries: the reads that a source asks through a port. A source writes
//! `query(first).through(port)`, and the effect journal records each read as
//! `query.answered` or `query.failed`.

mod builder;
mod context;
mod request;

pub use builder::{query, PendingQuery, Query};
#[doc(hidden)]
pub use builder::{Decode, Undecoded};
pub use context::QueryContext;
pub use request::FiniteRead;
pub(crate) use request::QueryCall;
#[doc(hidden)]
pub use request::{QueryRequest, QueryResult};

pub use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
pub use obzenflow_core::event::SourceDiagnosticReason;
pub use obzenflow_core::source::{QueryBookmark, QueryError};
