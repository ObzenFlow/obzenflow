// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Queries: the reads that a source asks through a port. A source writes
//! `query(first).through(port)`, and the effect journal records each read as
//! `query.answered` or `query.failed`.

pub use obzenflow_runtime::queries::{
    query, FiniteRead, PendingQuery, Query, QueryBookmark, QueryContext, QueryError,
    SourceDiagnosticReason, SourcePollErrorKind,
};
