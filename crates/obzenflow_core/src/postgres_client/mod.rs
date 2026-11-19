// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core outbound PostgreSQL client abstractions.
//!
//! Inner layers define the trait and its types, and `obzenflow_infra` provides
//! the client that opens connections.

pub mod bindings;
pub mod client;
pub mod error;
pub mod statement;

pub use bindings::{PostgresBindings, PostgresKind, PostgresParameter, PostgresValue};
pub use client::PostgresClient;
pub use error::PostgresClientError;
pub use statement::{PostgresStatement, PostgresStatementError};
