// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{PostgresBindings, PostgresClientError, PostgresStatement};
use async_trait::async_trait;

/// What a system speaks through to reach PostgreSQL.
#[async_trait]
pub trait PostgresClient: Send + Sync + 'static {
    /// Run the statement once with its bound values and bring back the number
    /// of rows it changed. A client never retries.
    async fn send(
        &self,
        statement: &PostgresStatement,
        bindings: PostgresBindings,
    ) -> Result<u64, PostgresClientError>;
}

impl dyn PostgresClient {
    /// Bind the statement's values in order, then run it once.
    pub async fn execute(
        &self,
        statement: &PostgresStatement,
        bind: impl FnOnce(&mut PostgresBindings),
    ) -> Result<u64, PostgresClientError> {
        let mut bindings = PostgresBindings::default();
        bind(&mut bindings);
        self.send(statement, bindings).await
    }
}
