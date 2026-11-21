// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fmt;

/// A fixed statement a system runs, built and checked without a connection.
#[derive(Clone, PartialEq, Eq)]
pub struct PostgresStatement {
    sql: String,
    destination: String,
}

/// Why a statement was refused before any connection opened.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PostgresStatementError {
    #[error("PostgreSQL schema or table identifier is invalid")]
    InvalidIdentifier,
    #[error("PostgreSQL INSERT body is missing")]
    MissingInsertBody,
    #[error("PostgreSQL INSERT body is invalid")]
    InvalidInsertBody,
}

impl PostgresStatement {
    /// `INSERT INTO "schema"."table"` followed by `body`, which names the
    /// columns and values, such as `(id, amount) VALUES ($1, $2)`. The schema
    /// and table are checked and quoted. The caller supplies trusted SQL for `body`.
    pub fn insert_into(
        schema: impl Into<String>,
        table: impl Into<String>,
        body: impl AsRef<str>,
    ) -> Result<Self, PostgresStatementError> {
        let schema = Self::checked_identifier(schema.into())?;
        let table = Self::checked_identifier(table.into())?;
        let body = body.as_ref().trim();
        if body.is_empty() {
            return Err(PostgresStatementError::MissingInsertBody);
        }
        if body.contains('\0') {
            return Err(PostgresStatementError::InvalidInsertBody);
        }
        Ok(Self {
            sql: format!(r#"INSERT INTO "{schema}"."{table}" {body}"#),
            destination: format!("{schema}.{table}"),
        })
    }

    fn checked_identifier(name: String) -> Result<String, PostgresStatementError> {
        const MAX_BYTES: usize = 63;
        let mut bytes = name.bytes();
        if name.len() <= MAX_BYTES
            && bytes
                .next()
                .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
            && bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
        {
            Ok(name)
        } else {
            Err(PostgresStatementError::InvalidIdentifier)
        }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The `schema.table` the statement writes to.
    pub fn destination(&self) -> &str {
        &self.destination
    }
}

impl fmt::Debug for PostgresStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PostgresStatement")
            .field(&self.destination)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_quotes_qualified_table_name() {
        let statement =
            PostgresStatement::insert_into("ledger", "entries", " (id) VALUES ($1) ").unwrap();
        assert_eq!(
            statement.sql(),
            "INSERT INTO \"ledger\".\"entries\" (id) VALUES ($1)"
        );
        assert_eq!(statement.destination(), "ledger.entries");
    }

    #[test]
    fn malformed_identifier_refused() {
        for (schema, table) in [
            ("", "entries"),
            ("ledger", "1entries"),
            ("led ger", "t"),
            ("ledger", "bad\"name"),
        ] {
            assert_eq!(
                PostgresStatement::insert_into(schema, table, "(id) VALUES ($1)"),
                Err(PostgresStatementError::InvalidIdentifier)
            );
        }
        assert_eq!(
            PostgresStatement::insert_into("ledger", "a".repeat(64), "(id) VALUES ($1)"),
            Err(PostgresStatementError::InvalidIdentifier)
        );
    }

    #[test]
    fn empty_or_nul_body_refused() {
        assert_eq!(
            PostgresStatement::insert_into("ledger", "entries", "  "),
            Err(PostgresStatementError::MissingInsertBody)
        );
        assert_eq!(
            PostgresStatement::insert_into("ledger", "entries", "(id) VALUES ($1)\0"),
            Err(PostgresStatementError::InvalidInsertBody)
        );
    }
}
