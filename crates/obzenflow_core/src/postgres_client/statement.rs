// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fmt;

const PORTABLE_IDENTIFIER_LIMIT: usize = 63;

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
    /// and table are checked and quoted, so `body` can't name another target.
    pub fn insert_into(
        schema: impl Into<String>,
        table: impl Into<String>,
        body: impl AsRef<str>,
    ) -> Result<Self, PostgresStatementError> {
        let schema = schema.into();
        let table = table.into();
        if !valid_identifier(&schema) || !valid_identifier(&table) {
            return Err(PostgresStatementError::InvalidIdentifier);
        }
        let body = body.as_ref().trim();
        if body.is_empty() {
            return Err(PostgresStatementError::MissingInsertBody);
        }
        if body.contains('\0') {
            return Err(PostgresStatementError::InvalidInsertBody);
        }
        Ok(Self {
            sql: format!(
                "INSERT INTO {}.{} {body}",
                quote_identifier(&schema),
                quote_identifier(&table)
            ),
            destination: format!("{schema}.{table}"),
        })
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

fn valid_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= PORTABLE_IDENTIFIER_LIMIT
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$')
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
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
        for (schema, table) in [("", "entries"), ("ledger", "1entries"), ("led ger", "t")] {
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
