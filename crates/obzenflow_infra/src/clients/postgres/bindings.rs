// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::PostgresClientError;
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, Encode, Postgres, Type};
use std::fmt;

/// The arguments for one statement, encoded in parameter order using SQLx
/// PostgreSQL codecs. An `Option<T>` binds a nullable parameter of type `T`.
#[derive(Clone)]
pub struct PostgresBindings {
    arguments: Result<PgArguments, PostgresClientError>,
}

impl Default for PostgresBindings {
    fn default() -> Self {
        Self {
            arguments: Ok(PgArguments::default()),
        }
    }
}

impl PostgresBindings {
    pub(super) fn prepare(bind: impl FnOnce(&mut Self)) -> Result<Self, PostgresClientError> {
        let mut bindings = Self::default();
        bind(&mut bindings);
        match bindings.arguments {
            Ok(_) => Ok(bindings),
            Err(error) => Err(error),
        }
    }

    /// Encode the next parameter, starting at `$1`. The first encoding failure
    /// is returned before the statement is sent.
    pub fn bind<'q, T>(&mut self, value: T) -> &mut Self
    where
        T: Encode<'q, Postgres> + Type<Postgres> + 'q,
    {
        if let Ok(arguments) = &mut self.arguments {
            let ordinal = arguments.len() + 1;
            if let Err(error) = arguments.add(value) {
                self.arguments = Err(PostgresClientError::Unencodable(format!(
                    "argument ${ordinal}: {error}"
                )));
            }
        }
        self
    }

    /// Consume the bindings for execution by a PostgreSQL client.
    pub fn into_arguments(self) -> Result<PgArguments, PostgresClientError> {
        self.arguments
    }
}

impl fmt::Debug for PostgresBindings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresBindings").finish_non_exhaustive()
    }
}
