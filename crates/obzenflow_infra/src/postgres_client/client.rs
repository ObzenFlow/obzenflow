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
        let bindings = PostgresBindings::prepare(bind)?;
        self.send(statement, bindings).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::encode::IsNull;
    use sqlx::error::BoxDynError;
    use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo};
    use sqlx::{Encode, Postgres, Type};

    struct InvalidParameter;

    impl Type<Postgres> for InvalidParameter {
        fn type_info() -> PgTypeInfo {
            <i32 as Type<Postgres>>::type_info()
        }
    }

    impl Encode<'_, Postgres> for InvalidParameter {
        fn encode_by_ref(&self, _: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
            Err("invalid parameter".into())
        }
    }

    struct UnreachableClient;

    #[async_trait]
    impl PostgresClient for UnreachableClient {
        async fn send(
            &self,
            _: &PostgresStatement,
            _: PostgresBindings,
        ) -> Result<u64, PostgresClientError> {
            panic!("encoding failed before send")
        }
    }

    #[tokio::test]
    async fn invalid_parameter_prevents_send() {
        let client: &dyn PostgresClient = &UnreachableClient;
        let statement =
            PostgresStatement::insert_into("public", "entries", "VALUES ($1, $2, $3)").unwrap();
        let result = client
            .execute(&statement, |bindings| {
                bindings
                    .bind(7_i32)
                    .bind(InvalidParameter)
                    .bind(InvalidParameter);
            })
            .await;
        assert_eq!(
            result,
            Err(PostgresClientError::Unencodable(
                "argument $2: invalid parameter".into()
            ))
        );
    }
}
