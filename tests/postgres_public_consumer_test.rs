// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Downstream-style compile witness for the root PostgreSQL facade.
//!
//! This source intentionally imports no SQLx name. `PostgresQuery` is the
//! adapter-owned query carrier a consumer needs to implement its binder.

use obzenflow::sinks::postgres::{
    PostgresBind, PostgresConnection, PostgresQuery, PostgresSink, PostgresTransport,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::sink;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct ConsumerPayment {
    id: i64,
}

impl TypedPayload for ConsumerPayment {
    const EVENT_TYPE: &'static str = "flowip_083c.public_consumer.payment";
}

#[derive(Clone)]
struct ConsumerBinder;

impl PostgresBind<ConsumerPayment> for ConsumerBinder {
    fn bind<'q>(
        &self,
        query: PostgresQuery<'q>,
        payment: &'q ConsumerPayment,
    ) -> PostgresQuery<'q> {
        query.bind(payment.id)
    }
}

#[test]
fn root_feature_exposes_a_sink_macro_compatible_connector_without_sqlx_imports() {
    let connection = PostgresConnection::from_url(
        "postgres://consumer:sentinel@localhost/consumer?sslmode=disable",
        PostgresTransport::ExternallyProtectedPlaintext,
    )
    .expect("consumer URL parses without I/O");
    let postgres = PostgresSink::<ConsumerPayment>::builder()
        .connection(connection)
        .insert_into("public", "payments", "(id) VALUES ($1)")
        .expect("consumer target validates")
        .bind_with(ConsumerBinder)
        .build()
        .expect("consumer connector builds without I/O");
    let _descriptor = sink!(ConsumerPayment => postgres);
}
