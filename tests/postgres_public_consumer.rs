// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow::effects::postgres::{
    self, default_postgres_client, PostgresClient, PostgresConnection, PostgresStatement,
    PostgresTransport,
};
use obzenflow::effects::{perform, EffectError, Port};
use obzenflow::flow::sink;
use obzenflow::schema::TypedPayload;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ConsumerPayment {
    id: i64,
}

impl TypedPayload for ConsumerPayment {
    const EVENT_TYPE: &'static str = "flowip_083c.public_consumer.payment";
}

#[obzenflow::port]
#[async_trait]
trait ConsumerPaymentPort {
    #[effect(type = "consumer.payment.store", at_least_once, records = reply)]
    async fn store(&self, payment: &ConsumerPayment) -> Result<(), EffectError>;
}

struct ConsumerPayments {
    postgres: Arc<dyn PostgresClient>,
    insert: PostgresStatement,
}

#[obzenflow::system("consumer-payments")]
#[async_trait]
impl ConsumerPaymentPort for ConsumerPayments {
    async fn store(&self, payment: &ConsumerPayment) -> Result<(), EffectError> {
        self.postgres
            .execute(&self.insert, |bindings| {
                bindings.bind(payment.id);
            })
            .await
            .map_err(postgres::unsent::<Self>)?;
        Ok(())
    }
}

#[tokio::test]
async fn postgres_port_builds_without_io() {
    let connection = PostgresConnection::from_url(
        "postgres://consumer:sentinel@localhost/consumer?sslmode=disable",
        PostgresTransport::ExternallyProtectedPlaintext,
    )
    .expect("consumer URL parses without I/O");
    let system = ConsumerPayments {
        postgres: default_postgres_client(connection).expect("consumer client builds without I/O"),
        insert: PostgresStatement::insert_into("public", "payments", "(id) VALUES ($1)")
            .expect("consumer target validates"),
    };
    let postgres = perform(|payment: &ConsumerPayment| payment.clone()).through(Port::to(system));
    sink!(ConsumerPayment => postgres);
}
