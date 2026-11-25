// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow::commands::perform;
use obzenflow::effects::postgres::{
    self, default_postgres_client, PostgresBindings, PostgresClient, PostgresClientError,
    PostgresConnection, PostgresStatement, PostgresTransport,
};
use obzenflow::effects::EffectError;
use obzenflow::flow::sink;
use obzenflow::schema::TypedPayload;
use obzenflow::systems::Port;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ConsumerPayment {
    id: i32,
    installment: Option<i32>,
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

#[obzenflow::system("consumer-payments")]
struct ConsumerPayments {
    postgres: Arc<dyn PostgresClient>,
    insert: PostgresStatement,
}

#[async_trait]
impl ConsumerPaymentPort for ConsumerPayments {
    async fn store(&self, payment: &ConsumerPayment) -> Result<(), EffectError> {
        self.postgres
            .execute(&self.insert, |bindings| {
                bindings.bind(payment.id).bind(payment.installment);
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
        insert: PostgresStatement::insert_into(
            "public",
            "payments",
            "(id, installment) VALUES ($1, $2)",
        )
        .expect("consumer target validates"),
    };
    let postgres = perform(|payment: &ConsumerPayment| payment.clone()).through(Port::to(system));
    sink!(ConsumerPayment => postgres);
}

#[derive(Default)]
struct RecordingClient {
    sends: AtomicUsize,
}

#[async_trait]
impl PostgresClient for RecordingClient {
    async fn send(
        &self,
        statement: &PostgresStatement,
        bindings: PostgresBindings,
    ) -> Result<u64, PostgresClientError> {
        assert_eq!(statement.destination(), "public.payments");
        bindings.into_arguments()?;
        self.sends.fetch_add(1, Ordering::Relaxed);
        Ok(1)
    }
}

#[tokio::test]
async fn payment_nullable_integer_executes() {
    let client = Arc::new(RecordingClient::default());
    let system = ConsumerPayments {
        postgres: client.clone(),
        insert: PostgresStatement::insert_into(
            "public",
            "payments",
            "(id, installment) VALUES ($1, $2)",
        )
        .unwrap(),
    };
    system
        .store(&ConsumerPayment {
            id: 42,
            installment: None,
        })
        .await
        .unwrap();
    assert_eq!(client.sends.load(Ordering::Relaxed), 1);
}
