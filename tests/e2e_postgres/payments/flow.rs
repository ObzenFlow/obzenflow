// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{sample_payments, PaymentAuthorized};
use anyhow::Result;
use async_trait::async_trait;
use obzenflow::effects::postgres::{
    self, default_postgres_client, PostgresClient, PostgresConnection, PostgresStatement,
    PostgresTransport,
};
use obzenflow::effects::{perform, EffectError, EffectPortResolutionError, Port};
use obzenflow::flow::{flow, sink, source, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::stages::sources;
use std::path::PathBuf;
use std::sync::Arc;

const PAYMENTS_INSERT: &str = "(payment_id, order_id, customer_id, amount_cents) \
     VALUES ($1, $2, $3, $4) \
     ON CONFLICT (payment_id) DO UPDATE SET \
     order_id = EXCLUDED.order_id, \
     customer_id = EXCLUDED.customer_id, \
     amount_cents = EXCLUDED.amount_cents";

#[obzenflow::port]
#[async_trait]
pub(crate) trait PaymentStorePort {
    #[effect(type = "payments.store", at_least_once, records = reply)]
    async fn store(&self, payment: &PaymentAuthorized) -> Result<(), EffectError>;
}

struct PostgresPayments {
    postgres: Arc<dyn PostgresClient>,
    insert: PostgresStatement,
}

#[obzenflow::system("payments-postgres")]
#[async_trait]
impl PaymentStorePort for PostgresPayments {
    async fn store(&self, payment: &PaymentAuthorized) -> Result<(), EffectError> {
        self.postgres
            .execute(&self.insert, |bindings| {
                bindings
                    .bind(payment.payment_id)
                    .bind(&payment.order_id)
                    .bind(&payment.customer_id)
                    .bind(payment.amount_cents);
            })
            .await
            .map_err(postgres::unsent::<Self>)?;
        Ok(())
    }
}

fn unavailable(error: impl std::fmt::Display) -> EffectPortResolutionError {
    tracing::error!(%error, "the payments PostgreSQL output could not be configured");
    EffectPortResolutionError::ClientConstructionFailed
}

pub(crate) fn build(
    journals: PathBuf,
    transport: PostgresTransport,
    schema: String,
) -> Result<FlowDefinition> {
    let insert = PostgresStatement::insert_into(schema, "payments", PAYMENTS_INSERT)?;
    let store = Port::deferred(move || {
        let connection = PostgresConnection::from_env("OBZENFLOW_POSTGRES_URL", transport)
            .map_err(unavailable)?;
        Ok(PostgresPayments {
            postgres: default_postgres_client(connection).map_err(unavailable)?,
            insert: insert.clone(),
        })
    });
    let postgres = perform(|payment: &PaymentAuthorized| payment.clone()).through(store);

    Ok(FlowDefinition::materialize(move |_runtime_config| {
        let payments = sources::ValuesSource::new(sample_payments());

        Ok(flow! {
            name: "postgres_sink_payments",
            journals: disk_journals(journals),

            stages: {
                payments = source!(PaymentAuthorized => payments);
                postgres = sink!(PaymentAuthorized => postgres);
            },

            topology: {
                payments |> postgres;
            }
        })
    }))
}
