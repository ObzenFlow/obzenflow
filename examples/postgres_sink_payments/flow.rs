// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{sample_payments, PaymentAuthorized, PaymentBinder};
use anyhow::Result;
use obzenflow::sinks::postgres::{PostgresConnection, PostgresSink};
use obzenflow::sources;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use std::path::PathBuf;

pub(crate) fn build(
    journals: PathBuf,
    connection: PostgresConnection,
    schema: String,
) -> Result<FlowDefinition> {
    // The conflict target makes delivery repeat-safe: replaying an authorised
    // payment converges on the same row instead of creating a duplicate.
    let postgres = PostgresSink::builder(PaymentBinder)
        .connection(connection)
        .insert_into(
            schema,
            "payments",
            "(payment_id, order_id, customer_id, amount_cents) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (payment_id) DO UPDATE SET \
             order_id = EXCLUDED.order_id, \
             customer_id = EXCLUDED.customer_id, \
             amount_cents = EXCLUDED.amount_cents",
        )?
        .batch_size(2)?
        .redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
        .build()?;

    Ok(FlowDefinition::materialize(move |_runtime_config| {
        let payments = sources::finite(sample_payments());

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
