// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::effects::postgres::PostgresBindings;
use obzenflow::schema::TypedPayload;
use serde::{Deserialize, Serialize};

/// A payment that has been authorised for an order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PaymentAuthorized {
    pub(crate) payment_id: i64,
    pub(crate) order_id: String,
    pub(crate) customer_id: String,
    pub(crate) amount_cents: i64,
}

impl TypedPayload for PaymentAuthorized {
    const EVENT_TYPE: &'static str = "payments.payment_authorized";
}

impl PaymentAuthorized {
    /// Binds the statement parameters configured in `flow.rs`.
    pub(crate) fn bind(&self, bindings: &mut PostgresBindings) {
        bindings
            .bind(self.payment_id)
            .bind(&self.order_id)
            .bind(&self.customer_id)
            .bind(self.amount_cents);
    }
}

pub(crate) fn sample_payments() -> [PaymentAuthorized; 2] {
    [
        PaymentAuthorized {
            payment_id: 1001,
            order_id: "order-501".to_string(),
            customer_id: "customer-71".to_string(),
            amount_cents: 12_500,
        },
        PaymentAuthorized {
            payment_id: 1002,
            order_id: "order-502".to_string(),
            customer_id: "customer-93".to_string(),
            amount_cents: 8_750,
        },
    ]
}
