// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::sinks::postgres::{PostgresBind, PostgresBindings};
use obzenflow_core::TypedPayload;
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

/// Maps one domain event to the statement parameters configured in `flow.rs`.
#[derive(Clone, Debug)]
pub(crate) struct PaymentBinder;

impl PostgresBind for PaymentBinder {
    type Input = PaymentAuthorized;

    fn bind(&self, bindings: &mut PostgresBindings, payment: &Self::Input) {
        bindings
            .bind(payment.payment_id)
            .bind(&payment.order_id)
            .bind(&payment.customer_id)
            .bind(payment.amount_cents);
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
