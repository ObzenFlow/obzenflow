// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Local payment validation for the resilience tutorial.
//!
//! Validation is deterministic business classification. A locally invalid order
//! is not a framework error; it is a domain outcome. The flow therefore branches
//! one upstream event into two typed channels:
//!
//! - `ValidatedOrder` for orders that may call the gateway.
//! - `InvalidOrder` for orders invalid before gateway I/O.

use super::domain::{
    CustomerOrderPlaced, InvalidOrder, InvalidOrderReason, PaymentMethodState, ValidatedOrder,
};

pub fn valid_order(order: CustomerOrderPlaced) -> Option<ValidatedOrder> {
    if invalid_reason_for(&order).is_some() {
        return None;
    }

    Some(ValidatedOrder {
        order_id: order.order_id,
        customer_id: order.customer_id,
        amount_cents: order.amount_cents,
        payment_method_state: order.payment_method_state,
        phase: order.phase,
    })
}

pub fn invalid_order(order: CustomerOrderPlaced) -> Option<InvalidOrder> {
    let reason = invalid_reason_for(&order)?;

    Some(InvalidOrder {
        order_id: order.order_id,
        customer_id: order.customer_id,
        amount_cents: order.amount_cents,
        phase: order.phase,
        reason,
    })
}

fn invalid_reason_for(order: &CustomerOrderPlaced) -> Option<InvalidOrderReason> {
    if matches!(
        order.payment_method_state,
        PaymentMethodState::InvalidNumber
    ) {
        Some(InvalidOrderReason::InvalidPaymentMethod)
    } else if order.amount_cents == 0 {
        Some(InvalidOrderReason::ZeroAmount)
    } else {
        None
    }
}
