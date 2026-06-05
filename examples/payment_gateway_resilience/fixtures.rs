// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{CustomerOrderPlaced, PaymentMethodState, TrafficPhase};

/// Scripted upstream event sequence used by the demo source.
///
/// The sequence is deliberately small and predictable so it is
/// easy to reason about circuit breaker behaviour:
///
/// - First 8 orders (Warmup): locally valid, gateway healthy, with a couple of
///   material payment declines.
/// - Next 10 orders (Outage): locally valid, gateway starts failing.
/// - Final 7 orders (Recovery): mix of local invalid orders, gateway declines,
///   and successful authorizations while the gateway is healthy again.
pub fn scripted_orders() -> Vec<CustomerOrderPlaced> {
    let mut orders = Vec::new();

    // Warmup: healthy dependency. Some payment methods are declined by the
    // gateway even though they pass local structural validation.
    for i in 0..8 {
        orders.push(CustomerOrderPlaced {
            order_id: format!("warmup-{i}"),
            customer_id: format!("cust-{i}"),
            amount_cents: 10_00,
            payment_method_state: match i {
                2 => PaymentMethodState::InsufficientFunds,
                5 => PaymentMethodState::AddressMismatch,
                _ => PaymentMethodState::Valid,
            },
            phase: TrafficPhase::Warmup,
        });
    }

    // Outage: dependency begins to fail even for valid orders.
    for i in 0..10 {
        orders.push(CustomerOrderPlaced {
            order_id: format!("outage-{i}"),
            customer_id: format!("cust-{}", 100 + i),
            amount_cents: 20_00,
            payment_method_state: PaymentMethodState::Valid,
            phase: TrafficPhase::Outage,
        });
    }

    // Recovery: dependency is healthy again, but we also inject a
    // couple of locally invalid orders and gateway declines to keep
    // domain outcomes separate from circuit-breaker behaviour.
    let recovery_pattern = [
        (PaymentMethodState::Valid, 30_00),
        (PaymentMethodState::InvalidNumber, 30_00),
        (PaymentMethodState::Valid, 0),
        (PaymentMethodState::InsufficientFunds, 15_00),
        (PaymentMethodState::AddressMismatch, 15_00),
        (PaymentMethodState::Valid, 50_00),
        (PaymentMethodState::Valid, 50_00),
    ];

    for (idx, (payment_method_state, amount_cents)) in recovery_pattern.into_iter().enumerate() {
        orders.push(CustomerOrderPlaced {
            order_id: format!("recovery-{idx}"),
            customer_id: format!("cust-{}", 200 + idx),
            amount_cents,
            payment_method_state,
            phase: TrafficPhase::Recovery,
        });
    }

    orders
}
