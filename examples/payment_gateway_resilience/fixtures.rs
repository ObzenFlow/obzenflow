// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{CustomerOrderPlaced, OrderChannel, PaymentMethodState, TrafficPhase};

/// Scripted upstream event sequences used by the demo sources (FLOWIP-095d).
///
/// The original single stream is split across two order channels, web and
/// store, by alternating orders. The channels deliberately end at different
/// lengths (13 web, 12 store) so the earlier-ending channel's merged EOF
/// point is a real thing to reproduce under replay.
///
/// The combined sequence keeps the three scripted phases so it is easy to
/// reason about circuit breaker behaviour:
///
/// - First 8 orders (Warmup): locally valid, gateway healthy, with a couple of
///   material payment declines.
/// - Next 10 orders (Outage): locally valid, gateway starts failing.
/// - Final 7 orders (Recovery): mix of local invalid orders, gateway declines,
///   and successful authorizations while the gateway is healthy again.
pub fn scripted_web_orders() -> Vec<CustomerOrderPlaced> {
    split_channel(OrderChannel::Web)
}

pub fn scripted_store_orders() -> Vec<CustomerOrderPlaced> {
    split_channel(OrderChannel::Store)
}

pub fn retry_proof_order() -> CustomerOrderPlaced {
    proof_orders("retry-proof", 1)
        .into_iter()
        .next()
        .expect("one retry proof order")
}

pub fn healthy_proof_orders() -> Vec<CustomerOrderPlaced> {
    proof_orders("healthy-proof", 5)
}

pub fn open_rejection_proof_orders() -> Vec<CustomerOrderPlaced> {
    proof_orders("open-proof", 6)
}

#[cfg(test)]
pub fn half_open_recovery_proof_orders() -> Vec<CustomerOrderPlaced> {
    proof_orders("half-open-proof", 7)
}

fn proof_orders(prefix: &str, count: usize) -> Vec<CustomerOrderPlaced> {
    (0..count)
        .map(|index| CustomerOrderPlaced {
            order_id: format!("{prefix}-order-{index}"),
            customer_id: format!("{prefix}-customer-{index}"),
            channel: OrderChannel::Web,
            amount_cents: 10_00,
            payment_method_state: PaymentMethodState::Valid,
            phase: TrafficPhase::Warmup,
        })
        .collect()
}

fn split_channel(channel: OrderChannel) -> Vec<CustomerOrderPlaced> {
    scripted_orders()
        .into_iter()
        .enumerate()
        .filter(|(index, _)| match channel {
            OrderChannel::Web => index % 2 == 0,
            OrderChannel::Store => index % 2 == 1,
        })
        .map(|(_, mut order)| {
            order.order_id = format!("{}-{}", channel.label(), order.order_id);
            order.channel = channel;
            order
        })
        .collect()
}

fn scripted_orders() -> Vec<CustomerOrderPlaced> {
    let mut orders = Vec::new();

    // Warmup: healthy dependency. Some payment methods are declined by the
    // gateway even though they pass local structural validation.
    for i in 0..8 {
        orders.push(CustomerOrderPlaced {
            order_id: format!("warmup-{i}"),
            customer_id: format!("cust-{i}"),
            channel: OrderChannel::Web,
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
            channel: OrderChannel::Web,
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
            channel: OrderChannel::Web,
            amount_cents,
            payment_method_state,
            phase: TrafficPhase::Recovery,
        });
    }

    orders
}
