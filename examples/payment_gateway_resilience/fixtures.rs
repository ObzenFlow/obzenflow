// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{CustomerOrderPlaced, OrderChannel, PaymentMethodState, TrafficPhase};

pub fn scripted_web_orders() -> Vec<CustomerOrderPlaced> {
    split_channel(OrderChannel::Web)
}

pub fn scripted_store_orders() -> Vec<CustomerOrderPlaced> {
    split_channel(OrderChannel::Store)
}

fn split_channel(channel: OrderChannel) -> Vec<CustomerOrderPlaced> {
    scripted_orders()
        .into_iter()
        .filter(|order| order.channel == channel)
        .collect()
}

fn scripted_orders() -> Vec<CustomerOrderPlaced> {
    use PaymentMethodState::{AddressMismatch, InsufficientFunds, InvalidNumber, Valid};

    let warmup = [
        ("cust-2041", 48_20, Valid),
        ("cust-1187", 86_40, Valid),
        ("cust-3302", 94_99, InsufficientFunds),
        ("cust-7160", 23_75, Valid),
        ("cust-2958", 64_00, Valid),
        ("cust-1453", 57_30, AddressMismatch),
        ("cust-6390", 12_50, Valid),
        ("cust-2624", 39_95, Valid),
    ];

    let outage = [
        ("cust-5017", 71_25, Valid),
        ("cust-3741", 18_00, Valid),
        ("cust-8822", 45_60, Valid),
        ("cust-1906", 29_99, Valid),
        ("cust-4815", 83_10, Valid),
        ("cust-7308", 52_75, Valid),
        ("cust-2189", 66_40, Valid),
        ("cust-9054", 15_25, Valid),
        ("cust-3627", 77_80, Valid),
        ("cust-6143", 33_50, Valid),
    ];

    let recovery = [
        ("cust-1702", 54_10, Valid),
        ("cust-8461", 27_30, InvalidNumber),
        ("cust-2930", 0, Valid),
        ("cust-5586", 91_20, InsufficientFunds),
        ("cust-4219", 18_25, AddressMismatch),
        ("cust-7795", 76_80, Valid),
        ("cust-3068", 42_60, Valid),
    ];

    let mut orders = Vec::new();
    for (phase, script) in [
        (TrafficPhase::Warmup, warmup.as_slice()),
        (TrafficPhase::Outage, outage.as_slice()),
        (TrafficPhase::Recovery, recovery.as_slice()),
    ] {
        for (customer_id, amount_cents, payment_method_state) in script {
            let index = orders.len();
            let (channel, first_order_number) = if index % 2 == 0 {
                (OrderChannel::Web, 10_481)
            } else {
                (OrderChannel::Store, 4_471)
            };
            orders.push(CustomerOrderPlaced {
                order_id: format!("{}-{}", channel.label(), first_order_number + index / 2),
                customer_id: customer_id.to_string(),
                channel,
                amount_cents: *amount_cents,
                payment_method_state: payment_method_state.clone(),
                phase: phase.clone(),
            });
        }
    }

    orders
}
