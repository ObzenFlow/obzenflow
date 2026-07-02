// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Console projection helpers for the tutorial's deliveries.
//!
//! These functions stand in for real subscribers. In production, paid orders
//! feed the `ShippingHandoff` typed delivery (see `deliveries.rs`), cancelled
//! orders might feed customer notification and order-status services, and
//! unavailable authorizations might feed retry or manual-review workflows.
//!
//! Sinks are external deliveries, not fact channels: `InvalidOrder` and
//! `PaymentDeclined` are recorded in the journal as provenance but have no
//! sink of their own; their lifecycle consequence arrives here as
//! `OrderCancelled`.

use super::domain::{OrderCancelled, PaymentAuthorizationUnavailable, PaymentAuthorized};
use obzenflow_runtime::stages::sink::DeliveryProvenance;

/// FLOWIP-120i: archived outcomes re-emitted during replay are labelled, so
/// the console never implies a delivery happened again. The label is stdout
/// presentation only; journals carry no marker.
fn provenance_prefix(provenance: DeliveryProvenance) -> &'static str {
    match provenance {
        DeliveryProvenance::Replayed => "[replay] ",
        _ => "",
    }
}

pub fn send_to_shipping(authorized: PaymentAuthorized, provenance: DeliveryProvenance) {
    println!(
        "{}📦 Paid order {} is ready for shipping (customer {}, amount: ${:.2}, auth {})",
        provenance_prefix(provenance),
        authorized.order_id,
        authorized.customer_id,
        authorized.amount_cents as f64 / 100.0,
        authorized.authorization_id
    );
}

pub fn record_cancelled_order(cancelled: OrderCancelled, provenance: DeliveryProvenance) {
    println!(
        "{}🚫 Order {} is cancelled: {} (customer {}, amount: ${:.2})",
        provenance_prefix(provenance),
        cancelled.order_id,
        cancelled.reason.label(),
        cancelled.customer_id,
        cancelled.amount_cents as f64 / 100.0
    );
}

pub fn record_authorization_unavailable(
    unavailable: PaymentAuthorizationUnavailable,
    provenance: DeliveryProvenance,
) {
    println!(
        "{}🟡 Payment authorization unavailable for order {}; route to retry/manual review: {} (customer {}, amount: ${:.2})",
        provenance_prefix(provenance),
        unavailable.order_id,
        unavailable.reason,
        unavailable.customer_id,
        unavailable.amount_cents as f64 / 100.0
    );
}
