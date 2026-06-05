// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tutorial sink actions.
//!
//! These functions stand in for real subscribers. In production, paid orders
//! might feed shipping, locally invalid orders might feed website status, remote
//! declines might feed customer notification, and unavailable authorizations
//! might feed retry or manual-review workflows.

use super::domain::{
    InvalidOrder, PaymentAuthorizationUnavailable, PaymentAuthorized, PaymentDeclined,
};

pub fn send_to_shipping(authorized: PaymentAuthorized) {
    println!(
        "📦 Paid order {} is ready for shipping (customer {}, amount: ${:.2}, auth {})",
        authorized.order_id,
        authorized.customer_id,
        authorized.amount_cents as f64 / 100.0,
        authorized.authorization_id
    );
}

pub fn record_invalid_order(invalid: InvalidOrder) {
    println!(
        "🚫 Order {} is invalid before gateway authorization: {} (customer {}, amount: ${:.2})",
        invalid.order_id,
        invalid.reason.label(),
        invalid.customer_id,
        invalid.amount_cents as f64 / 100.0
    );
}

pub fn record_gateway_decline(declined: PaymentDeclined) {
    println!(
        "💳 Gateway declined payment for order {}: {} (customer {}, amount: ${:.2})",
        declined.order_id,
        declined.reason.label(),
        declined.customer_id,
        declined.amount_cents as f64 / 100.0
    );
}

pub fn record_authorization_unavailable(unavailable: PaymentAuthorizationUnavailable) {
    println!(
        "🟡 Payment authorization unavailable for order {}; route to retry/manual review: {} (customer {}, amount: ${:.2})",
        unavailable.order_id,
        unavailable.reason,
        unavailable.customer_id,
        unavailable.amount_cents as f64 / 100.0
    );
}
