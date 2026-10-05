// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pure console projections for the tutorial's terminal outcomes.
//!
//! The console adapter owns output and replay labelling. In production, paid
//! orders could feed shipping, cancellations could feed customer notification,
//! and unavailable authorizations could feed retry or manual-review workflows.
//! `InvalidOrder` and `PaymentDeclined` remain journalled facts; their lifecycle
//! consequence reaches the terminal destination as `CancelledOrder`.

use super::domain::{CancelledOrder, PaymentAuthorizationUnavailable, PaymentAuthorized};

pub fn format_shipping(authorized: &PaymentAuthorized) -> String {
    format!(
        "📦 Paid order {} is ready for shipping (customer {}, amount: ${:.2}, auth {})",
        authorized.order_id,
        authorized.customer_id,
        authorized.amount_cents as f64 / 100.0,
        authorized.authorization_id
    )
}

pub fn format_cancelled(cancelled: &CancelledOrder) -> String {
    format!(
        "🚫 Order {} is cancelled: {} (customer {}, amount: ${:.2})",
        cancelled.order_id,
        cancelled.reason.label(),
        cancelled.customer_id,
        cancelled.amount_cents as f64 / 100.0
    )
}

pub fn format_unavailable(unavailable: &PaymentAuthorizationUnavailable) -> String {
    format!(
        "🟡 Payment authorization unavailable for order {}; route to retry/manual review: {} (customer {}, amount: ${:.2})",
        unavailable.order_id,
        unavailable.reason,
        unavailable.customer_id,
        unavailable.amount_cents as f64 / 100.0
    )
}
