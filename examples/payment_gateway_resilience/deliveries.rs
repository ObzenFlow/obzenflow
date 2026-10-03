// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Named typed shipping destination.

use super::console;
use super::domain::{CancelledOrder, PaymentAuthorizationUnavailable, PaymentAuthorized};
use async_trait::async_trait;
use obzenflow::middleware::{SinkDeliveryObserver, SinkDeliveryObserverContext};
use obzenflow::stages::sinks::DeliveryMethod;
use obzenflow::stages::sinks::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
};

/// Small in-process shipping handoff used by the demo.
#[derive(Clone, Debug, Default)]
pub struct ShippingHandoff;

#[async_trait]
impl InlineSink for ShippingHandoff {
    type Input = PaymentAuthorized;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::ConsoleStdout)
    }

    async fn write(
        &mut self,
        authorized: PaymentAuthorized,
        context: SinkWriteContext,
    ) -> obzenflow::stages::sinks::SinkWriteResult {
        console::send_to_shipping(authorized, context.delivery().provenance());
        Ok(SinkWriteReport::terminal(
            SinkTerminalOutcome::success(None).with_items(1),
        ))
    }
}

/// Console record of a cancelled order, including its delivery provenance.
#[derive(Clone, Debug, Default)]
pub struct RecordCancelled;

#[async_trait]
impl InlineSink for RecordCancelled {
    type Input = CancelledOrder;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::ConsoleStdout)
    }

    async fn write(
        &mut self,
        cancelled: CancelledOrder,
        context: SinkWriteContext,
    ) -> obzenflow::stages::sinks::SinkWriteResult {
        console::record_cancelled_order(cancelled, context.delivery().provenance());
        Ok(SinkWriteReport::terminal(
            SinkTerminalOutcome::success(None).with_items(1),
        ))
    }
}

/// Console handoff for authorizations that require manual review.
#[derive(Clone, Debug, Default)]
pub struct RecordUnavailable;

#[async_trait]
impl InlineSink for RecordUnavailable {
    type Input = PaymentAuthorizationUnavailable;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::ConsoleStdout)
    }

    async fn write(
        &mut self,
        unavailable: PaymentAuthorizationUnavailable,
        context: SinkWriteContext,
    ) -> obzenflow::stages::sinks::SinkWriteResult {
        tracing::info!(
            operation = "payment.authorization",
            handoff_kind = "manual_review",
            order_id = %unavailable.order_id,
            "authorization queued for manual review"
        );
        console::record_authorization_unavailable(unavailable, context.delivery().provenance());
        Ok(SinkWriteReport::terminal(
            SinkTerminalOutcome::success(None).with_items(1),
        ))
    }
}

/// Emits an application diagnostic after the runtime classifies a shipping
/// delivery. It receives an immutable view and cannot alter settlement.
pub struct ShippingDeliveryLog;

impl SinkDeliveryObserver for ShippingDeliveryLog {
    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) {
        tracing::info!(
            stage = ctx.stage_name(),
            outcome = ?ctx.outcome(),
            "shipping delivery observed"
        );
    }
}
