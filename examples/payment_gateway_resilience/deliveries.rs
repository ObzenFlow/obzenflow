// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed deliveries (FLOWIP-120s tier 3): named destinations carrying
//! identity, safety, and behaviour on the type, the delivery lane's analogue
//! of an `Effect`. `DELIVERY_TYPE` becomes the receipt's journalled
//! destination, `SAFETY` feeds the resume gate at compile time, and
//! `canonical_destination` is the instance identity the recovery
//! compatibility gate compares across runs.

use super::console;
use super::domain::PaymentAuthorized;
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{Delivered, Delivery};
use obzenflow_runtime::stages::sink::DeliveryContext;

/// The shipping-system handoff: in production a queue publish; in this demo
/// a labelled console line stands in for the subscriber.
#[derive(Clone, Debug, Default)]
pub struct ShippingHandoff;

impl ShippingHandoff {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Delivery for ShippingHandoff {
    type Input = PaymentAuthorized;
    const DELIVERY_TYPE: &'static str = "shipping.handoff";
    const SAFETY: SinkDeliverySafety = SinkDeliverySafety::IdempotentProjection;

    fn method(&self) -> DeliveryMethod {
        DeliveryMethod::QueuePublish {
            queue_name: "shipping".to_string(),
        }
    }

    fn canonical_destination(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({ "queue": "shipping" }))
    }

    async fn deliver(
        &mut self,
        authorized: PaymentAuthorized,
        ctx: &DeliveryContext,
    ) -> Result<Delivered, HandlerError> {
        console::send_to_shipping(authorized, ctx.provenance());
        Ok(Delivered::one())
    }
}
