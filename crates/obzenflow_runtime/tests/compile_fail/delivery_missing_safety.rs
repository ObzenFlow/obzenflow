// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_runtime::stages::common::handlers::{Delivered, Delivery};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::sink::DeliveryContext;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MissingSafetyInput;

impl TypedPayload for MissingSafetyInput {
    const EVENT_TYPE: &'static str = "test.missing_safety.input";
}

#[derive(Clone, Debug)]
struct MissingSafetyDelivery;

#[async_trait]
impl Delivery for MissingSafetyDelivery {
    type Input = MissingSafetyInput;
    const DELIVERY_TYPE: &'static str = "test.missing_safety";

    async fn deliver(
        &mut self,
        _input: Self::Input,
        _ctx: &DeliveryContext,
    ) -> Result<Delivered, HandlerError> {
        Ok(Delivered::one())
    }
}

fn main() {}
