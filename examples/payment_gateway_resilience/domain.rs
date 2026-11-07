// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::schema::TypedPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum TrafficPhase {
    Warmup,
    Outage,
    Recovery,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum OrderChannel {
    Web,
    Store,
}

impl OrderChannel {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Web => "web",
            Self::Store => "store",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CustomerOrderPlaced {
    pub order_id: String,
    pub customer_id: String,
    pub channel: OrderChannel,
    pub amount_cents: u64,
    pub payment_method_state: PaymentMethodState,
    pub phase: TrafficPhase,
}

impl TypedPayload for CustomerOrderPlaced {
    const EVENT_TYPE: &'static str = "commerce.customer_order_placed";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum PaymentMethodState {
    Valid,
    InvalidNumber,
    InsufficientFunds,
    AddressMismatch,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ValidatedOrder {
    pub order_id: String,
    pub customer_id: String,
    pub channel: OrderChannel,
    pub amount_cents: u64,
    pub payment_method_state: PaymentMethodState,
    pub phase: TrafficPhase,
}

impl TypedPayload for ValidatedOrder {
    const EVENT_TYPE: &'static str = "payment.order_validated";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum InvalidOrderReason {
    InvalidPaymentMethod,
    ZeroAmount,
}

impl InvalidOrderReason {
    pub fn label(&self) -> &'static str {
        match self {
            Self::InvalidPaymentMethod => "invalid payment method",
            Self::ZeroAmount => "amount must be greater than zero",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InvalidOrder {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub phase: TrafficPhase,
    pub reason: InvalidOrderReason,
}

impl TypedPayload for InvalidOrder {
    const EVENT_TYPE: &'static str = "order.invalid";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum OrderCancellationReason {
    LocalValidationFailed { reason: InvalidOrderReason },
}

impl OrderCancellationReason {
    pub fn label(&self) -> String {
        match self {
            Self::LocalValidationFailed { reason } => {
                format!("local validation failed: {}", reason.label())
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CancelledOrder {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub phase: TrafficPhase,
    pub reason: OrderCancellationReason,
}

impl TypedPayload for CancelledOrder {
    const EVENT_TYPE: &'static str = "order.cancelled";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum PaymentDeclineReason {
    InsufficientFunds,
    AddressMismatch,
}

impl PaymentDeclineReason {
    pub fn label(&self) -> &'static str {
        match self {
            Self::InsufficientFunds => "insufficient funds",
            Self::AddressMismatch => "billing address mismatch",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaymentAuthorized {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub phase: TrafficPhase,
    pub authorization_id: String,
}

impl PaymentAuthorized {
    pub const AUTHORIZATION_ID_DEMO: &'static str = "AUTH-DEMO-1234";
}

impl TypedPayload for PaymentAuthorized {
    const EVENT_TYPE: &'static str = "payment.authorized";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaymentDeclined {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub phase: TrafficPhase,
    pub reason: PaymentDeclineReason,
}

impl TypedPayload for PaymentDeclined {
    const EVENT_TYPE: &'static str = "payment.declined";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaymentAuthorizationUnavailable {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub phase: TrafficPhase,
    pub reason: String,
}

impl TypedPayload for PaymentAuthorizationUnavailable {
    const EVENT_TYPE: &'static str = "payment.authorization_unavailable";
    const SCHEMA_VERSION: u32 = 1;
}
