// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

/// High-level phase of the traffic pattern.
///
/// We deliberately script three phases so it is easy to
/// correlate behaviour in logs and metrics:
///
/// - Warmup: dependency is healthy, all gateway calls succeed.
/// - Outage: dependency starts failing, circuit breaker opens.
/// - Recovery: dependency is healthy again, but circuit may still
///   be open depending on cooldown / probe behaviour.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum TrafficPhase {
    Warmup,
    Outage,
    Recovery,
}

/// Upstream business event saying a customer placed an order.
///
/// This is the input boundary of the tutorial. In production it might be read
/// from an HTTP ingestion source, Kafka topic, database outbox, or another flow.
/// Here it comes from a scripted source so the tutorial is deterministic.
///
/// This is deliberately not called a command. The flow reacts to a fact that
/// already happened upstream, then decides whether to issue the outbound
/// `AuthorizePayment` effect command to the gateway.
///
/// `payment_method_state` is a demo fixture knob: `InvalidNumber` fails local
/// validation, while `InsufficientFunds` and `AddressMismatch` are valid enough
/// to call the gateway but are declined by the simulated remote gateway.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CustomerOrderPlaced {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub payment_method_state: PaymentMethodState,
    pub phase: TrafficPhase,
}

impl TypedPayload for CustomerOrderPlaced {
    const EVENT_TYPE: &'static str = "commerce.customer_order_placed";
    const SCHEMA_VERSION: u32 = 1;
}

/// Scripted payment-method state used by this tutorial.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum PaymentMethodState {
    Valid,
    InvalidNumber,
    InsufficientFunds,
    AddressMismatch,
}

/// Locally valid order ready for gateway authorization.
///
/// Only orders that pass deterministic business validation enter this type.
/// Invalid orders are emitted on a separate `InvalidOrder` channel instead of
/// being modeled as framework processing errors.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ValidatedOrder {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub payment_method_state: PaymentMethodState,
    pub phase: TrafficPhase,
}

impl TypedPayload for ValidatedOrder {
    const EVENT_TYPE: &'static str = "payment.order_validated";
    const SCHEMA_VERSION: u32 = 1;
}

/// Business reason an order cannot proceed to payment authorization locally.
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

/// Business event emitted when this flow rejects an order before gateway I/O.
///
/// This is not an exception rail. It is a normal domain outcome that a website,
/// order-status service, or customer-notification workflow could subscribe to.
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

/// Remote gateway decision for a valid payment authorization attempt.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum GatewayPaymentDecision {
    Authorized { authorization_id: String },
    Declined { reason: PaymentDeclineReason },
    AuthorizationUnavailable { reason: String },
}

impl TypedPayload for GatewayPaymentDecision {
    const EVENT_TYPE: &'static str = "payment.gateway_decision";
    const SCHEMA_VERSION: u32 = 1;
}

/// Material business reason the gateway declined payment.
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

/// Gateway authorization outcome before the flow branches into final channels.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaymentAuthorizationOutcome {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub phase: TrafficPhase,
    pub decision: GatewayPaymentDecision,
}

impl TypedPayload for PaymentAuthorizationOutcome {
    const EVENT_TYPE: &'static str = "payment.authorization_outcome";
    const SCHEMA_VERSION: u32 = 1;
}

/// Result of authorizing payment at the external gateway.
///
/// This is a fact produced by this flow after the gateway authorizes the order.
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

/// Material gateway decline, separate from local invalid-order handling.
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

/// Gateway authorization could not complete because the dependency was unavailable.
///
/// This is neither "paid" nor "declined"; it usually means retry, manual review,
/// or some other operational compensation.
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
