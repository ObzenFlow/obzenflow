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
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum TrafficPhase {
    Warmup,
    Outage,
    Recovery,
}

impl TrafficPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            TrafficPhase::Warmup => "warmup",
            TrafficPhase::Outage => "outage",
            TrafficPhase::Recovery => "recovery",
        }
    }
}

/// Command coming from an upstream system asking us to charge a card.
///
/// This is the "input" to the flow and models a typical payment
/// request: an amount, a customer, and whether local validation
/// thinks the card is structurally valid.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaymentCommand {
    pub request_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub card_ok: bool,
    pub phase: TrafficPhase,
}

impl TypedPayload for PaymentCommand {
    const EVENT_TYPE: &'static str = "payment.command";
    const SCHEMA_VERSION: u32 = 1;
}

/// Result of local validation before we ever talk to the gateway.
///
/// Validation failures are still emitted as events, but they are
/// marked with `ProcessingStatus::Error` so they contribute to
/// `obzenflow_errors_total` and can be routed to dead-letter sinks.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ValidatedPayment {
    pub request_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub card_ok: bool,
    pub phase: TrafficPhase,
    pub validation_error: Option<String>,
}

impl TypedPayload for ValidatedPayment {
    const EVENT_TYPE: &'static str = "payment.validated";
    const SCHEMA_VERSION: u32 = 1;
}

/// Result of attempting to charge the card at the external gateway.
///
/// This is the "happy path" output the business usually cares about.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthorizedPayment {
    pub request_id: String,
    pub customer_id: String,
    pub amount_cents: u64,
    pub phase: TrafficPhase,
    pub authorization_id: String,
}

impl TypedPayload for AuthorizedPayment {
    const EVENT_TYPE: &'static str = "payment.authorized";
    const SCHEMA_VERSION: u32 = 1;
}

