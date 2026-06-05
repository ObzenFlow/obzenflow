// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Gateway authorization for the payment-gateway resilience flow.
//!
//! The gateway call is the one place this flow touches the outside world, so it
//! is expressed as an [`Effect`] (a value the stage returns) rather than inline
//! I/O. The runtime executes the effect once, journals the captured gateway
//! decision, and on replay returns that recorded value without calling the
//! gateway again. That is the durable-execution property the tutorial teaches.
//!
//! Each payment-gateway example is self-contained; the high-volume variant keeps
//! its own copy of this logic and only swaps the source and the flow wiring.

use super::domain::{
    GatewayPaymentDecision, PaymentAuthorizationOutcome, PaymentAuthorizationUnavailable,
    PaymentAuthorized, PaymentDeclineReason, PaymentDeclined, PaymentMethodState, TrafficPhase,
    ValidatedOrder,
};
use async_trait::async_trait;
use obzenflow_core::{event::chain_event::ChainEvent, TypedPayload};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use serde_json::json;

/// The gateway authorization command, expressed as an effect.
///
/// Returning this command from a stage (instead of calling the gateway inline) is
/// what lets the runtime own execution: run it once, record the outcome, and
/// suppress it on replay. A real implementation would issue an HTTP request
/// inside [`Effect::execute`]; here we simulate latency and a scripted outage so
/// the behaviour is deterministic and easy to follow.
#[derive(Debug, Clone)]
pub struct AuthorizePayment {
    pub order: ValidatedOrder,
}

#[async_trait]
impl Effect for AuthorizePayment {
    const EFFECT_TYPE: &'static str = "payment.authorize";
    const SCHEMA_VERSION: u32 = 1;
    // A charge is never idempotent on its own, so the effect must carry a key
    // the gateway can dedupe on. The runtime enforces this before any I/O.
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Output = GatewayPaymentDecision;

    fn label(&self) -> &str {
        "authorize_payment"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({
            "order_id": self.order.order_id,
            "customer_id": self.order.customer_id,
            "amount_cents": self.order.amount_cents,
            "payment_method_state": self.order.payment_method_state,
            "phase": self.order.phase,
        })
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        // Latency comes from the deterministic, seeded RNG on the effect context
        // rather than a wall clock, so a replay reconstructs the same timing.
        let latency_ms: u64 = ctx.rng("gateway_latency").u64(50..=200);
        ctx.sleep(std::time::Duration::from_millis(latency_ms))
            .await;

        if matches!(self.order.phase, TrafficPhase::Outage) {
            return Err(EffectError::Execution(
                "gateway_timeout_simulated".to_string(),
            ));
        }

        match self.order.payment_method_state {
            PaymentMethodState::Valid => Ok(GatewayPaymentDecision::Authorized {
                authorization_id: PaymentAuthorized::AUTHORIZATION_ID_DEMO.to_string(),
            }),
            PaymentMethodState::InsufficientFunds => Ok(GatewayPaymentDecision::Declined {
                reason: PaymentDeclineReason::InsufficientFunds,
            }),
            PaymentMethodState::AddressMismatch => Ok(GatewayPaymentDecision::Declined {
                reason: PaymentDeclineReason::AddressMismatch,
            }),
            PaymentMethodState::InvalidNumber => Err(EffectError::Execution(
                "invalid_payment_method_reached_gateway".to_string(),
            )),
        }
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!(
            "payment-authorize:{}",
            self.order.order_id
        )))
    }
}

/// Effectful transform that turns a `ValidatedOrder` into a gateway outcome.
///
/// The handler body stays small and deterministic: it performs one effect and
/// folds the recorded outcome into a typed output. Everything replay-sensitive
/// lives inside the effect.
#[derive(Debug, Clone)]
pub struct GatewayTransform;

#[async_trait]
impl EffectfulTransformHandler for GatewayTransform {
    type Input = ValidatedOrder;
    type Output = PaymentAuthorizationOutcome;

    async fn process(
        &self,
        order: ValidatedOrder,
        fx: &mut Effects,
    ) -> Result<PaymentAuthorizationOutcome, HandlerError> {
        if matches!(
            order.payment_method_state,
            PaymentMethodState::InvalidNumber
        ) {
            return Err(HandlerError::Validation(
                "invalid_payment_method_reached_gateway".to_string(),
            ));
        }

        let decision = fx
            .perform(AuthorizePayment {
                order: order.clone(),
            })
            .await
            .unwrap_or_else(authorization_unavailable_decision);

        Ok(PaymentAuthorizationOutcome {
            order_id: order.order_id,
            customer_id: order.customer_id,
            amount_cents: order.amount_cents,
            phase: order.phase,
            decision,
        })
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "payment-gateway-v1"
    }
}

/// Map a remote effect failure onto an operational domain outcome.
///
/// A simulated gateway outage is infrastructure behavior, not a local business
/// invalid-order outcome and not a material gateway decline.
fn authorization_unavailable_decision(err: EffectError) -> GatewayPaymentDecision {
    let reason = match err {
        EffectError::Execution(message) => message,
        EffectError::RecordedFailure {
            error_type,
            error_message,
            ..
        } => {
            format!("{error_type}: {error_message}")
        }
        other => other.to_string(),
    };

    GatewayPaymentDecision::AuthorizationUnavailable { reason }
}

/// Tell the circuit breaker which scripted inputs represent dependency failure.
///
/// The stage still emits a domain event for subscribers, while the breaker keeps
/// its health model accurate and opens under sustained gateway unavailability.
pub fn simulated_gateway_unavailability_counts_as_failure(
    event: &ChainEvent,
    outputs: &[ChainEvent],
) -> bool {
    outputs.iter().any(|event| {
        matches!(
            PaymentAuthorizationOutcome::from_event(event).map(|outcome| outcome.decision),
            Some(GatewayPaymentDecision::AuthorizationUnavailable { .. })
        )
    }) || matches!(
        ValidatedOrder::from_event(event).map(|order| order.phase),
        Some(TrafficPhase::Outage)
    )
}

pub fn authorized_payment(outcome: PaymentAuthorizationOutcome) -> Option<PaymentAuthorized> {
    let GatewayPaymentDecision::Authorized { authorization_id } = outcome.decision else {
        return None;
    };

    Some(PaymentAuthorized {
        order_id: outcome.order_id,
        customer_id: outcome.customer_id,
        amount_cents: outcome.amount_cents,
        phase: outcome.phase,
        authorization_id,
    })
}

pub fn declined_payment(outcome: PaymentAuthorizationOutcome) -> Option<PaymentDeclined> {
    let GatewayPaymentDecision::Declined { reason } = outcome.decision else {
        return None;
    };

    Some(PaymentDeclined {
        order_id: outcome.order_id,
        customer_id: outcome.customer_id,
        amount_cents: outcome.amount_cents,
        phase: outcome.phase,
        reason,
    })
}

pub fn authorization_unavailable(
    outcome: PaymentAuthorizationOutcome,
) -> Option<PaymentAuthorizationUnavailable> {
    let GatewayPaymentDecision::AuthorizationUnavailable { reason } = outcome.decision else {
        return None;
    };

    Some(PaymentAuthorizationUnavailable {
        order_id: outcome.order_id,
        customer_id: outcome.customer_id,
        amount_cents: outcome.amount_cents,
        phase: outcome.phase,
        reason,
    })
}
