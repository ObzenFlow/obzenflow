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
    GatewayPaymentDecision, OrderCancellationReason, OrderCancelled,
    PaymentAuthorizationUnavailable, PaymentAuthorized, PaymentDeclineReason, PaymentDeclined,
    PaymentMethodState, TrafficPhase, ValidatedOrder,
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

/// Effectful transform that turns a `ValidatedOrder` into named gateway outcome facts.
///
/// The handler body stays small and deterministic: it performs one effect and
/// emits the named payment fact that happened. The gateway decision itself is
/// recorded as the effect outcome fact for replay suppression; the emitted
/// payment facts are the public channels downstream stages subscribe to.
#[derive(Debug, Clone)]
pub struct GatewayTransform;

#[async_trait]
impl EffectfulTransformHandler for GatewayTransform {
    type Input = ValidatedOrder;

    async fn process(&self, order: ValidatedOrder, fx: &mut Effects) -> Result<(), HandlerError> {
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

        emit_payment_decision_fact(order, decision, fx).await?;
        Ok(())
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
    GatewayPaymentDecision::AuthorizationUnavailable {
        reason: authorization_unavailable_reason(err),
    }
}

fn authorization_unavailable_reason(err: EffectError) -> String {
    match err {
        EffectError::Execution(message) => message,
        EffectError::RecordedFailure {
            error_type,
            error_message,
            ..
        } if error_type == "execution" => error_message
            .strip_prefix("effect execution failed: ")
            .unwrap_or(&error_message)
            .to_string(),
        EffectError::RecordedFailure {
            error_type,
            error_message,
            ..
        } => format!("{error_type}: {error_message}"),
        other => other.to_string(),
    }
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
            GatewayPaymentDecision::from_event(event),
            Some(GatewayPaymentDecision::AuthorizationUnavailable { .. })
        )
    }) || matches!(
        ValidatedOrder::from_event(event).map(|order| order.phase),
        Some(TrafficPhase::Outage)
    )
}

async fn emit_payment_decision_fact(
    order: ValidatedOrder,
    decision: GatewayPaymentDecision,
    fx: &mut Effects,
) -> Result<(), HandlerError> {
    match decision {
        GatewayPaymentDecision::Authorized { authorization_id } => {
            fx.emit(PaymentAuthorized {
                order_id: order.order_id,
                customer_id: order.customer_id,
                amount_cents: order.amount_cents,
                phase: order.phase,
                authorization_id,
            })
            .await
        }
        GatewayPaymentDecision::Declined { reason } => {
            // Fact first, consequence second: the gateway's decline is the
            // effect-adjacent fact, and the derived lifecycle consequence is
            // that the order is cancelled. Both are recorded, so the journal
            // keeps the provenance and the cancelled-orders subscriber sees
            // the fate.
            fx.emit(PaymentDeclined {
                order_id: order.order_id.clone(),
                customer_id: order.customer_id.clone(),
                amount_cents: order.amount_cents,
                phase: order.phase.clone(),
                reason: reason.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
            fx.emit(OrderCancelled {
                order_id: order.order_id,
                customer_id: order.customer_id,
                amount_cents: order.amount_cents,
                phase: order.phase,
                reason: OrderCancellationReason::PaymentDeclined { reason },
            })
            .await
        }
        GatewayPaymentDecision::AuthorizationUnavailable { reason } => {
            // Deliberately no cancellation here: unavailability means no
            // payment decision was reached, and manufacturing an order fate
            // out of a gateway outage would be a fake outcome (the FLOWIP-120m
            // non-performance doctrine). These orders go to manual review.
            fx.emit(PaymentAuthorizationUnavailable {
                order_id: order.order_id,
                customer_id: order.customer_id,
                amount_cents: order.amount_cents,
                phase: order.phase,
                reason,
            })
            .await
        }
    }
    .map_err(|e| HandlerError::Other(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{authorization_unavailable_reason, EffectError};
    use obzenflow_runtime::effects::RetryDisposition;

    #[test]
    fn replayed_execution_failure_preserves_live_domain_reason() {
        let live_reason = authorization_unavailable_reason(EffectError::Execution(
            "gateway_timeout_simulated".to_string(),
        ));
        let replay_reason = authorization_unavailable_reason(EffectError::RecordedFailure {
            error_type: "execution".into(),
            error_message: "effect execution failed: gateway_timeout_simulated".to_string(),
            retry: RetryDisposition::Retryable,
        });

        assert_eq!(live_reason, "gateway_timeout_simulated");
        assert_eq!(replay_reason, live_reason);
    }
}
